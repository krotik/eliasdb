/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package cluster

import (
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"

	"devt.de/krotik/common/timeutil"
	"devt.de/krotik/eliasdb/cluster/manager"
	"devt.de/krotik/eliasdb/hash"
	"devt.de/krotik/eliasdb/storage"
)

func init() {

	// Make sure we can use the relevant types in a gob operation

	gob.Register(&translationRec{})
	gob.Register(&transferRec{})
}

/*
rootIDTranslationTree is the root id for the translation map
*/
const rootIDTranslationTree = 2

/*
rootIDTransferTree is the root id for the transfer map
*/
const rootIDTransferTree = 3

/*
transPrefix is the prefix for translation entries (cluster location -> local location)
*/
const transPrefix = "t"

/*
rangePrefix is the prefix for range counters
*/
const newlocPrefix = "n"

/*
translationRec is a translation record which stores a local storage location with a
version number.
*/
type translationRec struct {
	Loc uint64 // Local storage location
	Ver uint64 // Version of the local stored data
}

/*
transferRec is a transfer record which stores a data transfer request.
*/
type transferRec struct {
	Members []string     // Target members
	Request *DataRequest // Data request
}

/*
memberAddressTable is used by a memberStorage to manage cluster locations and their link to
local locations.
*/
type memberAddressTable struct {
	ds                *DistributedStorage // Related distribution storage
	sm                storage.Manager     // Storage manager which stores this translation table
	translation       *hash.HTree         // Tree which stores the translation table (cluster location -> physical location)
	transfer          *hash.HTree         // Tree which stores the transfer table
	newlocCounters    map[string]uint64   // Cached counter values to create new cluster locations
	newlocCounterLock *sync.Mutex         // Lock for cached counter values
}

/*
newMemberAddressTable creates a new member address table.
*/
func newMemberAddressTable(ds *DistributedStorage, sm storage.Manager) (*memberAddressTable, error) {
	var err error
	var translation, transfer *hash.HTree
	var ret *memberAddressTable

	translation, err = getHtree(rootIDTranslationTree, sm)

	if err == nil {

		transfer, err = getHtree(rootIDTransferTree, sm)

		if err == nil {

			err = sm.Flush()

			if err == nil {

				ret = &memberAddressTable{ds, sm, translation, transfer, make(map[string]uint64), &sync.Mutex{}}
			}
		}
	}

	return ret, err
}

/*
NewClusterLoc returns a new cluster location for a given storage manager.
*/
func (mat *memberAddressTable) NewClusterLoc(dsname string) (uint64, error) {
	var ret uint64
	var err error
	var dsm *DistributedStorageManager

	// Check member is operational

	distTable, distTableErr := mat.checkState()

	if distTableErr != nil {
		return 0, distTableErr
	}

	// Get the location range which is allowed

	rangeStart, rangeStop := distTable.MemberRange(mat.ds.MemberManager.Name())

	// Get counter

	newLocCounter, _, _ := mat.newlocCounter(dsname)

	// Check that rangeCounter is sensible

	if newLocCounter < rangeStart {
		newLocCounter = rangeStart
	}

	// Get a StorageManager instance if required

	if newLocCounter == rangeStart {
		dsm = mat.ds.StorageManager(dsname, true).(*DistributedStorageManager)
	}

	locExists := func(dsname string, candidate uint64) (bool, error) {

		// We might be a new member - check with other members if we are at the start
		// of our range

		if newLocCounter == rangeStart {

			ok, err := dsm.Exists(candidate)

			if err != nil || ok {
				return err == nil && ok, err
			}
		}

		return mat.translation.Exists(transKey(dsname, candidate))
	}

	candidate := newLocCounter

	ok, err := locExists(dsname, candidate)

	if err == nil {

		if ok {

			// Candidate exists - search for a better one

			var i uint64
			for i = rangeStart; i <= rangeStop; i++ {

				ok, err = locExists(dsname, i)

				if err == nil && !ok && i != 0 {
					ret = i
					goto SearchResult

				} else if err != nil {

					goto SearchResult
				}
			}

			err = errors.New("Could not find any free storage location on this member")

		SearchResult:
		} else {

			// Candidate does not exist - it is a new location

			ret = candidate
		}
	}

	// At this point we either have an error or a valid location in ret

	if err == nil {

		newLocCounter = ret + 1

		if newLocCounter > rangeStop {

			// Reset range counter - next time we test which if there is anything
			// left in this range

			newLocCounter = 1
		}

		mat.setNewlocCounter(dsname, newLocCounter)
		mat.sm.Flush()
	}

	return ret, err
}

/*
AddTransferRequest adds a data transfer request which can be picked up by the transferWorker.
*/
func (mat *memberAddressTable) AddTransferRequest(targetMembers []string, request *DataRequest) {

	// Get a unique key for the transfer request

	key := timeutil.MakeTimestamp()

	ex, err := mat.transfer.Exists([]byte(key))
	for ex && err == nil {
		key = timeutil.MakeTimestamp()
		time.Sleep(time.Millisecond)
		ex, err = mat.transfer.Exists([]byte(key))
	}

	// Store the transfer request

	if err == nil {
		_, err := mat.transfer.Put([]byte(key), &transferRec{targetMembers, request})

		if err == nil {
			mat.sm.Flush()
		}
	}

	if request != nil {
		ts, _ := timeutil.TimestampString(string(key), "UTC")

		manager.LogDebug(mat.ds.Name(), "(Store): ",
			fmt.Sprintf("Added transfer request %v (Error: %v) to %v from %v",
				request.RequestType, err, targetMembers, ts))
	}
}

/*
TransClusterLoc translates a cluster location to a local location. Returns the translated
location, a flag if the location was found and lookup errors.
*/
func (mat *memberAddressTable) TransClusterLoc(dsname string, clusterLoc uint64) (*translationRec, bool, error) {
	v, err := mat.translation.Get(transKey(dsname, clusterLoc))
	if v == nil {
		return nil, false, err
	}
	return v.(*translationRec), true, err
}

/*
SetTransClusterLoc adds a translation from a cluster location to a local location. Returns the
previously stored translated location, a flag if the location was found and errors.
*/
func (mat *memberAddressTable) SetTransClusterLoc(dsname string, clusterLoc uint64,
	localLoc uint64, localVer uint64) (*translationRec, bool, error) {

	v, err := mat.translation.Put(transKey(dsname, clusterLoc), &translationRec{localLoc, localVer})

	if err == nil {
		mat.sm.Flush()
	}

	if v == nil {
		return nil, false, err
	}

	return v.(*translationRec), true, err
}

/*
RemoveTransClusterLoc removes a translation of a cluster location. Returns the
previously stored translated location, a flag if the location was found and errors.
*/
func (mat *memberAddressTable) RemoveTransClusterLoc(dsname string, clusterLoc uint64) (*translationRec, bool, error) {
	v, err := mat.translation.Remove(transKey(dsname, clusterLoc))

	if err == nil {
		mat.sm.Flush()
	}

	if v == nil {
		return nil, false, err
	}

	return v.(*translationRec), true, err
}

/*
Check the state of cluster member. Return an error if the member is not
operational.
*/
func (mat *memberAddressTable) checkState() (*DistributionTable, error) {

	distTable, distTableErr := mat.ds.DistributionTable()

	if distTableErr != nil {
		return nil, fmt.Errorf("Storage is currently disabled on member: %v (%v)",
			mat.ds.MemberManager.Name(), distTableErr)
	}

	return distTable, nil
}

// Helper functions
// ================

/*
newlocCounter returns the location counter for a given storage manager. Returns the translated
location, a flag if the location was found and lookup errors.
*/
func (mat *memberAddressTable) newlocCounter(dsname string) (uint64, bool, error) {

	// Try to get the counter from the cache

	mat.newlocCounterLock.Lock()
	cv, ok := mat.newlocCounters[dsname]
	mat.newlocCounterLock.Unlock()

	if ok {
		return cv, true, nil
	}

	// Lookup the counter

	v, err := mat.translation.Get(newlocCounterKey(dsname))
	if v == nil {
		return 1, false, err
	}

	ret := toUInt64(v)

	// Store counter in the cache

	mat.newlocCounterLock.Lock()
	mat.newlocCounters[dsname] = ret
	mat.newlocCounterLock.Unlock()

	return ret, true, err
}

/*
setNewlocCounter sets a location counter for a given storage manager.
*/
func (mat *memberAddressTable) setNewlocCounter(dsname string, counter uint64) error {

	// Store counter in the cache and HTree

	mat.newlocCounterLock.Lock()
	mat.newlocCounters[dsname] = counter
	mat.newlocCounterLock.Unlock()

	_, err := mat.translation.Put(newlocCounterKey(dsname), counter)

	return err
}

/*
newlocCounterKey returns the counter key for a given storage manager.
*/
func newlocCounterKey(dsname string) []byte {
	return []byte(fmt.Sprint(newlocPrefix, dsname))
}

/*
transKey returns the translation map lookup key for a given cluster location and storage manager.
*/
func transKey(dsname string, loc uint64) []byte {
	return []byte(fmt.Sprint(transPrefix, dsname, "#", loc))
}

/*
getHtree returns a HTree from a given storage.Manager with a given root ID.
*/
func getHtree(rootID int, sm storage.Manager) (*hash.HTree, error) {
	var htree *hash.HTree
	var err error

	loc := sm.Root(rootID)

	if loc == 0 {

		// Create a new HTree and store its location

		htree, err = hash.NewHTree(sm)

		if err == nil {

			// Make sure the new root id is persisted

			sm.SetRoot(rootID, htree.Location())
		}

	} else {

		// Load existing HTree

		htree, err = hash.LoadHTree(sm, loc)
	}

	return htree, err
}
