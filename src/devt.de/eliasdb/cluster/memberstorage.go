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
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"devt.de/common/sortutil"
	"devt.de/eliasdb/cluster/manager"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/hash"
	"devt.de/eliasdb/storage"
)

/*
ClusterStoragePrefix is the prefix for cluster related storage managers
*/
const ClusterStoragePrefix = "cs_"

/*
LocalStoragePrefix is the prefix for local storage managers
*/
const LocalStoragePrefix = "ls_"

/*
memberStorage models the local storage of a cluster member. This data structure
is the only thing which has access to the wrapped graphstorage.Storage.
*/
type memberStorage struct {
	ds *DistributedStorage  // Distributed storage which created this member storage
	gs graphstorage.Storage // Wrapped graphstorage.Storage
	at *memberAddressTable  // Address table (cluster location -> local location)

	transferLock    *sync.Mutex // Lock for the transfer task
	transferRunning bool        // Flag to indicate that the transfer task is running

	rebalanceLock    *sync.Mutex // Lock for the rebalance task
	rebalanceRunning bool        // Flag to indicate that the rebalance task is running
	rebalanceCounter int
}

/*
newMemberStorage creates a new memberStorage instance.
*/
func newMemberStorage(ds *DistributedStorage, gs graphstorage.Storage) (*memberStorage, error) {

	sm := gs.StorageManager("cluster_translation", true)

	at, err := newMemberAddressTable(ds, sm)
	if err != nil {
		return nil, err
	}

	return &memberStorage{ds, gs, at, &sync.Mutex{}, false, &sync.Mutex{}, false, 0}, nil
}

/*
handleDataRequest deals with RPC requests. It is the only function which is
called by the RPC server of the member manager.
*/
func (ms *memberStorage) handleDataRequest(request interface{}, response *interface{}) error {
	var err error

	// Make sure a request can be served

	distTable, distTableErr := ms.at.checkState()

	if distTableErr != nil {
		return distTableErr
	}

	dr := request.(*DataRequest)

	switch dr.RequestType {
	case RTGetMain:
		*response = ms.gs.MainDB()

	case RTSetMain:
		err = ms.handleSetMainRequest(distTable, dr, response)

	case RTSetRoot:
		err = ms.handleSetRootRequest(distTable, dr, response)

	case RTGetRoot:
		err = ms.handleGetRootRequest(distTable, dr, response)

	case RTInsert:
		err = ms.handleInsertRequest(distTable, dr, response)

	case RTUpdate:
		err = ms.handleUpdateRequest(distTable, dr, response)

	case RTFree:
		err = ms.handleFreeRequest(distTable, dr, response)

	case RTExists:
		err = ms.handleFetchRequest(distTable, dr, response, false)

	case RTFetch:
		err = ms.handleFetchRequest(distTable, dr, response, true)

	case RTRebalance:
		err = ms.handleRebalanceRequest(distTable, dr, response)

	default:
		err = fmt.Errorf("Unknown request type")
	}

	manager.LogDebug(ms.ds.MemberManager.Name(), fmt.Sprintf("(Store): Handled: %v %s (Transfer: %v, Error: %v)",
		dr.RequestType, dr.Args, dr.Transfer, err))

	return err
}

/*
handleSetMainRequest sets the mainDB on the local storage manager.
*/
func (ms *memberStorage) handleSetMainRequest(distTable *DistributionTable, request *DataRequest, response *interface{}) error {
	mainDB := ms.gs.MainDB()
	newMainDB := request.Value.(map[string]string)

	// Update keys and values

	for k, v := range newMainDB {
		mainDB[k] = v
	}

	// Check if things should be deleted

	var toRemove []string

	for k := range mainDB {
		if _, ok := newMainDB[k]; !ok {
			toRemove = append(toRemove, k)
		}
	}

	for _, k := range toRemove {
		delete(mainDB, k)
	}

	err := ms.gs.FlushMain()

	if !request.Transfer {
		ms.at.AddTransferRequest(distTable.OtherReplicationMembers(0, ms.ds.MemberManager.Name()),
			&DataRequest{RTSetMain, nil, request.Value, true})
	}

	return err
}

/*
handleGetRootRequest retrieves a root value from a local storage manager.
*/
func (ms *memberStorage) handleGetRootRequest(distTable *DistributionTable, request *DataRequest, response *interface{}) error {

	dsname := request.Args[RPStoreName].(string)
	root := request.Args[RPRoot].(int)

	sm := ms.dataStorage(dsname, false)

	if sm != nil {
		*response = sm.Root(root)
	}

	return nil
}

/*
handleSetRootRequest sets a new root value in a local storage manager.
*/
func (ms *memberStorage) handleSetRootRequest(distTable *DistributionTable, request *DataRequest, response *interface{}) error {

	dsname := request.Args[RPStoreName].(string)
	root := request.Args[RPRoot].(int)

	sm := ms.dataStorage(dsname, true)

	sm.SetRoot(root, request.Value.(uint64))

	if !request.Transfer {
		ms.at.AddTransferRequest(distTable.OtherReplicationMembers(0, ms.ds.MemberManager.Name()),
			&DataRequest{RTSetRoot, request.Args, request.Value, true})
	}

	return sm.Flush()
}

/*
handleInsertRequest inserts an object and return its cluster storage location.

Distribution procedure:
Client -> Cluster Member Request Receiver
Cluster Member Request Receiver -> Cluster Member Primary Storage (chosen round-robin / available)
Cluster Member Primary Storage writes into its Transfer Table
Cluster Member Primary Storage (Transfer worker) -> Replicating Cluster Members
*/
func (ms *memberStorage) handleInsertRequest(distTable *DistributionTable, request *DataRequest, response *interface{}) error {
	var err error
	var cloc uint64

	dsname := request.Args[RPStoreName].(string)
	*response = 0

	sm := ms.dataStorage(dsname, true)

	if !request.Transfer {

		// First get a new cluster location (on this member)

		cloc, err = ms.at.NewClusterLoc(dsname)

	} else {

		// If this is a transfer request we know already the cluster location

		cloc = request.Args[RPLoc].(uint64)
	}

	if err == nil {

		// Insert into the local storage

		loc, err := sm.Insert(request.Value)

		if err == nil {

			// Add a translation

			_, _, err = ms.at.SetTransClusterLoc(dsname, cloc, loc, 1)

			if err == nil {

				if !request.Transfer {

					// Add transfer request for replication

					// At this point the operation has succedded. We still need to
					// replicate the change to all the replicating members but
					// any errors happening during this shall not fail this operation.
					// The next rebalancing will then synchronize all members again.

					ms.at.AddTransferRequest(distTable.Replicas(ms.ds.MemberManager.Name()),
						&DataRequest{RTInsert, map[DataRequestArg]interface{}{
							RPStoreName: dsname,
							RPLoc:       cloc,
						}, request.Value, true})
				}

				*response = cloc
			}
		}
	}

	return err
}

/*
handleUpdateRequest updates an object and return its cluster storage location.

There is indeed a chance to produce inconsistencies if members fail in the right
sequence. It is assumed that these will be delt with in the next rebalance.

Distribution procedure:
Client -> Cluster Member Request Receiver
Cluster Member Request Receiver -> Cluster Member Primary Storage or Replicating Cluster Member
Storing Cluster Member does the update and writes into its transfer table
Storing Cluster Member (Transfer worker) -> Replicating / Primary Cluster Members
*/
func (ms *memberStorage) handleUpdateRequest(distTable *DistributionTable, request *DataRequest, response *interface{}) error {
	var err error
	var newVersion uint64

	dsname := request.Args[RPStoreName].(string)
	cloc := request.Args[RPLoc].(uint64)
	*response = 0

	// Get the translation

	transRec, ok, err := ms.at.TransClusterLoc(dsname, cloc)

	if ok {

		sm := ms.dataStorage(dsname, false)

		if sm != nil {

			// Update the local storage

			if !request.Transfer {
				err = sm.Update(transRec.loc, request.Value)
				newVersion = transRec.ver + 1

			} else {
				newVersion = request.Args[RPVer].(uint64)

				if newVersion >= transRec.ver {
					err = sm.Update(transRec.loc, request.Value)

				} else {

					// Outdated update requests are simply ignored

					err = fmt.Errorf("Received outdated update request (%v - Location: %v)",
						ms.ds.MemberManager.Name(), cloc)

					manager.LogDebug(ms.ds.MemberManager.Name(), err.Error())

					// Need to return no error so the transfer worker on the
					// other side removes its entry

					err = nil
				}
			}

			if err == nil {

				// Increase the version of the translation record

				_, _, err = ms.at.SetTransClusterLoc(dsname, cloc, transRec.loc, newVersion)

				if err == nil {

					if !request.Transfer {

						// Add transfer request for replication

						// At this point the operation has succedded. We still need to
						// replicate the change to all the replicating members but
						// any errors happening during this shall not fail this operation.
						// The next rebalancing will then synchronize all members again.

						ms.at.AddTransferRequest(distTable.OtherReplicationMembers(cloc, ms.ds.MemberManager.Name()),
							&DataRequest{RTUpdate, map[DataRequestArg]interface{}{
								RPStoreName: dsname,
								RPLoc:       cloc,
								RPVer:       newVersion,
							}, request.Value, true})
					}

					*response = cloc

					return nil
				}
			}
		}
	}

	if err == nil {
		err = fmt.Errorf("Cluster slot not found (%v - Location: %v)",
			ms.ds.MemberManager.Name(), cloc)
	}

	return err
}

/*
handleFreeRequest removes an object.

Distribution procedure:
Client -> Cluster Member Request Receiver
Cluster Member Request Receiver -> Cluster Member Primary Storage or Replicating Cluster Member
Storing Cluster Member does the free and writes into its transfer table
Storing Cluster Member (Transfer worker) -> Replicating / Primary Cluster Members
*/
func (ms *memberStorage) handleFreeRequest(distTable *DistributionTable, request *DataRequest, response *interface{}) error {
	var err error

	dsname := request.Args[RPStoreName].(string)
	cloc := request.Args[RPLoc].(uint64)

	// Get the translation

	transRec, ok, err := ms.at.TransClusterLoc(dsname, cloc)

	if ok {

		sm := ms.dataStorage(dsname, false)

		if sm != nil {

			//  Remove the translation

			_, _, err = ms.at.RemoveTransClusterLoc(dsname, cloc)

			if err == nil {

				// Remove from the local storage

				err = sm.Free(transRec.loc)

				if !request.Transfer {

					// Add transfer request for replication

					// At this point the operation has succedded. We still need to
					// replicate the change to all the replicating members but
					// any errors happening during this shall not fail this operation.
					// The next rebalancing will then synchronize all members again.

					ms.at.AddTransferRequest(distTable.OtherReplicationMembers(cloc, ms.ds.MemberManager.Name()),
						&DataRequest{RTFree, map[DataRequestArg]interface{}{
							RPStoreName: dsname,
							RPLoc:       cloc,
						}, nil, true})
				}

				return err
			}
		}
	}

	if err == nil {
		err = fmt.Errorf("Cluster slot not found (%v - Location: %v)", ms.ds.MemberManager.Name(), cloc)
	}

	return err
}

/*
handleFetchRequest inserts an object and return its cluster storage location.
*/
func (ms *memberStorage) handleFetchRequest(distTable *DistributionTable,
	request *DataRequest, response *interface{}, fetch bool) error {

	var err error

	dsname := request.Args[RPStoreName].(string)
	cloc := request.Args[RPLoc].(uint64)

	// Get the translation

	transRec, ok, err := ms.at.TransClusterLoc(dsname, cloc)

	if ok {

		// Check if the data should be retrieved

		if !fetch {

			*response = true

			return nil
		}

		sm := ms.dataStorage(dsname, false)

		if sm != nil {
			var res []byte

			err = sm.Fetch(transRec.loc, &res)

			if err == nil {

				*response = res

				return nil
			}
		}

	} else if !fetch {

		*response = false

		return err
	}

	if err == nil {

		err = fmt.Errorf("Cluster slot not found (%v - Location: %v)", ms.ds.MemberManager.Name(), cloc)
	}

	return err
}

/*
handleRebalanceRequest processes rebalance requests.
*/
func (ms *memberStorage) handleRebalanceRequest(distTable *DistributionTable, request *DataRequest, response *interface{}) error {
	var err error
	var tr *translationRec
	var found bool
	var res interface{}
	var lloc uint64

	handleError := func(err error) {
		if err != nil {
			manager.LogDebug(ms.ds.MemberManager.Name(), fmt.Sprintf("(Store): Error during rebalancing request handling: %v", err))
		}
	}

	// Get the location ranges for this member and locations which are replicated on this member.

	storeRangeStart, storeRangeStop := distTable.MemberRange(ms.ds.MemberManager.Name())
	repRangeStart, repRangeStop := distTable.ReplicationRange(ms.ds.MemberManager.Name())

	// Get the request data

	rsource := request.Args[RPSrc].(string)
	smnames := request.Args[RPStoreName]
	locs := request.Args[RPLoc]
	vers := request.Args[RPVer]

	for i, cloc := range locs.([]uint64) {

		// Check if there was an error from the previous iteration

		handleError(err)
		err = nil

		smname := smnames.([]string)[i]
		ver := vers.([]uint64)[i]

		// Do not proceed if there is an error or if the location is out of
		// range of responsibility

		notInStoreRange := cloc < storeRangeStart || cloc > storeRangeStop
		notInRepRange := cloc < repRangeStart || cloc > repRangeStop

		// Check if the location exists in the local storage

		tr, found, err = ms.at.TransClusterLoc(smname, cloc)

		if err != nil || (notInStoreRange && notInRepRange) {

			// Skip the location if there was an error or if this member
			// is not relevant for the location in question (either as primary
			// storage member or as replica)

			continue
		}

		if found {

			// Check if the version is newer and update the local record if it is

			if tr.ver < ver {

				// Local record exists and needs to be updated

				sm := ms.dataStorage(smname, false)

				// Fetch the data from the remote machine

				res, err = ms.ds.sendDataRequest(rsource, &DataRequest{RTFetch, map[DataRequestArg]interface{}{
					RPStoreName: smname,
					RPLoc:       cloc,
				}, nil, false})

				if err == nil {

					// Update the local storage

					if err = sm.Update(tr.loc, res); err == nil {

						// Update the translation

						_, _, err = ms.at.SetTransClusterLoc(smname, cloc, tr.loc, ver)

						manager.LogDebug(ms.ds.MemberManager.Name(),
							fmt.Sprintf("(Store): Rebalance updated %v location: %v", smname, cloc))
					}
				}
			}

		} else {

			// The data on the remote system should be inserted into the local
			// datastore.

			sm := ms.dataStorage(smname, true)

			// Fetch the data from the remote machine

			res, err = ms.ds.sendDataRequest(rsource, &DataRequest{RTFetch, map[DataRequestArg]interface{}{
				RPStoreName: smname,
				RPLoc:       cloc,
			}, nil, false})

			if err == nil {

				// Insert into the local storage

				lloc, err = sm.Insert(res)

				if err == nil {

					// Add a translation

					_, _, err = ms.at.SetTransClusterLoc(smname, cloc, lloc, ver)

					manager.LogDebug(ms.ds.MemberManager.Name(),
						fmt.Sprintf("(Store): Rebalance inserted %v location: %v", smname, cloc))
				}
			}
		}

		if err == nil {

			// Should the sender have the data

			sourceSRangeStart, sourceSRangeStop := distTable.MemberRange(rsource)
			sourceRRangeStart, sourceRRangeStop := distTable.ReplicationRange(rsource)

			notInSourceSRange := cloc < sourceSRangeStart || cloc > sourceSRangeStop
			notInSourceRRange := cloc < sourceRRangeStart || cloc > sourceRRangeStop

			if notInSourceSRange && notInSourceRRange {

				manager.LogDebug(ms.ds.MemberManager.Name(),
					fmt.Sprintf("(Store): Rebalance removes %v location: %v from member %v",
						smname, tr.loc, rsource))

				res, err = ms.ds.sendDataRequest(rsource, &DataRequest{RTFree, map[DataRequestArg]interface{}{
					RPStoreName: smname,
					RPLoc:       cloc,
				}, nil, true})
			}
		}
	}

	handleError(err)

	return nil
}

/*
dataStorage returns a storage.StorageManager which will only store byte slices.
*/
func (ms *memberStorage) dataStorage(dsname string, create bool) storage.Manager {
	return ms.gs.StorageManager(LocalStoragePrefix+dsname, create)
}

/*
dump dumps the contents of a particular member storage manager as escaped strings.
(Works only for MemoryStorageManagers.)
*/
func (ms *memberStorage) dump(smname string) string {
	var res string

	printTransferTable := func(buf *bytes.Buffer) {

		// Go through the transfer table and see if there is anything

		it := hash.NewHTreeIterator(ms.at.transfer)

		for it.HasNext() {
			_, val := it.Next()

			if val != nil {
				tr := val.(*transferRec)

				args, _ := json.Marshal(tr.request.Args)

				vals, ok := tr.request.Value.([]byte)
				if !ok {
					vals, _ = json.Marshal(tr.request.Value)
				}

				buf.WriteString(fmt.Sprintf("transfer: %v - %v %v %q\n",
					tr.members, tr.request.RequestType, string(args), vals))
			}
		}
	}

	if smname == "" {

		// Dump the contents of the MainDB if no name is given

		buf := new(bytes.Buffer)

		buf.WriteString(fmt.Sprintf("%v MemberStorageManager MainDB\n",
			ms.ds.MemberManager.Name()))

		var keys []string
		for k := range ms.gs.MainDB() {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		// Output local storage content with mapped cluster locations

		for _, k := range keys {
			v := ms.gs.MainDB()[k]

			buf.WriteString(fmt.Sprintf("%v - %q\n", k, v))
		}

		printTransferTable(buf)

		return buf.String()
	}

	sm := ms.dataStorage(smname, false)

	if sm != nil {

		// Make sure the storage manager is a MemoryStorageManager

		if msm, ok := sm.(*storage.MemoryStorageManager); ok {

			// Get all stored cluster locations

			locmap := make(map[uint64]string)

			it := hash.NewHTreeIterator(ms.at.translation)
			for it.HasNext() {
				k, v := it.Next()
				key := string(k)

				if strings.HasPrefix(key, transPrefix) {
					key = string(key[len(fmt.Sprint(transPrefix, smname, "#")):])

					locmap[v.(*translationRec).loc] = fmt.Sprintf("%v (v:%v)",
						key, v.(*translationRec).ver)
				}
			}

			buf := new(bytes.Buffer)

			buf.WriteString(fmt.Sprintf("%v MemberStorageManager %v\n",
				ms.ds.MemberManager.Name(), msm.Name()))

			buf.WriteString("Roots: ")

			// Go through root values

			for i := 0; i < 10; i++ {
				rootVal := msm.Root(i)
				buf.WriteString(fmt.Sprintf("%v=%v ", i, rootVal))
			}

			buf.WriteString("\n")

			var keys []uint64

			for k := range msm.Data {
				keys = append(keys, k)
			}

			sortutil.UInt64s(keys)

			// Output local storage content with mapped cluster locations

			for _, k := range keys {
				v := msm.Data[k]

				caddr := locmap[k]
				buf.WriteString(fmt.Sprintf("cloc: %v - lloc: %v - %q\n",
					caddr, k, v))
			}

			printTransferTable(buf)

			res = buf.String()
		}
	}

	return res
}
