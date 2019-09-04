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
	"encoding/gob"
	"fmt"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/eliasdb/storage"
)

/*
DistributedStorageManager is a storage.Manager which sends requests to the
distributed storage.
*/
type DistributedStorageManager struct {
	name      string              // Name of the storage manager
	rrc       int                 // Round robin counter
	ds        *DistributedStorage // Distributed storage which created the instance
	rootError error               // Last error when root values were handled
}

/*
Name returns the name of the StorageManager instance.
*/
func (dsm *DistributedStorageManager) Name() string {
	return fmt.Sprint("DistributedStorageManager: ", dsm.name)
}

/*
Root returns a root value.
*/
func (dsm *DistributedStorageManager) Root(root int) uint64 {
	var ret uint64

	// Do not do anything is the cluster is not operational

	distTable, distTableErr := dsm.ds.DistributionTable()

	if distTableErr != nil {
		return 0
	}

	// Root ids always go to member 1

	member := distTable.Members()[0]

	request := &DataRequest{RTGetRoot, map[DataRequestArg]interface{}{
		RPStoreName: dsm.name,
		RPRoot:      root,
	}, nil, false}

	res, err := dsm.ds.sendDataRequest(member, request)

	if err != nil {

		// Cycle through all replicating members if there was an error.
		// (as long as the cluster is considered operational there must be a
		// replicating member available to accept the request)

		for _, rmember := range distTable.Replicas(member) {
			res, err = dsm.ds.sendDataRequest(rmember, request)

			if err == nil {
				break
			}
		}
	}

	dsm.rootError = err

	if res != nil {
		ret = res.(uint64)
	}

	return ret
}

/*
SetRoot writes a root value.
*/
func (dsm *DistributedStorageManager) SetRoot(root int, val uint64) {

	// Do not do anything is the cluster is not operational

	distTable, distTableErr := dsm.ds.DistributionTable()

	if distTableErr != nil {
		return
	}

	// Root ids always go to member 1

	member := distTable.Members()[0]

	request := &DataRequest{RTSetRoot, map[DataRequestArg]interface{}{
		RPStoreName: dsm.name,
		RPRoot:      root,
	}, val, false}

	_, err := dsm.ds.sendDataRequest(member, request)

	if err != nil {

		// Cycle through all replicating members if there was an error.
		// (as long as the cluster is considered operational there must be a
		// replicating member available to accept the request)

		for _, rmember := range distTable.Replicas(member) {
			_, err = dsm.ds.sendDataRequest(rmember, request)

			if err == nil {
				break
			}
		}
	}

	dsm.rootError = err
}

/*
Insert inserts an object and return its storage location.
*/
func (dsm *DistributedStorageManager) Insert(o interface{}) (uint64, error) {
	return dsm.insertOrUpdate(true, 0, o)
}

/*
Update updates a storage location.
*/
func (dsm *DistributedStorageManager) Update(loc uint64, o interface{}) error {
	_, err := dsm.insertOrUpdate(false, loc, o)
	return err
}

/*
insertOrUpdate stores an object and returns its storage location and any error.
*/
func (dsm *DistributedStorageManager) insertOrUpdate(insert bool, loc uint64, o interface{}) (uint64, error) {
	var member string
	var replicatingMembers []string
	var rtype RequestType
	var ret uint64

	// Do not do anything is the cluster is not operational

	distTable, distTableErr := dsm.ds.DistributionTable()

	if distTableErr != nil {
		return 0, distTableErr
	}

	// Choose the instance this request should be routed to

	if insert {
		members := distTable.Members()
		member = members[(dsm.rrc)%len(members)]

		rtype = RTInsert

	} else {
		member, replicatingMembers = distTable.LocationHome(loc)

		rtype = RTUpdate
	}

	// Serialize the object into a gob bytes stream

	bb := storage.BufferPool.Get().(*bytes.Buffer)
	defer func() {
		bb.Reset()
		storage.BufferPool.Put(bb)
	}()

	errorutil.AssertOk(gob.NewEncoder(bb).Encode(o))

	request := &DataRequest{rtype, map[DataRequestArg]interface{}{
		RPStoreName: dsm.name,
		RPLoc:       loc,
	}, bb.Bytes(), false}

	cloc, err := dsm.ds.sendDataRequest(member, request)

	if err == nil {
		return cloc.(uint64), err

	}

	// An error has occurred we need to use another member

	if rtype == RTInsert {

		// Cycle through all members and see which one accepts first

		members := distTable.Members()
		lenMembers := len(members)

		for i := 1; i < lenMembers; i++ {
			member = members[(dsm.rrc+i)%lenMembers]

			cloc, nerr := dsm.ds.sendDataRequest(member, request)
			if nerr == nil {
				ret = cloc.(uint64)
				err = nil
				break
			}
		}

	} else {

		// Cycle through all replicating members and see which one accepts first
		// (as long as the cluster is considered operational there must be a
		// replicating member available to accept the request)

		for _, member := range replicatingMembers {
			cloc, nerr := dsm.ds.sendDataRequest(member, request)
			if nerr == nil {
				ret = cloc.(uint64)
				err = nil
				break
			}
		}
	}

	return ret, err
}

/*
Free frees a storage location.
*/
func (dsm *DistributedStorageManager) Free(loc uint64) error {

	// Do not do anything is the cluster is not operational

	distTable, distTableErr := dsm.ds.DistributionTable()

	if distTableErr != nil {
		return distTableErr
	}

	// Choose the instance this request should be routed to

	member, replicatingMembers := distTable.LocationHome(loc)

	request := &DataRequest{RTFree, map[DataRequestArg]interface{}{
		RPStoreName: dsm.name,
		RPLoc:       loc,
	}, nil, false}

	_, err := dsm.ds.sendDataRequest(member, request)

	if err != nil {

		// Cycle through all replicating members and see which one accepts first
		// (as long as the cluster is considered operational there must be a
		// replicating member available to accept the request)

		for _, member := range replicatingMembers {
			_, nerr := dsm.ds.sendDataRequest(member, request)
			if nerr == nil {
				err = nil
				break
			}
		}
	}

	return err
}

/*
Exists checks if an object exists in a given storage location.
*/
func (dsm *DistributedStorageManager) Exists(loc uint64) (bool, error) {
	var ret bool
	err := dsm.lookupData(loc, &ret, false)
	return ret, err
}

/*
Fetch fetches an object from a given storage location and writes it to
a given data container.
*/
func (dsm *DistributedStorageManager) Fetch(loc uint64, o interface{}) error {
	return dsm.lookupData(loc, o, true)
}

/*
lookupData fetches or checks for an object in a given storage location.
*/
func (dsm *DistributedStorageManager) lookupData(loc uint64, o interface{}, fetch bool) error {
	var rt RequestType

	// Do not do anything if the cluster is not operational
	distTable, distTableErr := dsm.ds.DistributionTable()

	if distTableErr != nil {
		return distTableErr
	}

	// Choose the instance this request should be routed to

	primaryMember, secondaryMembers := distTable.LocationHome(loc)

	if fetch {
		rt = RTFetch
	} else {
		rt = RTExists
	}

	request := &DataRequest{rt, map[DataRequestArg]interface{}{
		RPStoreName: dsm.name,
		RPLoc:       loc,
	}, nil, false}

	res, err := dsm.ds.sendDataRequest(primaryMember, request)

	if err != nil || (!fetch && !res.(bool)) {

		// Try secondary members if the primary member failed or the data didn't exist there

		var serr error

		for _, member := range secondaryMembers {
			res, serr = dsm.ds.sendDataRequest(member, request)
			if serr == nil {
				err = nil
				break
			}
		}
	}

	if err == nil {
		if !fetch {
			*o.(*bool) = res.(bool)
		} else {
			gob.NewDecoder(bytes.NewReader(res.([]byte))).Decode(o)
		}
	}

	return err
}

/*
FetchCached is not implemented for a DistributedStorageManager. Only defined to satisfy
the StorageManager interface.
*/
func (dsm *DistributedStorageManager) FetchCached(loc uint64) (interface{}, error) {
	return nil, storage.ErrNotInCache
}

/*
Flush is not implemented for a DistributedStorageManager. All changes are immediately
written to disk in a cluster.
*/
func (dsm *DistributedStorageManager) Flush() error {

	_, distTableErr := dsm.ds.DistributionTable()

	// Do not do anything if the cluster is not operational

	if distTableErr != nil {
		return distTableErr
	}

	// Increase round robin counter - things which belond together should be
	// stored on the same members

	dsm.rrc++

	return nil
}

/*
Rollback is not implemented for a DistributedStorageManager. All changes are immediately
written to disk in a cluster.
*/
func (dsm *DistributedStorageManager) Rollback() error {
	return nil
}

/*
Close is not implemented for a DistributedStorageManager. Only the local storage must
be closed which is done when the DistributedStore is shut down.
*/
func (dsm *DistributedStorageManager) Close() error {

	if _, distTableErr := dsm.ds.DistributionTable(); distTableErr != nil {
		return distTableErr
	}

	return dsm.rootError
}
