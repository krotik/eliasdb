/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
Package cluster contains EliasDB's clustering code.

The clustering code provides an abstraction layer to EliasDB's graphstorage.Storage.
This means the actual storage of a cluster can be entirely memory based or use
any other backend as long as it satisfies the graphstorage.Storage interface.

DistributedStorage wraps a graphstorage.Storage and has a manager.MemberManager
object.

Members are identified by a unique name. Calling Start() on manager.MemberManager
registers and starts the RPC server for the member. Cluster internal RPC requests
are served by manager.Server. It is a singleton object which routes RPC calls
to registered MemberManagers - this architecture makes it easy to unit test
the cluster code. The manager.MemberManager has a manager.Client object which
can be used to send messages to the cluster.

The integrity of the cluster is protected by a shared secret (string) among
all members of the cluster. A new member can only join and communicate with
the cluster if it has the secret string. The secret string is never transferred
directly over the network - it is only used for generating a member specific
token which can be verified by all other members.

The clustering code was inspired by Amazon DynamoDB
http://www.allthingsdistributed.com/2012/01/amazon-dynamodb.html
*/
package cluster

import (
	"fmt"
	"math"
	"sync"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/eliasdb/cluster/manager"
	"devt.de/krotik/eliasdb/graph/graphstorage"
	"devt.de/krotik/eliasdb/storage"
)

/*
DistributedStorageError is an error related to the distribution storage. This
error is returned when the data distribution fails for example when too many
cluster members have failed.
*/
type DistributedStorageError struct {
	err error // Wrapped error
}

/*
newError creates a new DistributedStorageError.
*/
func newError(err error) error {
	return &DistributedStorageError{err}
}

/*
Error returns a string representation of a DistributedStorageError.
*/
func (dse *DistributedStorageError) Error() string {
	return fmt.Sprint("Storage disabled: ", dse.err.Error())
}

/*
DistributedStorage data structure
*/
type DistributedStorage struct {
	MemberManager *manager.MemberManager // Manager object

	distributionTableLock  *sync.Mutex        // Mutex to access the distribution table
	distributionTable      *DistributionTable // Distribution table for the cluster - may be nil
	distributionTableError error              // Error detail if the storage is disabled

	localName         string                                // Name of the local graph storage
	localDRHandler    func(interface{}, *interface{}) error // Local data request handler
	localFlushHandler func() error                          // Handler to flush the local storage
	localCloseHandler func() error                          // Handler to close the local storage

	mainDB      map[string]string // Local main copy (only set when requested)
	mainDBError error             // Last error when main db was requested
}

/*
NewDistributedStorage creates a new cluster graph storage. The distributed storage
wraps around a local graphstorage.Storage. The configuration of the distributed
storage consists of two parts: A normal config map which defines static information
like rpc port, secret string, etc and a StateInfo object which is used for dynamic
information like cluster members, member status, etc. An empty StateInfo means
that the cluster has only one member.
*/
func NewDistributedStorage(gs graphstorage.Storage, config map[string]interface{},
	stateInfo manager.StateInfo) (*DistributedStorage, error) {

	ds, _, err := newDistributedAndMemberStorage(gs, config, stateInfo)
	return ds, err
}

/*
DSRetNew is the return value on successful creating a distributed storage
(used for testing)
*/
var DSRetNew error

/*
newDistributedAndMemberStorage creates a new cluster graph storage but also returns a
reference to the internal memberStorage object.
*/
func newDistributedAndMemberStorage(gs graphstorage.Storage, config map[string]interface{},
	stateInfo manager.StateInfo) (*DistributedStorage, *memberStorage, error) {

	var repFac int

	// Merge given configuration with default configuration

	clusterconfig := datautil.MergeMaps(manager.DefaultConfig, config)

	// Make 100% sure there is a secret string

	if clusterconfig[manager.ConfigClusterSecret] == "" {
		clusterconfig[manager.ConfigClusterSecret] = manager.DefaultConfig[manager.ConfigClusterSecret]
	}

	// Set replication factor

	if f, ok := stateInfo.Get(manager.StateInfoREPFAC); !ok {
		repFac = int(math.Max(clusterconfig[manager.ConfigReplicationFactor].(float64), 1))
		stateInfo.Put(manager.StateInfoREPFAC, repFac)
		stateInfo.Flush()
	} else {
		repFac = f.(int)
	}

	// Create member objects - these calls will initialise this member's state info

	mm := manager.NewMemberManager(clusterconfig[manager.ConfigRPC].(string),
		clusterconfig[manager.ConfigMemberName].(string),
		clusterconfig[manager.ConfigClusterSecret].(string), stateInfo)

	dt, err := NewDistributionTable(mm.Members(), repFac)
	if err != nil {
		mm.LogInfo("Storage disabled:", err)
	}

	ds := &DistributedStorage{mm, &sync.Mutex{}, dt, err, gs.Name(), nil, nil, nil, nil, nil}

	// Create MemberStorage instance which is not exposed - the object will
	// only be used by the RPC server and called during start and stop. It is
	// the only instance which has access to the wrapped storage.GraphStorage.

	memberStorage, err := newMemberStorage(ds, gs)
	if err != nil {
		return nil, nil, err
	}

	// Register handler function for RPC calls and for closing the local storage

	mm.SetHandleDataRequest(memberStorage.handleDataRequest)
	ds.localDRHandler = memberStorage.handleDataRequest
	ds.localFlushHandler = memberStorage.gs.FlushAll
	ds.localCloseHandler = memberStorage.gs.Close

	// Set update handler

	ds.MemberManager.SetEventHandler(func() {

		// Handler for state info updates (this handler is called once the state
		// info object has been updated from the current state)

		si := mm.StateInfo()

		rfo, ok := si.Get(manager.StateInfoREPFAC)
		rf := rfo.(int)
		members, ok2 := si.Get(manager.StateInfoMEMBERS)

		if ok && ok2 {

			distTable, distTableErr := ds.DistributionTable()

			numMembers := len(members.([]string)) / 2
			numFailedPeers := len(mm.Client.FailedPeers())

			// Check if the cluster is operational

			if distTableErr == nil && rf > 0 && numFailedPeers > rf-1 {

				// Cluster is not operational

				if distTable != nil {

					err := fmt.Errorf("Too many members failed (total: %v, failed: %v, replication: %v)",
						numMembers, numFailedPeers, rf)

					mm.LogInfo("Storage disabled:", err.Error())

					ds.SetDistributionTableError(err)
				}

				return
			}

			// Check if the replication factor has changed or the amount of members

			if distTable == nil ||
				numMembers != len(distTable.Members()) ||
				rf != distTable.repFac {

				// Try to renew the distribution table

				if dt, err := NewDistributionTable(mm.Members(), rf); err == nil {
					ds.SetDistributionTable(dt)
				}

			}
		}

	}, memberStorage.transferWorker)

	return ds, memberStorage, DSRetNew
}

/*
Start starts the distributed storage.
*/
func (ds *DistributedStorage) Start() error {
	return ds.MemberManager.Start()
}

/*
Close closes the distributed storage.
*/
func (ds *DistributedStorage) Close() error {
	ds.MemberManager.Shutdown()
	return ds.localCloseHandler()
}

/*
IsOperational returns if this distribution storage is operational
*/
func (ds *DistributedStorage) IsOperational() bool {
	ds.distributionTableLock.Lock()
	defer ds.distributionTableLock.Unlock()

	return ds.distributionTableError == nil && ds.distributionTable != nil
}

/*
DistributionTable returns the current distribution table or an error if the
storage is not available.
*/
func (ds *DistributedStorage) DistributionTable() (*DistributionTable, error) {
	ds.distributionTableLock.Lock()
	defer ds.distributionTableLock.Unlock()

	return ds.distributionTable, ds.distributionTableError
}

/*
SetDistributionTable sets the distribution table and clears any error.
*/
func (ds *DistributedStorage) SetDistributionTable(dt *DistributionTable) {
	ds.distributionTableLock.Lock()
	defer ds.distributionTableLock.Unlock()

	ds.distributionTable = dt
	ds.distributionTableError = nil
}

/*
SetDistributionTableError records an distribution table related error. This
clears the current distribution table.
*/
func (ds *DistributedStorage) SetDistributionTableError(err error) {
	ds.distributionTableLock.Lock()
	defer ds.distributionTableLock.Unlock()
	ds.distributionTable = nil
	ds.distributionTableError = newError(err)
}

/*
sendDataRequest is used to send data requests into the cluster.
*/
func (ds *DistributedStorage) sendDataRequest(member string, request *DataRequest) (interface{}, error) {

	// Check if the request should be handled locally

	if member == ds.MemberManager.Name() {

		// Make sure to copy the request value for local insert or update requests.
		// This is necessary since the serialization buffers are pooled and never
		// dismissed. Locally the values are just passed around.

		if request.RequestType == RTInsert || request.RequestType == RTUpdate {
			var val []byte
			datautil.CopyObject(request.Value, &val)
			request.Value = val
		}

		var response interface{}
		err := ds.localDRHandler(request, &response)
		return response, err
	}

	return ds.MemberManager.Client.SendDataRequest(member, request)
}

/*
Name returns the name of the cluster DistributedStorage instance.
*/
func (ds *DistributedStorage) Name() string {
	return ds.MemberManager.Name()
}

/*
LocalName returns the local name of the wrapped DistributedStorage instance.
*/
func (ds *DistributedStorage) LocalName() string {
	return ds.localName
}

/*
ReplicationFactor returns the replication factor of this cluster member. A
value of 0 means the cluster is not operational in the moment.
*/
func (ds *DistributedStorage) ReplicationFactor() int {

	// Do not do anything is the cluster is not operational

	distTable, distTableErr := ds.DistributionTable()

	if distTableErr != nil {
		return 0
	}

	return distTable.repFac
}

/*
MainDB returns the main database. The main database is a quick
lookup map for meta data which is always kept in memory.
*/
func (ds *DistributedStorage) MainDB() map[string]string {
	ret := make(map[string]string)

	// Clear the current mainDB cache

	ds.mainDB = nil

	// Do not do anything is the cluster is not operational

	distTable, distTableErr := ds.DistributionTable()

	if distTableErr != nil {
		ds.mainDBError = distTableErr
		return ret
	}

	// Main db requests always go to member 1

	member := distTable.Members()[0]

	request := &DataRequest{RTGetMain, nil, nil, false}

	mainDB, err := ds.sendDataRequest(member, request)

	if err != nil {

		// Cycle through all replicating members if there was an error.
		// (as long as the cluster is considered operational there must be a
		// replicating member available to accept the request)

		for _, rmember := range distTable.Replicas(member) {
			mainDB, err = ds.sendDataRequest(rmember, request)

			if err == nil {
				break
			}
		}
	}

	ds.mainDBError = err

	if mainDB != nil {
		ds.mainDB = mainDB.(map[string]string)
		ret = ds.mainDB
	}

	// We failed to get the main db - any flush will fail.

	return ret
}

/*
RollbackMain rollback the main database.
*/
func (ds *DistributedStorage) RollbackMain() error {

	// Nothing to do here - the main db will be updated next time it is requested

	ds.mainDB = nil
	ds.mainDBError = nil

	return nil
}

/*
FlushMain writes the main database to the storage.
*/
func (ds *DistributedStorage) FlushMain() error {

	// Make sure there is no error

	distTable, distTableErr := ds.DistributionTable()

	if ds.mainDBError != nil {
		return ds.mainDBError
	} else if distTableErr != nil {
		return distTableErr
	}

	// Main db requests always go to member 1

	member := distTable.Members()[0]

	request := &DataRequest{RTSetMain, nil, ds.mainDB, false}

	_, err := ds.sendDataRequest(member, request)

	if err != nil {

		// Cycle through all replicating members if there was an error.
		// (as long as the cluster is considered operational there must be a
		// replicating member available to accept the request)

		for _, rmember := range distTable.Replicas(member) {
			_, err = ds.sendDataRequest(rmember, request)

			if err == nil {
				break
			}
		}
	}

	return err
}

/*
FlushAll writes all pending local changes to the storage.
*/
func (ds *DistributedStorage) FlushAll() error {
	return ds.localFlushHandler()
}

/*
StorageManager gets a storage manager with a certain name. A non-exisClusterting StorageManager
is not created automatically if the create flag is set to false.
*/
func (ds *DistributedStorage) StorageManager(smname string, create bool) storage.Manager {

	// Make sure there is no error

	distTable, distTableErr := ds.DistributionTable()

	if ds.mainDBError != nil {
		return nil
	} else if distTableErr != nil {
		return nil
	}

	if !create {

		// Make sure the storage manage exists if it should not be created.
		// Try to get its 1st root value. If nil is returned then the storage
		// manager does not exist.

		// Root ids always go to member 1 as well as the first insert request for data.

		member := distTable.Members()[0]

		request := &DataRequest{RTGetRoot, map[DataRequestArg]interface{}{
			RPStoreName: smname,
			RPRoot:      1,
		}, nil, false}

		res, err := ds.sendDataRequest(member, request)

		if res == nil && err == nil {
			return nil
		}
	}

	return &DistributedStorageManager{smname, 0, ds, nil}
}
