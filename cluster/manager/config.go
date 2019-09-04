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
Package manager contains the management code for EliasDB's clustering feature.

The management code deals with cluster building, general communication between cluster
members, verification of communicating peers and monitoring of members.

The cluster structure is pure peer-to-peer design with no single point of failure. All
members of the cluster share a versioned cluster state which is persisted. Members have
to manually be added or removed from the cluster. Each member also has a member info object
which can be used by the application which uses the cluster to store additional member
related information.

Temporary failures are detected automatically. Every member of the cluster monitors the
state of all its peers by sending ping requests to them on a regular schedule.
*/
package manager

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/eliasdb/storage"
)

// Cluster config
// ==============

/*
ConfigRPC is the PRC network interface for the local cluster manager
*/
const ConfigRPC = "ClusterMemberRPC"

/*
ConfigMemberName is the name of the cluster member
*/
const ConfigMemberName = "ClusterMemberName"

/*
ConfigClusterSecret is the secret which authorizes a cluster member
(the secret must never be send directly over the network)
*/
const ConfigClusterSecret = "ClusterSecret"

/*
ConfigReplicationFactor is the number of times a given datum must be stored
redundently. The cluster can suffer n-1 member losses before it becomes
inoperational. The value is set once in the configuration and becomes afterwards
part of the global cluster state info (once this is there the config value is ignored).
*/
const ConfigReplicationFactor = "ReplicationFactor"

/*
DefaultConfig is the defaut configuration
*/
var DefaultConfig = map[string]interface{}{
	ConfigRPC:               "127.0.0.1:9030",
	ConfigMemberName:        "member1",
	ConfigClusterSecret:     "secret123",
	ConfigReplicationFactor: 1.0,
}

// Cluster state info
// ==================

/*
Known StateInfo entries
*/
const (
	StateInfoTS      = "ts"          // Timestamp of state info
	StateInfoTSOLD   = "tsold"       // Previous timestamp of state info
	StateInfoMEMBERS = "members"     // List of known cluster members
	StateInfoFAILED  = "failed"      // List of failed peers
	StateInfoREPFAC  = "replication" // Replication factor of the cluster
)

/*
Known MemberInfo entries
*/
const (
	MemberInfoError   = "error"   // Error message if a member was not reachable
	MemberInfoTermURL = "termurl" // URL to the cluster terminal of the member
)

/*
StateInfo models a state object which stores cluster related data. This
information is exchanged between cluster members. It is not expected that
the info changes frequently.
*/
type StateInfo interface {

	/*
		Put stores some data in the state info.
	*/
	Put(key string, value interface{})

	/*
		Get retrievtes some data from the state info.
	*/
	Get(key string) (interface{}, bool)

	/*
		Map returns the state info as a map.
	*/
	Map() map[string]interface{}

	/*
		Flush persists the state info.
	*/
	Flush() error
}

/*
DefaultStateInfo is the default state info which uses a file to persist its data.
*/
type DefaultStateInfo struct {
	*datautil.PersistentMap
	datalock *sync.RWMutex
}

/*
NewDefaultStateInfo creates a new DefaultStateInfo.
*/
func NewDefaultStateInfo(filename string) (StateInfo, error) {
	var pm *datautil.PersistentMap
	var err error

	if res, _ := fileutil.PathExists(filename); !res {

		pm, err = datautil.NewPersistentMap(filename)
		if err != nil {
			return nil, &Error{ErrClusterConfig,
				fmt.Sprintf("Cannot create state info file %v: %v",
					filename, err.Error())}
		}

	} else {

		pm, err = datautil.LoadPersistentMap(filename)
		if err != nil {
			return nil, &Error{ErrClusterConfig,
				fmt.Sprintf("Cannot load state info file %v: %v",
					filename, err.Error())}
		}
	}

	return &DefaultStateInfo{pm, &sync.RWMutex{}}, nil
}

/*
Map returns the state info as a map.
*/
func (dsi *DefaultStateInfo) Map() map[string]interface{} {
	var ret map[string]interface{}
	datautil.CopyObject(dsi.Data, &ret)
	return ret
}

/*
Get retrieves some data from the state info.
*/
func (dsi *DefaultStateInfo) Get(key string) (interface{}, bool) {
	dsi.datalock.RLock()
	defer dsi.datalock.RUnlock()
	v, ok := dsi.Data[key]
	return v, ok
}

/*
Put stores some data in the state info.
*/
func (dsi *DefaultStateInfo) Put(key string, value interface{}) {
	dsi.datalock.Lock()
	defer dsi.datalock.Unlock()
	dsi.Data[key] = value
}

/*
Flush persists the state info.
*/
func (dsi *DefaultStateInfo) Flush() error {
	if err := dsi.PersistentMap.Flush(); err != nil {
		return &Error{ErrClusterConfig,
			fmt.Sprintf("Cannot persist state info: %v",
				err.Error())}
	}
	return nil
}

/*
MsiRetFlush nil or the error which should be returned by a Flush call
*/
var MsiRetFlush error

/*
MemStateInfo is a state info object which does not persist its data.
*/
type MemStateInfo struct {
	data     map[string]interface{}
	datalock *sync.RWMutex
}

/*
NewMemStateInfo creates a new MemStateInfo.
*/
func NewMemStateInfo() StateInfo {
	return &MemStateInfo{make(map[string]interface{}), &sync.RWMutex{}}
}

/*
Map returns the state info as a map.
*/
func (msi *MemStateInfo) Map() map[string]interface{} {
	var ret map[string]interface{}
	datautil.CopyObject(msi.data, &ret)
	return ret
}

/*
Get retrieves some data from the state info.
*/
func (msi *MemStateInfo) Get(key string) (interface{}, bool) {
	msi.datalock.RLock()
	defer msi.datalock.RUnlock()
	v, ok := msi.data[key]
	return v, ok
}

/*
Put stores some data in the state info.
*/
func (msi *MemStateInfo) Put(key string, value interface{}) {
	msi.datalock.Lock()
	defer msi.datalock.Unlock()
	msi.data[key] = value
}

/*
Flush does not do anything :-)
*/
func (msi *MemStateInfo) Flush() error {
	return MsiRetFlush
}

// Helper functions to properly serialize maps
// ===========================================

/*
mapToBytes converts a given map to bytes. This method panics on errors.
*/
func mapToBytes(m map[string]interface{}) []byte {
	bb := storage.BufferPool.Get().(*bytes.Buffer)
	defer func() {
		bb.Reset()
		storage.BufferPool.Put(bb)
	}()

	errorutil.AssertOk(gob.NewEncoder(bb).Encode(m))

	return bb.Bytes()
}

/*
bytesToMap tries to convert a given byte array into a map. This method panics on errors.
*/
func bytesToMap(b []byte) map[string]interface{} {
	var ret map[string]interface{}

	errorutil.AssertOk(gob.NewDecoder(bytes.NewReader(b)).Decode(&ret))

	return ret
}
