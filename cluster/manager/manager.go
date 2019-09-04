/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package manager

import (
	"crypto/sha512"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"time"

	"devt.de/krotik/common/datautil"
)

/*
MemberManager is the management object for a cluster member.

This is the main object of the clustering code it contains the main API.
A member registers itself to the rpc server which is the global
ManagerServer (server) object. Each cluster member needs to have a unique name.
Communication between members is secured by using a secret string which
is never exchanged over the network and a hash generated token which
identifies a member.

Each MemberManager object contains a Client object which can be used to
communicate with other cluster members. This object should be used by pure
clients - code which should communicate with the cluster without running an
actual member.

*/
type MemberManager struct {
	name   string // Name of the cluster member
	secret string // Cluster secret

	stateInfo        StateInfo              // StateInfo object which can persist runtime configuration
	memberInfo       map[string]interface{} // Static info about this member
	housekeeping     bool                   // Housekeeping thread running
	housekeepingLock *sync.Mutex            // Lock for housekeeping (prevent housekeeping from running)
	StopHousekeeping bool                   // Flag to temporarily stop housekeeping

	handleDataRequest func(interface{}, *interface{}) error // Handler for cluster data requests

	notifyStateUpdate  func() // Handler which is called when the state info is updated
	notifyHouseKeeping func() // Handler which is called each time the housekeeping thread has finished

	Client   *Client        // RPC client object
	listener net.Listener   // RPC server listener
	wg       sync.WaitGroup // RPC server Waitgroup for listener shutdown
}

/*
NewMemberManager create a new MemberManager object.
*/
func NewMemberManager(rpcInterface string, name string, secret string, stateInfo StateInfo) *MemberManager {

	// Generate member token

	token := &MemberToken{name, fmt.Sprintf("%X", sha512.Sum512_224([]byte(name+secret)))}

	// By default a client can hold a lock for up to 30 seconds before it is cleared.

	mm := &MemberManager{name, secret, stateInfo, make(map[string]interface{}),
		false, &sync.Mutex{}, false, func(interface{}, *interface{}) error { return nil }, func() {}, func() {},
		&Client{token, rpcInterface, make(map[string]string), make(map[string]*rpc.Client),
			make(map[string]string), &sync.RWMutex{}, datautil.NewMapCache(0, 30)},
		nil, sync.WaitGroup{}}

	// Check if given state info should be initialized or applied

	if _, ok := stateInfo.Get(StateInfoTS); !ok {
		mm.updateStateInfo(true)
	} else {
		mm.applyStateInfo(stateInfo.Map())
	}

	return mm
}

// General cluster member API
// ==========================

/*
Start starts the manager process for this cluster member.
*/
func (mm *MemberManager) Start() error {

	mm.LogInfo("Starting member manager ", mm.name, " rpc server on: ", mm.Client.rpc)

	l, err := net.Listen("tcp", mm.Client.rpc)
	if err != nil {
		return err
	}

	go func() {
		rpc.Accept(l)
		mm.wg.Done()
		mm.LogInfo("Connection closed: ", mm.Client.rpc)
	}()

	mm.listener = l

	server.managers[mm.name] = mm

	if runHousekeeping {

		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)

		// Start housekeeping thread which will check for configuration changes

		mm.housekeeping = true
		go func() {
			for mm.housekeeping {
				mm.HousekeepingWorker()
				time.Sleep(time.Duration(FreqHousekeeping*(1+r1.Float64())) * time.Millisecond)
			}
			mm.wg.Done()
		}()
	}

	return nil
}

/*
Shutdown shuts the member manager rpc server for this cluster member down.
*/
func (mm *MemberManager) Shutdown() error {

	// Stop housekeeping

	if mm.housekeeping {
		mm.wg.Add(1)
		mm.housekeeping = false
		mm.wg.Wait()
		mm.LogInfo("Housekeeping stopped")
	}

	// Close socket

	if mm.listener != nil {
		mm.LogInfo("Shutdown rpc server on: ", mm.Client.rpc)
		mm.wg.Add(1)
		mm.listener.Close()
		mm.listener = nil
		mm.wg.Wait()
	} else {
		LogDebug("Member manager ", mm.name, " already shut down")
	}

	return nil
}

/*
LogInfo logs a member related message at info level.
*/
func (mm *MemberManager) LogInfo(v ...interface{}) {
	LogInfo(mm.name, ": ", fmt.Sprint(v...))
}

/*
Name returns the member name.
*/
func (mm *MemberManager) Name() string {
	return mm.name
}

/*
NetAddr returns the network address of the member.
*/
func (mm *MemberManager) NetAddr() string {
	return mm.Client.rpc
}

/*
Members returns a list of all cluster members.
*/
func (mm *MemberManager) Members() []string {
	var ret []string

	siMembers, _ := mm.stateInfo.Get(StateInfoMEMBERS)
	members := siMembers.([]string)

	for i := 0; i < len(members); i += 2 {
		ret = append(ret, members[i])
	}

	sort.Strings(ret)

	return ret
}

/*
StateInfo returns the current state info.
*/
func (mm *MemberManager) StateInfo() StateInfo {
	return mm.stateInfo
}

/*
MemberInfo returns the current static member info. Clients may modify the
returned map. Member info can be used to store additional information
on every member (e.g. a member specific URL).
*/
func (mm *MemberManager) MemberInfo() map[string]interface{} {
	return mm.memberInfo
}

/*
SetEventHandler sets event handler funtions which are called when the state info
is updated or when housekeeping has been done.
*/
func (mm *MemberManager) SetEventHandler(notifyStateUpdate func(), notifyHouseKeeping func()) {
	mm.notifyStateUpdate = notifyStateUpdate
	mm.notifyHouseKeeping = notifyHouseKeeping
}

/*
SetHandleDataRequest sets the data request handler.
*/
func (mm *MemberManager) SetHandleDataRequest(handleDataRequest func(interface{}, *interface{}) error) {
	mm.handleDataRequest = handleDataRequest
}

/*
MemberInfoCluster returns the current static member info for every known
cluster member. This calls every member in the cluster.
*/
func (mm *MemberManager) MemberInfoCluster() map[string]map[string]interface{} {

	clusterMemberInfo := make(map[string]map[string]interface{})

	clusterMemberInfo[mm.name] = mm.MemberInfo()

	for p := range mm.Client.peers {

		mi, err := mm.Client.SendMemberInfoRequest(p)

		if err != nil {
			clusterMemberInfo[p] = map[string]interface{}{MemberInfoError: err.Error()}
		} else {
			clusterMemberInfo[p] = mi
		}
	}

	return clusterMemberInfo
}

// Cluster membership functions
// ============================

/*
JoinCluster lets this member try to join an existing cluster. The secret must
be correct otherwise the member will be rejected.
*/
func (mm *MemberManager) JoinCluster(newMemberName string, newMemberRPC string) error {

	// Housekeeping should not be running while joining a cluster

	mm.housekeepingLock.Lock()
	defer mm.housekeepingLock.Unlock()

	res, err := mm.Client.SendJoinCluster(newMemberName, newMemberRPC)

	if err == nil {

		// Update the state info of this member if the join was successful

		mm.applyStateInfo(res)
	}

	return err
}

/*
JoinNewMember joins a new member to the current cluster. It is assumed that
the new members token has already been verified.
*/
func (mm *MemberManager) JoinNewMember(newMemberName string, newMemberRPC string) error {

	// Acquire cluster lock for updating the state info

	if err := mm.Client.SendAcquireClusterLock(ClusterLockUpdateStateInfo); err != nil {
		return err
	}

	// Get operational peers (operational cluster is NOT required - other members should
	// update eventually)

	peers, _ := mm.Client.OperationalPeers()

	mm.LogInfo("Adding member ", newMemberName, " with rpc ", newMemberRPC, " to the cluster")

	// Add member to local state info

	if err := mm.addMember(newMemberName, newMemberRPC, nil); err != nil {

		// Try to release the cluster lock if something went wrong at this point

		mm.Client.SendReleaseClusterLock(ClusterLockUpdateStateInfo)

		return err
	}

	// Add member to all other cluster members (ignore failures - failed members
	// should be updated eventually by the BackgroundWorker)

	for _, p := range peers {
		mm.Client.SendRequest(p, RPCAddMember, map[RequestArgument]interface{}{
			RequestMEMBERNAME:   newMemberName,
			RequestMEMBERRPC:    newMemberRPC,
			RequestSTATEINFOMAP: mapToBytes(mm.stateInfo.Map()),
		})
	}

	// Release cluster lock for updating the state info

	return mm.Client.SendReleaseClusterLock(ClusterLockUpdateStateInfo)
}

/*
EjectMember ejects a member from the current cluster. Trying to remove a non-existent
member has no effect.
*/
func (mm *MemberManager) EjectMember(memberToEject string) error {
	var err error

	// Get operational peers (operational cluster is NOT required - other members should
	// update eventually)

	peers, _ := mm.Client.OperationalPeers()

	// Check if the given member name is valid - it must be a peer or this member

	if memberToEjectRPC, ok := mm.Client.peers[memberToEject]; ok {

		// Acquire cluster lock for updating the state info

		if err := mm.Client.SendAcquireClusterLock(ClusterLockUpdateStateInfo); err != nil {
			return err
		}

		mm.LogInfo("Ejecting member ", memberToEject, " from the cluster")

		mm.Client.maplock.Lock()
		delete(mm.Client.peers, memberToEject)
		delete(mm.Client.conns, memberToEject)
		delete(mm.Client.failed, memberToEject)
		mm.Client.maplock.Unlock()

		if err := mm.updateStateInfo(true); err != nil {

			// Put the member to eject back into the peers map

			mm.Client.peers[memberToEject] = memberToEjectRPC

			// Try to release the cluster lock if something went wrong at this point

			mm.Client.SendReleaseClusterLock(ClusterLockUpdateStateInfo)

			return err
		}

		// Send the state info to all other cluster members (ignore failures - failed members
		// should be updated eventually by the BackgroundWorker)

		for _, k := range peers {
			mm.Client.SendRequest(k, RPCUpdateStateInfo, map[RequestArgument]interface{}{
				RequestSTATEINFOMAP: mapToBytes(mm.stateInfo.Map()),
			})
		}

		// Release cluster lock for updating the state info

		err = mm.Client.SendReleaseClusterLock(ClusterLockUpdateStateInfo)

	} else if mm.name == memberToEject {

		// If we should eject ourselves then forward the request

		mm.LogInfo("Ejecting this member from the cluster")

		if len(peers) > 0 {
			if err := mm.Client.SendEjectMember(peers[0], mm.name); err != nil {
				return err
			}
		}

		// Clear peer maps and update the cluster state

		mm.Client.maplock.Lock()
		mm.Client.peers = make(map[string]string)
		mm.Client.conns = make(map[string]*rpc.Client)
		mm.Client.failed = make(map[string]string)
		mm.Client.maplock.Unlock()

		err = mm.updateStateInfo(true)
	}

	return err
}

// StateInfo functions
// ===================

/*
UpdateClusterStateInfo updates the members state info and sends it to all members in
the cluster.
*/
func (mm *MemberManager) UpdateClusterStateInfo() error {

	// Get operational peers - fail if the cluster is not operational

	peers, err := mm.Client.OperationalPeers()
	if err != nil {
		return err
	}

	// Acquire cluster lock for updating the state info

	if err := mm.Client.SendAcquireClusterLock(ClusterLockUpdateStateInfo); err != nil {
		return err
	}

	mm.LogInfo("Updating cluster state info")

	if err := mm.updateStateInfo(true); err != nil {

		// Try to release the cluster lock if something went wrong at this point

		mm.Client.SendReleaseClusterLock(ClusterLockUpdateStateInfo)

		return err
	}

	// Send the state info to all other cluster members (ignore failures - failed members
	// should be updated eventually by the BackgroundWorker)

	for _, k := range peers {
		mm.Client.SendRequest(k, RPCUpdateStateInfo, map[RequestArgument]interface{}{
			RequestSTATEINFOMAP: mapToBytes(mm.stateInfo.Map()),
		})
	}

	// Release cluster lock for updating the state info

	return mm.Client.SendReleaseClusterLock(ClusterLockUpdateStateInfo)
}

// Helper functions
// ================

/*
addMember adds a new member to the local state info.
*/
func (mm *MemberManager) addMember(newMemberName string, newMemberRPC string,
	newStateInfo map[string]interface{}) error {

	// Check if member exists already

	if _, ok := mm.Client.peers[newMemberName]; ok {
		return &Error{ErrClusterConfig,
			fmt.Sprintf("Cannot add member %v as a member with the same name exists already",
				newMemberName)}
	}

	// Add new peer to peer map - member.Client.conns will be updated on the
	// first connection

	mm.Client.maplock.Lock()
	mm.Client.peers[newMemberName] = newMemberRPC
	mm.Client.maplock.Unlock()

	// Store the new state or just update the state

	if newStateInfo != nil {
		return mm.applyStateInfo(newStateInfo)
	}

	return mm.updateStateInfo(true)
}

/*
updateStateInfo updates the StateInfo from the current runtime state.
Only updates the timestamp if newTS is true.
*/
func (mm *MemberManager) updateStateInfo(newTS bool) error {

	sortMapKeys := func(m map[string]string) []string {
		var ks []string
		for k := range m {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		return ks
	}

	// Populate members entry

	members := make([]string, 0, len(mm.Client.peers)*2)

	// Add this member to the state info

	members = append(members, mm.name)
	members = append(members, mm.Client.rpc)

	// Add other known members to the state info

	mm.Client.maplock.Lock()

	for _, name := range sortMapKeys(mm.Client.peers) {
		rpc := mm.Client.peers[name]
		members = append(members, name)
		members = append(members, rpc)
	}

	mm.stateInfo.Put(StateInfoMEMBERS, members)

	failed := make([]string, 0, len(mm.Client.failed)*2)

	// Add all known failed members to the state info

	for _, name := range sortMapKeys(mm.Client.failed) {
		errstr := mm.Client.failed[name]
		failed = append(failed, name)
		failed = append(failed, errstr)
	}

	mm.Client.maplock.Unlock()

	mm.stateInfo.Put(StateInfoFAILED, failed)

	// Check for replication factor entry - don't touch if it is set

	if _, ok := mm.stateInfo.Get(StateInfoREPFAC); !ok {
		mm.stateInfo.Put(StateInfoREPFAC, 1)
	}

	if newTS {

		// Populate old timestamp and timestamp

		newOldTS, ok := mm.stateInfo.Get(StateInfoTS)
		if !ok {
			newOldTS = []string{"", "0"}
		}
		mm.stateInfo.Put(StateInfoTSOLD, newOldTS)

		v, _ := strconv.ParseInt(newOldTS.([]string)[1], 10, 64)
		mm.stateInfo.Put(StateInfoTS, []string{mm.name, fmt.Sprint(v + 1)})
	}

	err := mm.stateInfo.Flush()

	if err == nil {

		// Notify others of the state update

		mm.notifyStateUpdate()
	}

	return err
}

/*
applyStateInfo sets the runtime state from the given StateInfo map.
*/
func (mm *MemberManager) applyStateInfo(stateInfoMap map[string]interface{}) error {

	// Set peers entry

	mm.applyStateInfoPeers(stateInfoMap, true)

	// Set failed entry

	mm.Client.maplock.Lock()

	mm.Client.failed = make(map[string]string)

	siFailed, _ := stateInfoMap[StateInfoFAILED]
	failed := siFailed.([]string)

	for i := 0; i < len(failed); i += 2 {
		mm.Client.failed[failed[i]] = failed[i+1]
	}

	mm.Client.maplock.Unlock()

	// Set give replication factor entry

	mm.stateInfo.Put(StateInfoREPFAC, stateInfoMap[StateInfoREPFAC])

	// Set given timestamp

	mm.stateInfo.Put(StateInfoTS, stateInfoMap[StateInfoTS])
	mm.stateInfo.Put(StateInfoTSOLD, stateInfoMap[StateInfoTSOLD])

	// Set state info

	return mm.updateStateInfo(false)
}

/*
applyStateInfoPeers sets the peer related runtime state from the given StateInfo map.
*/
func (mm *MemberManager) applyStateInfoPeers(stateInfoMap map[string]interface{}, replaceExisting bool) {

	// Set peers entry

	if replaceExisting {
		mm.Client.maplock.Lock()
		mm.Client.peers = make(map[string]string)
		mm.Client.maplock.Unlock()
	}

	siMembers, _ := stateInfoMap[StateInfoMEMBERS]
	members := siMembers.([]string)

	for i := 0; i < len(members); i += 2 {

		// Do not add this member as peer

		if members[i] != mm.name {
			mm.Client.maplock.Lock()
			mm.Client.peers[members[i]] = members[i+1]
			mm.Client.maplock.Unlock()
		}
	}
}
