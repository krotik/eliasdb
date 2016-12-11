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
	"net/rpc"

	"devt.de/common/errorutil"
)

func init() {

	// Create singleton Server instance.

	server = &Server{make(map[string]*MemberManager)}

	// Register the cluster API as RPC server

	errorutil.AssertOk(rpc.Register(server))
}

/*
RPCFunction is used to identify the called function in a RPC call
*/
type RPCFunction string

/*
List of all possible RPC functions. The list includes all RPC callable functions
in this file.
*/
const (

	// General functions

	RPCPing      RPCFunction = "Ping"
	RPCSIRequest             = "StateInfoRequest"
	RPCMIRequest             = "MemberInfoRequest"

	// Cluster-wide locking

	RPCAcquireLock = "AcquireLock"
	RPCReleaseLock = "ReleaseLock"

	// Cluster member management

	RPCJoinCluster = "JoinCluster"
	RPCAddMember   = "AddMember"
	RPCEjectMember = "EjectMember"

	// StateInfo functions

	RPCUpdateStateInfo = "UpdateStateInfo"

	// Data request functions

	RPCDataRequest = "DataRequest"
)

/*
RequestArgument is used to identify arguments in a RPC call
*/
type RequestArgument int

/*
List of all possible arguments in a RPC request. There are usually no checks which
give back an error if a required argument is missing. The RPC API is an internal
API and might change without backwards compatibility.
*/
const (

	// General arguments

	RequestTARGET       RequestArgument = iota // Required argument which identifies the target cluster memeber
	RequestTOKEN                               // Client token which is used for authorization checks
	RequestLOCK                                // Lock name which a member requests to take
	RequestMEMBERNAME                          // Name for a member
	RequestMEMBERRPC                           // Rpc address and port for a member
	RequestSTATEINFOMAP                        // StateInfo object as a map
	RequestDATA                                // Data request object
)

/*
server is the Server instance which serves rpc calls
*/
var server *Server

/*
Server is the RPC exposed cluster API of a cluster member. Server
is a singleton and will route incoming (authenticated) requests to registered
MemberManagers. The calling member is referred to as source member and the called
member is referred to as target member.
*/
type Server struct {
	managers map[string]*MemberManager // Map of local cluster members
}

// General functions
// =================

/*
Ping answers with a Pong if the given client token was verified and the local
cluster member exists.
*/
func (ms *Server) Ping(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, false)
	if err != nil {
		return err
	}

	// Send a simple response

	res := []string{"Pong"}

	// Check if request is from a cluster member - only reveal timestamps
	// to members

	token := request[RequestTOKEN].(*MemberToken)

	if _, ok := manager.Client.peers[token.MemberName]; ok {

		ts, _ := manager.stateInfo.Get(StateInfoTS)

		res = append(res, ts.([]string)...)

		tsold, _ := manager.stateInfo.Get(StateInfoTSOLD)

		res = append(res, tsold.([]string)...)
	}

	*response = res

	return nil
}

/*
StateInfoRequest answers with the member's state info.
*/
func (ms *Server) StateInfoRequest(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, false)
	if err != nil {
		return err
	}

	*response = mapToBytes(manager.stateInfo.Map())

	return nil
}

/*
MemberInfoRequest answers with the member's static info.
*/
func (ms *Server) MemberInfoRequest(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, false)
	if err != nil {
		return err
	}

	*response = mapToBytes(manager.memberInfo)

	return nil
}

// Cluster membership functions
// ============================

/*
JoinCluster is used by a new member if it wants to join the cluster.
*/
func (ms *Server) JoinCluster(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, false)
	if err != nil {
		return err
	}

	newMemberName := request[RequestMEMBERNAME].(string)
	newMemberRPC := request[RequestMEMBERRPC].(string)

	err = manager.JoinNewMember(newMemberName, newMemberRPC)

	if err == nil {

		// Return updated state info if there was no error

		*response = mapToBytes(manager.stateInfo.Map())
	}

	return err
}

/*
AddMember adds a new member on the target member.
*/
func (ms *Server) AddMember(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, true)
	if err != nil {
		return err
	}

	// Acquire lock to modify client map

	newMemberName := request[RequestMEMBERNAME].(string)
	newMemberRPC := request[RequestMEMBERRPC].(string)
	newStateInfo := bytesToMap(request[RequestSTATEINFOMAP].([]byte))

	return manager.addMember(newMemberName, newMemberRPC, newStateInfo)
}

/*
EjectMember can be called by a cluster member to eject itself or another cluster member.
*/
func (ms *Server) EjectMember(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, true)
	if err != nil {
		return err
	}

	memberToEject := request[RequestMEMBERNAME].(string)

	return manager.EjectMember(memberToEject)
}

// Cluster-wide locking
// ====================

/*
AcquireLock tries to acquire a named lock for the source member on the
target member. It fails if the lock is alread acquired by a different member.
The lock can only be held for a limited amount of time.
*/
func (ms *Server) AcquireLock(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, true)
	if err != nil {
		return err
	}

	// Acquire lock to modify lock map

	manager.Client.maplock.Lock()
	manager.Client.maplock.Unlock()

	requestedLock := request[RequestLOCK].(string)
	sourceMember := request[RequestTOKEN].(*MemberToken).MemberName

	// Get the lock owner

	lockOwner, ok := manager.Client.clusterLocks.Get(requestedLock)

	if ok && lockOwner != sourceMember {

		// If there is already an owner return an error which mentions the owner

		return &Error{ErrLockTaken, lockOwner.(string)}
	}

	// If there is no owner set the source client as the new owner

	manager.Client.clusterLocks.Put(requestedLock, sourceMember)

	*response = sourceMember

	return nil
}

/*
ReleaseLock releases a lock. Only the member which holds the lock can release it.
*/
func (ms *Server) ReleaseLock(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, true)
	if err != nil {
		return err
	}

	// Acquire lock to modify lock map

	manager.Client.maplock.Lock()
	defer manager.Client.maplock.Unlock()

	requestedLock := request[RequestLOCK].(string)
	sourceMember := request[RequestTOKEN].(*MemberToken).MemberName

	// Get the lock owner

	lockOwner, ok := manager.Client.clusterLocks.Get(requestedLock)

	if ok {

		if lockOwner == sourceMember {

			// Release lock

			manager.Client.clusterLocks.Remove(requestedLock)

		} else {

			// Lock is owned by someone else

			return &Error{ErrLockNotOwned, fmt.Sprintf("Owned by %v not by %v",
				lockOwner, sourceMember)}
		}
	}

	// Operation on a non-existing lock is a NOP

	return nil
}

// StateInfo functions
// ===================

/*
UpdateStateInfo updates the state info of the target member.
*/
func (ms *Server) UpdateStateInfo(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, true)
	if err != nil {
		return err
	}

	newStateInfo := bytesToMap(request[RequestSTATEINFOMAP].([]byte))

	return manager.applyStateInfo(newStateInfo)
}

// Data request functions
// ======================

/*
DataRequest handles a data request.
*/
func (ms *Server) DataRequest(request map[RequestArgument]interface{},
	response *interface{}) error {

	// Verify the given token and retrieve the target member

	manager, err := ms.checkToken(request, true)
	if err != nil {
		return err
	}

	// Handle the data request

	reqdata := request[RequestDATA]

	return manager.handleDataRequest(reqdata, response)
}

// Helper functions
// ================

/*
checkToken checks the member token in a given request.
*/
func (ms *Server) checkToken(request map[RequestArgument]interface{},
	checkClusterMembership bool) (*MemberManager, error) {

	// Get the target member

	target := request[RequestTARGET].(string)
	token := request[RequestTOKEN].(*MemberToken)

	if manager, ok := ms.managers[target]; ok {

		// Generate expected auth from given requesting member name in token and secret of target

		expectedAuth := fmt.Sprintf("%X", sha512.Sum512_224([]byte(token.MemberName+manager.secret)))

		if token.MemberAuth == expectedAuth {

			if checkClusterMembership {

				// Check if the requesting client is actually a member of the cluster

				manager.Client.maplock.Lock()
				_, ok := manager.Client.peers[token.MemberName]
				manager.Client.maplock.Unlock()

				if !ok {
					return nil, ErrNotMember
				}
			}

			return manager, nil
		}

		return nil, ErrInvalidToken
	}

	return nil, ErrUnknownTarget
}
