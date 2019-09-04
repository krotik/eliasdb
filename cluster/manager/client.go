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
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"

	"devt.de/krotik/common/datautil"
)

func init() {

	// Make sure we can use the relevant types in a gob operation

	gob.Register(&MemberToken{})
}

/*
Known cluster locks
*/
const (
	ClusterLockUpdateStateInfo = "ClusterLockUpdateStateInfo"
)

/*
DialTimeout is the dial timeout for RPC connections
*/
var DialTimeout = 10 * time.Second

/*
MemberToken is used to authenticate a member in the cluster
*/
type MemberToken struct {
	MemberName string
	MemberAuth string
}

/*
Client is the client for the RPC cluster API of a cluster member.
*/
type Client struct {
	token        *MemberToken           // Token to be send to other members for authentication
	rpc          string                 // This client's rpc network interface (may be empty in case of pure clients)
	peers        map[string]string      // Map of member names to their rpc network interface
	conns        map[string]*rpc.Client // Map of member names to network connections
	failed       map[string]string      // Map of (temporary) failed members
	maplock      *sync.RWMutex          // Lock for maps
	clusterLocks *datautil.MapCache     // Cluster locks and which member holds them
}

/*
MemberErrors map for simulated member errors (only used for testing)
*/
var MemberErrors map[string]error

/*
MemberErrorExceptions map to exclude members from simulated member errors (only used for testing)
*/
var MemberErrorExceptions map[string][]string

// General cluster client API
// ==========================

/*
IsFailed checks if the given member is in the failed state.
*/
func (mc *Client) IsFailed(name string) bool {
	mc.maplock.Lock()
	defer mc.maplock.Unlock()

	_, ok := mc.failed[name]
	return ok
}

/*
FailedTotal returns the total number of failed members.
*/
func (mc *Client) FailedTotal() int {
	mc.maplock.Lock()
	defer mc.maplock.Unlock()

	return len(mc.failed)
}

/*
FailedPeers returns a list of failed members.
*/
func (mc *Client) FailedPeers() []string {
	var ret []string

	mc.maplock.Lock()
	defer mc.maplock.Unlock()

	for p := range mc.failed {
		ret = append(ret, p)
	}

	sort.Strings(ret)

	return ret
}

/*
FailedPeerErrors returns the same list as FailedPeers but with error messages.
*/
func (mc *Client) FailedPeerErrors() []string {
	var ret []string

	for _, p := range mc.FailedPeers() {
		e := mc.failed[p]
		ret = append(ret, fmt.Sprintf("%v (%v)", p, e))
	}
	return ret
}

/*
OperationalPeers returns all operational peers and an error if too many cluster members
have failed.
*/
func (mc *Client) OperationalPeers() ([]string, error) {
	var err error
	var peers []string

	mc.maplock.Lock()
	defer mc.maplock.Unlock()

	for peer := range mc.peers {
		if _, ok := mc.failed[peer]; !ok {
			peers = append(peers, peer)
		}
	}

	if len(mc.peers) > 0 && len(peers) == 0 {
		err = &Error{ErrClusterState, fmt.Sprintf("No peer cluster member is reachable")}
	} else {
		sort.Strings(peers)
	}

	return peers, err
}

/*
SendRequest sends a request to another cluster member. Not reachable members
get an entry in the failed map and the error return is ErrMemberComm. All
other error returns should be considered serious errors.
*/
func (mc *Client) SendRequest(member string, remoteCall RPCFunction,
	args map[RequestArgument]interface{}) (interface{}, error) {

	var err error

	// Function to categorize errors

	handleError := func(err error) error {

		if _, ok := err.(net.Error); ok {

			// We got a network error and the communication with a member
			// is interrupted - add the member to the failing members list

			mc.maplock.Lock()

			// Set failure state

			mc.failed[member] = err.Error()

			// Remove the connection

			delete(mc.conns, member)

			mc.maplock.Unlock()

			return &Error{ErrMemberComm, err.Error()}
		}

		// Do not wrap a cluster network error in another cluster network error

		if strings.HasPrefix(err.Error(), "ClusterError: "+ErrMemberError.Error()) {
			return err
		}

		return &Error{ErrMemberError, err.Error()}
	}

	mc.maplock.Lock()
	laddr, ok := mc.peers[member]
	mc.maplock.Unlock()

	if ok {

		// Get network connection to the member

		mc.maplock.Lock()
		conn, ok := mc.conns[member]
		mc.maplock.Unlock()

		if !ok {
			c, err := net.DialTimeout("tcp", laddr, DialTimeout)

			if err != nil {
				LogDebug(mc.token.MemberName, ": ",
					fmt.Sprintf("- %v.%v (laddr=%v err=%v)", member, remoteCall, laddr, err))
				return nil, handleError(err)
			}

			conn = rpc.NewClient(c)

			mc.maplock.Lock()
			mc.conns[member] = conn
			mc.maplock.Unlock()
		}

		// Assemble the request

		request := map[RequestArgument]interface{}{
			RequestTARGET: member,
			RequestTOKEN:  mc.token,
		}

		if args != nil {
			for k, v := range args {
				request[k] = v
			}
		}

		var response interface{}

		LogDebug(mc.token.MemberName, ": ",
			fmt.Sprintf("> %v.%v (laddr=%v)", member, remoteCall, laddr))

		if err, _ = MemberErrors[member]; err == nil || isErrorExcepted(mc.token.MemberName, member) {
			err = conn.Call("Server."+string(remoteCall), request, &response)
		}

		LogDebug(mc.token.MemberName, ": ",
			fmt.Sprintf("< %v.%v (err=%v)", member, remoteCall, err))

		if err != nil {
			return nil, handleError(err)
		}

		return response, nil
	}

	return nil, &Error{ErrUnknownPeer, member}
}

/*
SendPing sends a ping to a member and returns the result. Second argument is
optional if the target member is not a known peer. Should be an empty string
in all other cases.
*/
func (mc *Client) SendPing(member string, rpc string) ([]string, error) {

	if _, ok := mc.peers[member]; rpc != "" && !ok {

		// Add member temporary

		mc.peers[member] = rpc

		defer func() {
			mc.maplock.Lock()
			delete(mc.peers, member)
			delete(mc.conns, member)
			delete(mc.failed, member)
			mc.maplock.Unlock()
		}()
	}

	res, err := mc.SendRequest(member, RPCPing, nil)

	if res != nil {
		return res.([]string), err
	}

	return nil, err
}

// Cluster membership functions
// ============================

/*
SendJoinCluster sends a request to a cluster member to join the caller to the cluster.
Pure clients cannot use this function as this call requires the Client.rpc field to be set.
*/
func (mc *Client) SendJoinCluster(targetMember string, targetMemberRPC string) (map[string]interface{}, error) {

	// Check we are on a cluster member - pure clients will fail here

	if mc.rpc == "" {
		return nil, &Error{ErrClusterConfig, "Cannot add member without RPC interface"}
	}

	// Ensure the new member is in the peers map

	mc.maplock.Lock()
	mc.peers[targetMember] = targetMemberRPC
	mc.maplock.Unlock()

	// Join the cluster

	res, err := mc.SendRequest(targetMember, RPCJoinCluster, map[RequestArgument]interface{}{
		RequestMEMBERNAME: mc.token.MemberName,
		RequestMEMBERRPC:  mc.rpc,
	})

	if res != nil && err == nil {
		return bytesToMap(res.([]byte)), err
	}

	mc.maplock.Lock()
	delete(mc.peers, targetMember)
	delete(mc.conns, targetMember)
	delete(mc.failed, targetMember)
	mc.maplock.Unlock()

	return nil, err
}

/*
SendEjectMember sends a request to eject a member from the cluster.
*/
func (mc *Client) SendEjectMember(member string, memberToEject string) error {

	_, err := mc.SendRequest(member, RPCEjectMember, map[RequestArgument]interface{}{
		RequestMEMBERNAME: memberToEject,
	})

	return err
}

// Cluster-wide locking
// ====================

/*
SendAcquireClusterLock tries to acquire a named lock on all members of the cluster.
It fails if the lock is alread acquired or if not enough cluster members can be
reached.
*/
func (mc *Client) SendAcquireClusterLock(lockName string) error {

	// Get operational peers (operational cluster is NOT required - up to the calling
	// function to decide if the cluster should be operational)

	peers, _ := mc.OperationalPeers()

	// Try to acquire the lock on all members

	var takenLocks []string

	for _, peer := range peers {
		_, err := mc.SendRequest(peer,
			RPCAcquireLock, map[RequestArgument]interface{}{
				RequestLOCK: lockName,
			})

		if err != nil && err.(*Error).Type == ErrMemberComm {

			// If we can't communicate with a member just continue and
			// don't take the lock - the member is now in the failed list
			// and subsequent calls to operational peers should determine
			// if the cluster is functional or not

			continue

		} else if err != nil {

			// If there was a serious error try to release all taken locks

			for _, lockPeer := range takenLocks {
				mc.SendRequest(lockPeer,
					RPCReleaseLock, map[RequestArgument]interface{}{
						RequestLOCK: lockName,
					})
			}

			return err

		} else {

			takenLocks = append(takenLocks, peer)
		}
	}

	// Now take the lock on this member

	mc.maplock.Lock()
	mc.clusterLocks.Put(lockName, mc.token.MemberName)
	mc.maplock.Unlock()

	return nil
}

/*
SendReleaseClusterLock tries to release a named lock on all members of the cluster.
It is not an error if a lock is not takfen (or has expired) on this member or any other
target member.
*/
func (mc *Client) SendReleaseClusterLock(lockName string) error {

	// Get operational peers (operational cluster is NOT required - up to the calling
	// function to decide if the cluster should be operational)

	peers, _ := mc.OperationalPeers()

	// Try to acquire the lock on all members

	for _, peer := range peers {
		_, err := mc.SendRequest(peer,
			RPCReleaseLock, map[RequestArgument]interface{}{
				RequestLOCK: lockName,
			})

		if err != nil && err.(*Error).Type != ErrMemberComm {
			return err
		}
	}

	// Now release the lock on this member

	mc.maplock.Lock()
	mc.clusterLocks.Remove(lockName)
	mc.maplock.Unlock()

	return nil
}

// StateInfo functions
// ===================

/*
SendStateInfoRequest requests the state info of a member and returns it.
*/
func (mc *Client) SendStateInfoRequest(member string) (map[string]interface{}, error) {
	res, err := mc.SendRequest(member, RPCSIRequest, nil)

	if res != nil {
		return bytesToMap(res.([]byte)), err
	}

	return nil, err
}

// Data request functions
// ======================

/*
SendDataRequest sends a data request to a member and returns its response.
*/
func (mc *Client) SendDataRequest(member string, reqdata interface{}) (interface{}, error) {
	return mc.SendRequest(member, RPCDataRequest, map[RequestArgument]interface{}{
		RequestDATA: reqdata,
	})
}

// Static member info functions
// ============================

/*
SendMemberInfoRequest requests the static member info of a member and returns it.
*/
func (mc *Client) SendMemberInfoRequest(member string) (map[string]interface{}, error) {
	res, err := mc.SendRequest(member, RPCMIRequest, nil)

	if res != nil {
		return bytesToMap(res.([]byte)), err
	}

	return nil, err
}

// Helper functions
// ================

/*
Check if a given route should be excepted from errors (only used for testing)
*/
func isErrorExcepted(source string, target string) bool {

	if exceptions, ok := MemberErrorExceptions[source]; ok {

		for _, exception := range exceptions {
			if exception == target {
				return true
			}
		}
	}

	return false
}
