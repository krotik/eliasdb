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
	"errors"
	"fmt"
	"log"
)

// Logging
// =======

/*
Logger is a function which processes log messages from the cluster
*/
type Logger func(v ...interface{})

/*
LogInfo is called if an info message is logged in the cluster code
*/
var LogInfo = Logger(log.Print)

/*
LogDebug is called if a debug message is logged in the cluster code
(by default disabled)
*/
var LogDebug = Logger(LogNull)

/*
LogNull is a discarding logger to be used for disabling loggers
*/
var LogNull = func(v ...interface{}) {
}

// Errors
// ======

/*
Error is a cluster related error
*/
type Error struct {
	Type   error  // Error type (to be used for equal checks)
	Detail string // Details of this error
}

/*
Error returns a human-readable string representation of this error.
*/
func (ge *Error) Error() string {
	if ge.Detail != "" {
		return fmt.Sprintf("ClusterError: %v (%v)", ge.Type, ge.Detail)
	}

	return fmt.Sprintf("ClusterError: %v", ge.Type)
}

/*
Cluster related error types
*/
var (
	ErrMemberComm    = errors.New("Network error")
	ErrMemberError   = errors.New("Member error")
	ErrClusterConfig = errors.New("Cluster configuration error")
	ErrClusterState  = errors.New("Cluster state error")
	ErrUnknownPeer   = errors.New("Unknown peer member")
	ErrUnknownTarget = errors.New("Unknown target member")
	ErrInvalidToken  = errors.New("Invalid member token")
	ErrNotMember     = errors.New("Client is not a cluster member")
	ErrLockTaken     = errors.New("Requested lock is already taken")
	ErrLockNotOwned  = errors.New("Requested lock not owned")
)
