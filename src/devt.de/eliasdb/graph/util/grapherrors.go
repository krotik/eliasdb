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
Graph related errors. Low-level errors should be wrapped in a graph error
before they are returned to a client.
*/
package util

import (
	"errors"
	"fmt"
)

/*
Graph related error
*/
type GraphError struct {
	Type   error  // Error type (to be used for equal checks)
	Detail string // Details of this error
}

/*
Error returns a human-readable string representation of this error.
*/
func (ge *GraphError) Error() string {
	if ge.Detail != "" {
		return fmt.Sprintf("GraphError: %v (%v)", ge.Type, ge.Detail)
	} else {
		return fmt.Sprintf("GraphError: %v", ge.Type)
	}
}

/*
Graph storage related error types
*/
var (
	ErrOpening         = errors.New("Failed to open graph storage")
	ErrFlushing        = errors.New("Failed to flush changes")
	ErrRollback        = errors.New("Failed to rollback changes")
	ErrClosing         = errors.New("Failed to close graph storage")
	ErrAccessComponent = errors.New("Failed to access graph storage component")
)

/*
Graph related error types
*/
var (
	ErrInvalidData = errors.New("Invalid data")
	ErrIndexError  = errors.New("Index error")
	ErrReading     = errors.New("Could not read graph information")
	ErrWriting     = errors.New("Could not write graph information")
	ErrRule        = errors.New("Graph rule error")
)
