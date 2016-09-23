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
Package util contains utility classes for the graph storage.

GraphError

Models a graph related error. Low-level errors should be wrapped in a GraphError
before they are returned to a client.

IndexManager

Manages the full text search index. The index supports simple word searches as
well as phrase searches.

The index is a basically a key-value lookup which manages 2 types of entries:

Each node attribute value is split up into words. Each word gets an entry:

PrefixAttrWord + attr num + word (string) -> ids + pos
(provides word and phrase lookup)

Each node attribute value is also converted into a MD5 sum which makes attribute
value lookups very efficient:

PrefixAttrHash + attr num + hash (md5) -> ids
(provides exact match lookup)

NamesManager

Manages names of kinds, roles and attributes. Each stored name gets either a 16
or 32 bit (little endian) number assigned. The manager provides functions to lookup
either the names or their numbers.
*/
package util

import (
	"errors"
	"fmt"
)

/*
GraphError is a graph related error
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
	}

	return fmt.Sprintf("GraphError: %v", ge.Type)
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
	ErrReadOnly        = errors.New("Failed write to readonly storage")
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
