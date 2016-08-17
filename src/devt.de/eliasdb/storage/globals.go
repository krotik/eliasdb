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
Global variables for StorageManagers.
*/
package storage

import (
	"fmt"

	"devt.de/common/pools"
)

var BufferPool = pools.NewByteBufferPool()

/*
Common storage manager related errors. Having these global definitions
makes the error comparison easier but has potential race-conditions.
If two storage manager objects throw an error at the same time both errors
will appear to come from the same instance.
*/
var (
	ErrSlotNotFound = newStorageManagerError("Slot not found")
	ErrNotInCache   = newStorageManagerError("No entry in cache")
)

/*
newStorageManagerError returns a new StorageManager specific error.
*/
func newStorageManagerError(text string) *storagemanagerError {
	return &storagemanagerError{text, "?", ""}
}

/*
StorageManager specific error datastructure
*/
type storagemanagerError struct {
	msg      string
	filename string
	info     string
}

/*
fireError returns the error instance from a specific StorageManager instance.
*/
func (e *storagemanagerError) fireError(s StorageManager, info string) error {
	e.filename = s.Name()
	e.info = info
	return e
}

/*
Error returns a string representation of the error.
*/
func (e *storagemanagerError) Error() string {
	return fmt.Sprintf("%s (%s - %s)", e.msg, e.filename, e.info)
}
