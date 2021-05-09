/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package storage

import (
	"errors"
	"fmt"

	"devt.de/krotik/common/pools"
)

/*
BufferPool is a pool of byte buffers.
*/
var BufferPool = pools.NewByteBufferPool()

/*
Common storage manager related errors.
*/
var (
	ErrSlotNotFound = errors.New("Slot not found")
	ErrNotInCache   = errors.New("No entry in cache")
)

/*
ManagerError is a storage manager related error.
*/
type ManagerError struct {
	Type        error
	Detail      string
	Managername string
}

/*
NewStorageManagerError returns a new StorageManager specific error.
*/
func NewStorageManagerError(smeType error, smeDetail string, smeManagername string) *ManagerError {
	return &ManagerError{smeType, smeDetail, smeManagername}
}

/*
Error returns a string representation of the error.
*/
func (e *ManagerError) Error() string {
	return fmt.Sprintf("%s (%s - %s)", e.Type.Error(), e.Managername, e.Detail)
}
