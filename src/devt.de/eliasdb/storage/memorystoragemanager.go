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
A storage manager which keeps all its data in memory and provides several
error simulation facilities.
*/
package storage

import (
	"bytes"
	"fmt"
	"sync"

	"devt.de/common/datautil"
	"devt.de/eliasdb/storage/file"
)

/*
The address will not be accessible via FetchCached
*/
const ACCESS_NOT_IN_CACHE = 1

/*
The address will not be accessible via Fetch
*/
const ACCESS_FETCH_ERROR = 2

/*
The address will not be accessible via Update
*/
const ACCESS_UPDATE_ERROR = 3

/*
The address will not be accessible via Free
*/
const ACCESS_FREE_ERROR = 4

/*
The address will not be accessible via Insert
*/
const ACCESS_INSERT_ERROR = 5

/*
The address will not be accessible via FetchCached nor Fetch
*/
const ACCESS_CACHE_AND_FETCH_ERROR = 6

/*
The address will not be accessible via FetchCached nor Fetch
*/
const ACCESS_CACHE_AND_FETCH_SERIOUS_ERROR = 7

/*
Return values for Close, Flush and Rollback calls
*/
var MsmRetClose, MsmRetFlush, MsmRetRollback error
var MsmCallNumClose, MsmCallNumFlush, MsmCallNumRollback int

/*
MemoryStorageManager data structure
*/
type MemoryStorageManager struct {
	name  string                 // Name of the storage manager
	roots map[int]uint64         // Map of roots
	data  map[uint64]interface{} // Map of data
	mutex *sync.Mutex            // Mutex to protect map operations

	LocCount  uint64         // Counter for locations
	AccessMap map[uint64]int // Special map to simulate access issues
}

func NewMemoryStorageManager(name string) *MemoryStorageManager {
	return &MemoryStorageManager{name, make(map[int]uint64),
		make(map[uint64]interface{}), &sync.Mutex{}, 1, make(map[uint64]int)}
}

/*
Name returns the name of the StorageManager instance.
*/
func (msm *MemoryStorageManager) Name() string {
	return msm.name
}

/*
Root returns a root value.
*/
func (msm *MemoryStorageManager) Root(root int) uint64 {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	return msm.roots[root]
}

/*
SetRoot writes a root value.
*/
func (msm *MemoryStorageManager) SetRoot(root int, val uint64) {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	msm.roots[root] = val
}

/*
Insert inserts an object and return its storage location.
*/
func (msm *MemoryStorageManager) Insert(o interface{}) (uint64, error) {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[msm.LocCount] == ACCESS_INSERT_ERROR {
		return 0, file.ErrAlreadyInUse
	}
	loc := msm.LocCount
	msm.LocCount++
	msm.data[loc] = o
	return loc, nil
}

/*
Update updates a storage location.
*/
func (msm *MemoryStorageManager) Update(loc uint64, o interface{}) error {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[loc] == ACCESS_UPDATE_ERROR {
		return ErrSlotNotFound.fireError(msm, fmt.Sprint("Location:", loc))
	}
	msm.data[loc] = o
	return nil
}

/*
Free frees a storage location.
*/
func (msm *MemoryStorageManager) Free(loc uint64) error {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[loc] == ACCESS_FREE_ERROR {
		return ErrSlotNotFound.fireError(msm, fmt.Sprint("Location:", loc))
	}
	delete(msm.data, loc)
	return nil
}

/*
Fetch fetches an object from a given storage location and writes it to
a given data container.
*/
func (msm *MemoryStorageManager) Fetch(loc uint64, o interface{}) error {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[loc] == ACCESS_FETCH_ERROR || msm.AccessMap[loc] == ACCESS_CACHE_AND_FETCH_ERROR {
		return ErrSlotNotFound.fireError(msm, fmt.Sprint("Location:", loc))
	} else if msm.AccessMap[loc] == ACCESS_CACHE_AND_FETCH_SERIOUS_ERROR {
		return file.ErrAlreadyInUse
	}

	if obj, ok := msm.data[loc]; ok {
		datautil.CopyObject(obj, o)
	} else {
		return ErrSlotNotFound.fireError(msm, fmt.Sprint("Location:", loc))
	}
	return nil
}

/*
FetchCached fetches an object from a cache and returns its reference.
Returns a storage.ErrNotInCache error if the entry is not in the cache.
*/
func (msm *MemoryStorageManager) FetchCached(loc uint64) (interface{}, error) {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[loc] == ACCESS_NOT_IN_CACHE || msm.AccessMap[loc] == ACCESS_CACHE_AND_FETCH_ERROR {
		return nil, ErrNotInCache
	} else if msm.AccessMap[loc] == ACCESS_CACHE_AND_FETCH_SERIOUS_ERROR {
		return nil, file.ErrAlreadyInUse
	}

	return msm.data[loc], nil
}

/*
Flush writes all pending changes to disk.
*/
func (msm *MemoryStorageManager) Flush() error {
	MsmCallNumFlush++
	return MsmRetFlush
}

/*
Rollback cancels all pending changes which have not yet been written to disk.
*/
func (msm *MemoryStorageManager) Rollback() error {
	MsmCallNumRollback++
	return MsmRetRollback
}

/*
Close the StorageManager and write all pending changes to disk.
*/
func (msm *MemoryStorageManager) Close() error {
	MsmCallNumClose++
	return MsmRetClose
}

/*
Show a string representation of the storage manager.
*/
func (msm *MemoryStorageManager) String() string {
	buf := new(bytes.Buffer)

	buf.WriteString(fmt.Sprintf("MemoryStorageManager %v\n", msm.name))

	for k, v := range msm.data {
		buf.WriteString(fmt.Sprintf("%v - %v\n", k, v))
	}

	return buf.String()
}
