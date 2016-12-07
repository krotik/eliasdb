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
	"bytes"
	"fmt"
	"sync"

	"devt.de/common/datautil"
	"devt.de/eliasdb/storage/file"
)

/*
Special flags which cause the manager to return errors on specific function calls
*/
const (
	AccessNotInCache                = 1 // The address will not be accessible via FetchCached
	AccessFetchError                = 2 // The address will not be accessible via Fetch
	AccessUpdateError               = 3 // The address will not be accessible via Update
	AccessFreeError                 = 4 // The address will not be accessible via Free
	AccessInsertError               = 5 // The address will not be accessible via Insert
	AccessCacheAndFetchError        = 6 // The address will not be accessible via FetchCached nor Fetch
	AccessCacheAndFetchSeriousError = 7 // The address will not be accessible via FetchCached nor Fetch
)

/*
MsmRetClose nil or the error which should be returned by a Close call
*/
var MsmRetClose error

/*
MsmCallNumClose counter how often Close is called
*/
var MsmCallNumClose int

/*
MsmRetFlush nil or the error which should be returned by a Flush call
*/
var MsmRetFlush error

/*
MsmCallNumFlush counter how often Flush is called
*/
var MsmCallNumFlush int

/*
MsmRetRollback nil or the error which should be returned by a Rollback call
*/
var MsmRetRollback error

/*
MsmCallNumRollback counter how often Rollback is called
*/
var MsmCallNumRollback int

/*
MemoryStorageManager data structure
*/
type MemoryStorageManager struct {
	name  string                 // Name of the storage manager
	Roots map[int]uint64         // Map of roots
	Data  map[uint64]interface{} // Map of data
	mutex *sync.Mutex            // Mutex to protect map operations

	LocCount  uint64         // Counter for locations
	AccessMap map[uint64]int // Special map to simulate access issues
}

/*
NewMemoryStorageManager creates a new MemoryStorageManager
*/
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

	return msm.Roots[root]
}

/*
SetRoot writes a root value.
*/
func (msm *MemoryStorageManager) SetRoot(root int, val uint64) {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	msm.Roots[root] = val
}

/*
Insert inserts an object and return its storage location.
*/
func (msm *MemoryStorageManager) Insert(o interface{}) (uint64, error) {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[msm.LocCount] == AccessInsertError {
		return 0, file.ErrAlreadyInUse
	}
	loc := msm.LocCount
	msm.LocCount++
	msm.Data[loc] = o
	return loc, nil
}

/*
Update updates a storage location.
*/
func (msm *MemoryStorageManager) Update(loc uint64, o interface{}) error {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[loc] == AccessUpdateError {
		return ErrSlotNotFound.fireError(msm, fmt.Sprint("Location:", loc))
	}
	msm.Data[loc] = o
	return nil
}

/*
Free frees a storage location.
*/
func (msm *MemoryStorageManager) Free(loc uint64) error {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[loc] == AccessFreeError {
		return ErrSlotNotFound.fireError(msm, fmt.Sprint("Location:", loc))
	}
	delete(msm.Data, loc)
	return nil
}

/*
Fetch fetches an object from a given storage location and writes it to
a given data container.
*/
func (msm *MemoryStorageManager) Fetch(loc uint64, o interface{}) error {
	var err error

	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[loc] == AccessFetchError || msm.AccessMap[loc] == AccessCacheAndFetchError {
		return ErrSlotNotFound.fireError(msm, fmt.Sprint("Location:", loc))
	} else if msm.AccessMap[loc] == AccessCacheAndFetchSeriousError {
		return file.ErrAlreadyInUse
	}

	if obj, ok := msm.Data[loc]; ok {
		err = datautil.CopyObject(obj, o)
	} else {
		err = ErrSlotNotFound.fireError(msm, fmt.Sprint("Location:", loc))
	}
	return err
}

/*
FetchCached fetches an object from a cache and returns its reference.
Returns a storage.ErrNotInCache error if the entry is not in the cache.
*/
func (msm *MemoryStorageManager) FetchCached(loc uint64) (interface{}, error) {
	msm.mutex.Lock()
	defer msm.mutex.Unlock()

	if msm.AccessMap[loc] == AccessNotInCache || msm.AccessMap[loc] == AccessCacheAndFetchError {
		return nil, ErrNotInCache
	} else if msm.AccessMap[loc] == AccessCacheAndFetchSeriousError {
		return nil, file.ErrAlreadyInUse
	}
	return msm.Data[loc], nil
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

	for k, v := range msm.Data {
		buf.WriteString(fmt.Sprintf("%v - %v\n", k, v))
	}

	return buf.String()
}
