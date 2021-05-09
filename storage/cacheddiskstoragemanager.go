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
Package storage contains the low-level API for data storage. Data is stored
in slots. The interface defines methods to store, retrieve, update and delete
a given object to and from the disk. There are 3 main implementations:

DiskStorageManager

A disk storage manager handles the data storage on disk. It controls the actual
PhysicalSlotManager and LogicalSlotManager objects. It holds references to all
involved files and ensures exclusive access to them through a generated lock
file. The lockfile is checked and attempting to open another instance of the
DiskStorageManager on the same files will result in an error. The DiskStorageManager
is also responsible for marshalling given abstract objects into a binary form which
can be written to physical slots.

CachedDiskStorageManager

The CachedDiskStorageManager is a cache wrapper for the DiskStorageManager. Its
purpose is to intercept calls and to maintain a cache of stored objects. The cache
is limited in size by the number of total objects it references. Once the cache
is full it will forget the objects which have been requested the least.

MemoryStorageManager

A storage manager which keeps all its data in memory and provides several
error simulation facilities.
*/
package storage

import "sync"

/*
CachedDiskStorageManager data structure
*/
type CachedDiskStorageManager struct {
	diskstoragemanager *DiskStorageManager    // Wrapped instance of DiskStorageManager
	mutex              *sync.Mutex            // Mutex to protect list and map operations
	cache              map[uint64]*cacheEntry // Map of stored cacheEntry objects
	maxObjects         int                    // Max number of objects which should be held in the cache
	firstentry         *cacheEntry            // Pointer to first entry in cacheEntry linked list
	lastentry          *cacheEntry            // Pointer to last entry in cacheEntry linked list
}

/*
cacheEntry data structure
*/
type cacheEntry struct {
	location uint64      // Slot (logical) of the entry
	object   interface{} // Object of the entry
	prev     *cacheEntry // Pointer to previous entry in cacheEntry linked list
	next     *cacheEntry // Pointer to next entry in cacheEntry linked list
}

/*
Pool for cache entries
*/
var entryPool = &sync.Pool{New: func() interface{} { return &cacheEntry{} }}

/*
NewCachedDiskStorageManager creates a new cache wrapper for a DiskStorageManger.
*/
func NewCachedDiskStorageManager(diskstoragemanager *DiskStorageManager, maxObjects int) *CachedDiskStorageManager {
	return &CachedDiskStorageManager{diskstoragemanager, &sync.Mutex{}, make(map[uint64]*cacheEntry),
		maxObjects, nil, nil}
}

/*
Name returns the name of the StorageManager instance.
*/
func (cdsm *CachedDiskStorageManager) Name() string {
	return cdsm.diskstoragemanager.Name()
}

/*
Root returns a root value.
*/
func (cdsm *CachedDiskStorageManager) Root(root int) uint64 {
	return cdsm.diskstoragemanager.Root(root)
}

/*
SetRoot writes a root value.
*/
func (cdsm *CachedDiskStorageManager) SetRoot(root int, val uint64) {
	cdsm.diskstoragemanager.SetRoot(root, val)
}

/*
Insert inserts an object and return its storage location.
*/
func (cdsm *CachedDiskStorageManager) Insert(o interface{}) (uint64, error) {

	// Cannot cache inserts since the calling code needs a location

	loc, err := cdsm.diskstoragemanager.Insert(o)

	if loc != 0 && err == nil {

		cdsm.mutex.Lock()
		defer cdsm.mutex.Unlock()

		cdsm.addToCache(loc, o)
	}

	return loc, err
}

/*
Update updates a storage location.
*/
func (cdsm *CachedDiskStorageManager) Update(loc uint64, o interface{}) error {

	// Store the update in the cache

	cdsm.mutex.Lock()

	if entry, ok := cdsm.cache[loc]; !ok {
		cdsm.addToCache(loc, o)
	} else {
		entry.object = o
		cdsm.llTouchEntry(entry)
	}

	cdsm.mutex.Unlock()

	return cdsm.diskstoragemanager.Update(loc, o)
}

/*
Free frees a storage location.
*/
func (cdsm *CachedDiskStorageManager) Free(loc uint64) error {

	if ret := cdsm.diskstoragemanager.Free(loc); ret != nil {
		return ret
	}

	cdsm.mutex.Lock()
	defer cdsm.mutex.Unlock()

	// Remove location entry from the cache

	if entry, ok := cdsm.cache[loc]; ok {
		delete(cdsm.cache, entry.location)
		cdsm.llRemoveEntry(entry)
	}

	return nil
}

/*
Fetch fetches an object from a given storage location and writes it to
a given data container.
*/
func (cdsm *CachedDiskStorageManager) Fetch(loc uint64, o interface{}) error {

	err := cdsm.diskstoragemanager.Fetch(loc, o)
	if err != nil {
		return err
	}

	cdsm.mutex.Lock()
	defer cdsm.mutex.Unlock()

	// Put the retrieved value into the cache

	if entry, ok := cdsm.cache[loc]; !ok {
		cdsm.addToCache(loc, o)
	} else {
		cdsm.llTouchEntry(entry)
	}

	return nil
}

/*
FetchCached fetches an object from a cache and returns its reference.
Returns a storage.ErrNotInCache error if the entry is not in the cache.
*/
func (cdsm *CachedDiskStorageManager) FetchCached(loc uint64) (interface{}, error) {

	cdsm.mutex.Lock()
	defer cdsm.mutex.Unlock()

	if entry, ok := cdsm.cache[loc]; ok {
		return entry.object, nil
	}

	return nil, NewStorageManagerError(ErrNotInCache, "", cdsm.Name())
}

/*
Rollback cancels all pending changes which have not yet been written to disk.
*/
func (cdsm *CachedDiskStorageManager) Rollback() error {

	if cdsm.diskstoragemanager.transDisabled {
		return nil
	}

	err := cdsm.diskstoragemanager.Rollback()

	cdsm.mutex.Lock()
	defer cdsm.mutex.Unlock()

	// Cache is emptied in any case

	cdsm.cache = make(map[uint64]*cacheEntry)
	cdsm.firstentry = nil
	cdsm.lastentry = nil

	return err
}

/*
Close the StorageManager and write all pending changes to disk.
*/
func (cdsm *CachedDiskStorageManager) Close() error {
	return cdsm.diskstoragemanager.Close()
}

/*
Flush writes all pending changes to disk.
*/
func (cdsm *CachedDiskStorageManager) Flush() error {
	return cdsm.diskstoragemanager.Flush()
}

/*
addToCache adds an entry to the cache.
*/
func (cdsm *CachedDiskStorageManager) addToCache(loc uint64, o interface{}) {

	var entry *cacheEntry

	// Get an entry from the pool or recycle an entry from the cacheEntry
	// linked list if the list is full

	if len(cdsm.cache) >= cdsm.maxObjects {
		entry = cdsm.removeOldestFromCache()
	} else {
		entry = entryPool.Get().(*cacheEntry)
	}

	// Fill the entry

	entry.location = loc
	entry.object = o

	// Insert entry into the cacheEntry linked list (this will set the entries
	// prev and next pointer)

	cdsm.llAppendEntry(entry)

	// Insert into the map of stored cacheEntry objects

	cdsm.cache[loc] = entry
}

/*
removeOldestFromCache removes the oldest entry from the cache and return it.
*/
func (cdsm *CachedDiskStorageManager) removeOldestFromCache() *cacheEntry {
	entry := cdsm.firstentry

	// If no entries were stored yet just return an entry from the pool

	if entry == nil {
		return entryPool.Get().(*cacheEntry)
	}

	// Remove entry from the cacheEntry linked list (this will set the entries
	// prev and next pointer)

	cdsm.llRemoveEntry(entry)

	// Remove entry from the map of stored cacheEntry objects

	delete(cdsm.cache, entry.location)

	return entry
}

/*
llTouchEntry puts an entry to the last position of the cacheEntry linked list.
Calling llTouchEntry on all requested items ensures that the oldest used
entry is at the beginning of the list.
*/
func (cdsm *CachedDiskStorageManager) llTouchEntry(entry *cacheEntry) {
	if cdsm.lastentry == entry {
		return
	}

	cdsm.llRemoveEntry(entry)
	cdsm.llAppendEntry(entry)
}

/*
llAppendEntry appends a cacheEntry to the end of the cacheEntry linked list.
*/
func (cdsm *CachedDiskStorageManager) llAppendEntry(entry *cacheEntry) {
	if cdsm.firstentry == nil {
		cdsm.firstentry = entry
		cdsm.lastentry = entry
		entry.prev = nil
	} else {
		cdsm.lastentry.next = entry
		entry.prev = cdsm.lastentry
		cdsm.lastentry = entry
	}
	entry.next = nil
}

/*
llRemoveEntry removes a cacheEntry from the cacheEntry linked list.
*/
func (cdsm *CachedDiskStorageManager) llRemoveEntry(entry *cacheEntry) {
	if entry == cdsm.firstentry {
		cdsm.firstentry = entry.next
	}
	if cdsm.lastentry == entry {
		cdsm.lastentry = entry.prev
	}

	if entry.prev != nil {
		entry.prev.next = entry.next
		entry.prev = nil
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
		entry.next = nil
	}
}
