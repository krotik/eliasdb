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
A disk storage manager handles the data storage on disk.
*/
package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"devt.de/common/errorutil"
	"devt.de/common/fileutil"
	"devt.de/common/lockutil"
	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging"
	"devt.de/eliasdb/storage/slotting"
	"devt.de/eliasdb/storage/util"
)

/*
DiskStorageManager Version
*/
const VERSION = 1

/*
File ending for lockfile.
*/
const FILE_SUFFIX_LOCKFILE = "lck"

/*
File ending for logical slot storage.
*/
const FILE_SUFFIX_LOGICAL_SLOTS = "ix"

/*
File ending for free logical slot storage.
*/
const FILE_SUFFIX_LOGICAL_FREE_SLOTS = "ixf"

/*
File ending for physical slot storage.
*/
const FILE_SUFFIX_PHYSICAL_SLOTS = "db"

/*
File ending for free physical slot storage.
*/
const FILE_SUFFIX_PHYSICAL_FREE_SLOTS = "dbf"

/*
Physical slots will contain actual data they need to have fairly large block sizes.
*/
const BLOCK_SIZE_PHYSICAL_SLOTs = 1024 * 8

/*
Logical slots contain only data they only need small blocks.
*/
const BLOCK_SIZE_LOGICAL_SLOTS = 1024 * 2

/*
Files containing only free slot pointers will always be small. They only need
tiny blocks.
*/
const BLOCK_SIZE_FREE_SLOTS = 1024

/*
DiskStorageManager data structure
*/
type DiskStorageManager struct {
	filename      string      // Filename for all managed files
	onlyAppend    bool        // Flag for append-only mode
	transDisabled bool        // Flag if transactions are enabled
	mutex         *sync.Mutex // Mutex to protect actual file operations

	physical_slots_sf         *file.StorageFile        // StorageFile for physical slots
	physical_slots_pager      *paging.PagedStorageFile // Pager for physical slots StorageFile
	physical_free_slots_sf    *file.StorageFile        // StorageFile for free physical slots
	physical_free_slots_pager *paging.PagedStorageFile // Pager for free physical slots StorageFile

	physical_slot_manager *slotting.PhysicalSlotManager // Manager for physical slots

	logical_slots_sf         *file.StorageFile        // StorageFile for logical slots
	logical_slots_pager      *paging.PagedStorageFile // Pager for logical slots StorageFile
	logical_free_slots_sf    *file.StorageFile        // StorageFile for free logical slots
	logical_free_slots_pager *paging.PagedStorageFile // Pager for free logical slots StorageFile

	logical_slot_manager *slotting.LogicalSlotManager // Manager for physical slots

	lockfile *lockutil.LockFile // Lockfile manager
}

/*
NewDiskStorageManager creates a new disk storage manager with optional
transaction management. If the onlyAppend flag is set then the manager will
not attempt to reuse space once it was released after use. If the
transDisabled flag is set then the storage manager will not support
transactions.
*/
func NewDiskStorageManager(filename string, onlyAppend bool, transDisabled bool, lockfileDisabled bool) *DiskStorageManager {
	var lf *lockutil.LockFile

	// Create a lockfile which is checked every 50 milliseconds

	if !lockfileDisabled {
		lf = lockutil.NewLockFile(fmt.Sprintf("%v.%v", filename, FILE_SUFFIX_LOCKFILE),
			time.Duration(50)*time.Millisecond)
	}

	dsm := &DiskStorageManager{filename, onlyAppend, transDisabled, &sync.Mutex{}, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, lf}

	err := initDiskStorageManager(dsm)
	if err != nil {
		panic(fmt.Sprintf("Could not initialize DiskStroageManager:", filename))
	}

	return dsm
}

/*
Check if the main datastore file exists.
*/
func StorageFileExist(filename string) bool {
	ret, err := fileutil.PathExists(fmt.Sprintf("%v.%v.0", filename,
		FILE_SUFFIX_PHYSICAL_SLOTS))

	if err != nil {
		return false
	}

	return ret
}

func (dsm *DiskStorageManager) Name() string {
	return fmt.Sprint("DiskStorageFile:", dsm.filename)
}

/*
Root returns a root value.
*/
func (dsm *DiskStorageManager) Root(root int) uint64 {
	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	dsm.checkFileOpen()
	return dsm.physical_slots_pager.Header().Root(root)
}

/*
SetRoot writes a root value.
*/
func (dsm *DiskStorageManager) SetRoot(root int, val uint64) {
	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	dsm.checkFileOpen()
	dsm.physical_slots_pager.Header().SetRoot(root, val)
}

/*
Insert inserts an object and return its storage location.
*/
func (dsm *DiskStorageManager) Insert(o interface{}) (uint64, error) {
	dsm.checkFileOpen()

	// Request a buffer from the buffer pool

	bb := BufferPool.Get().(*bytes.Buffer)

	// Serialize the object into a gob bytes stream

	err := gob.NewEncoder(bb).Encode(o)
	if err != nil {
		return 0, err
	}

	// Continue single threaded from here on

	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	// Store the data in a physical slot

	ploc, err := dsm.physical_slot_manager.Insert(bb.Bytes(), 0, uint32(bb.Len()))
	if err != nil {
		return 0, err
	}

	// Get a logical slot for the physical slot

	loc, err := dsm.logical_slot_manager.Insert(ploc)
	if err != nil {
		return 0, err
	}

	// Release the buffer to the buffer pool

	bb.Reset()
	BufferPool.Put(bb)

	return loc, nil
}

/*
Update updates a storage location.
*/
func (dsm *DiskStorageManager) Update(loc uint64, o interface{}) error {
	dsm.checkFileOpen()

	// Get the physical slot for the given logical slot

	dsm.mutex.Lock()
	ploc, err := dsm.logical_slot_manager.Fetch(loc)
	dsm.mutex.Unlock()
	if err != nil {
		return err
	}

	if ploc == 0 {
		return ErrSlotNotFound.fireError(dsm, fmt.Sprint("Location:",
			util.LocationRecord(loc), util.LocationOffset(loc)))
	}

	// Request a buffer from the buffer pool

	bb := BufferPool.Get().(*bytes.Buffer)

	// Serialize the object into a gob bytes stream

	err = gob.NewEncoder(bb).Encode(o)
	if err != nil {
		return err
	}

	// Continue single threaded from here on

	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	// Update the physical record

	newPloc, err := dsm.physical_slot_manager.Update(ploc, bb.Bytes(), 0, uint32(bb.Len()))
	if err != nil {
		return err
	}

	// Release the buffer to the buffer pool

	bb.Reset()
	BufferPool.Put(bb)

	// Update the logical slot if the physical slot has changed

	if newPloc != ploc {
		return dsm.logical_slot_manager.Update(loc, newPloc)
	}

	return nil
}

/*
Fetch fetches an object from a given storage location and writes it to
a given data container.
*/
func (dsm *DiskStorageManager) Fetch(loc uint64, o interface{}) error {
	dsm.checkFileOpen()

	// Get the physical slot for the given logical slot

	dsm.mutex.Lock()
	ploc, err := dsm.logical_slot_manager.Fetch(loc)
	dsm.mutex.Unlock()
	if err != nil {
		return err
	}

	if ploc == 0 {
		return ErrSlotNotFound.fireError(dsm, fmt.Sprint("Location:",
			util.LocationRecord(loc), util.LocationOffset(loc)))
	}

	// Request a buffer from the buffer pool

	bb := BufferPool.Get().(*bytes.Buffer)

	// Request the stored bytes

	dsm.mutex.Lock()
	err = dsm.physical_slot_manager.Fetch(ploc, bb)
	dsm.mutex.Unlock()
	if err != nil {
		return err
	}

	//  Deserialize the object from a gob bytes stream

	err = gob.NewDecoder(bb).Decode(o)
	if err != nil {
		return err
	}

	// Release the buffer to the buffer pool

	bb.Reset()
	BufferPool.Put(bb)

	return nil
}

/*
FetchCached is not implemented for a DiskStorageManager.
Only defined to satisfy the StorageManager interface.
*/
func (dsm *DiskStorageManager) FetchCached(loc uint64) (interface{}, error) {
	return nil, ErrNotInCache
}

/*
Free frees a storage location.
*/
func (dsm *DiskStorageManager) Free(loc uint64) error {
	dsm.checkFileOpen()

	// Continue single threaded from here on

	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	// Get the physical slot for the given logical slot

	ploc, err := dsm.logical_slot_manager.Fetch(loc)
	if err != nil {
		return err
	}

	if ploc == 0 {
		return ErrSlotNotFound.fireError(dsm, fmt.Sprint("Location:",
			util.LocationRecord(loc), util.LocationOffset(loc)))
	}

	// First try to free the physical slot since here is the data
	// if this fails we don't touch the logical slot

	err = dsm.physical_slot_manager.Free(ploc)
	if err != nil {
		return err
	}

	// This is very unlikely to fail - either way we can't do anything
	// at this point since the physical slot has already gone away

	return dsm.logical_slot_manager.Free(loc)
}

/*
Flush writes all pending changes to disk.
*/
func (dsm *DiskStorageManager) Flush() error {
	dsm.checkFileOpen()

	ce := errorutil.NewCompositeError()

	// Continue single threaded from here on

	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	// Write pending changes

	if err := dsm.physical_slot_manager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := dsm.logical_slot_manager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := dsm.physical_slots_pager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := dsm.physical_free_slots_pager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := dsm.logical_slots_pager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := dsm.logical_free_slots_pager.Flush(); err != nil {
		ce.Add(err)
	}

	// Return errors if there were any

	if ce.HasErrors() {
		return ce
	}

	return nil
}

/*
Rollback cancels all pending changes which have not yet been written to disk.
*/
func (dsm *DiskStorageManager) Rollback() error {

	// Rollback has no effect if transactions are disabled

	if dsm.transDisabled {
		return nil
	}

	dsm.checkFileOpen()

	ce := errorutil.NewCompositeError()

	// Continue single threaded from here on

	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	// Write pending manager changes to transaction log

	if err := dsm.physical_slot_manager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := dsm.logical_slot_manager.Flush(); err != nil {
		ce.Add(err)
	}

	// Rollback current transaction

	if err := dsm.physical_slots_pager.Rollback(); err != nil {
		ce.Add(err)
	}

	if err := dsm.physical_free_slots_pager.Rollback(); err != nil {
		ce.Add(err)
	}

	if err := dsm.logical_slots_pager.Rollback(); err != nil {
		ce.Add(err)
	}

	if err := dsm.logical_free_slots_pager.Rollback(); err != nil {
		ce.Add(err)
	}

	// Return errors if there were any

	if ce.HasErrors() {
		return ce
	}

	return nil
}

/*
Close closes the StorageManager and write all pending changes to disk.
*/
func (dsm *DiskStorageManager) Close() error {
	dsm.checkFileOpen()

	ce := errorutil.NewCompositeError()

	// Continue single threaded from here on

	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	// Try to close all files and collect any errors which are returned

	if err := dsm.physical_slots_pager.Close(); err != nil {
		ce.Add(err)
	}
	if err := dsm.physical_free_slots_pager.Close(); err != nil {
		ce.Add(err)
	}
	if err := dsm.logical_slots_pager.Close(); err != nil {
		ce.Add(err)
	}
	if err := dsm.logical_free_slots_pager.Close(); err != nil {
		ce.Add(err)
	}

	// Return errors if there were any

	if ce.HasErrors() {
		return ce
	}

	// Release all file related objects

	dsm.physical_slots_sf = nil
	dsm.physical_slots_pager = nil
	dsm.physical_free_slots_sf = nil
	dsm.physical_free_slots_pager = nil
	dsm.physical_slot_manager = nil
	dsm.logical_slots_sf = nil
	dsm.logical_slots_pager = nil
	dsm.logical_free_slots_sf = nil
	dsm.logical_free_slots_pager = nil
	dsm.logical_slot_manager = nil

	if dsm.lockfile != nil {
		return dsm.lockfile.Finish()
	}

	return nil
}

/*
checkFileOpen checks that the files on disk are still open.
*/
func (dsm *DiskStorageManager) checkFileOpen() {
	if dsm.physical_slots_sf == nil {
		panic(fmt.Sprint("Trying to access DiskStorageManager after it was closed: ", dsm.filename))
	}
	if dsm.lockfile != nil && !dsm.lockfile.WatcherRunning() {
		err := dsm.lockfile.Finish()
		panic(fmt.Sprint("Error while checking lockfile:", err))
	}
}

/*
initDiskStorageManager initialises the file managers of a given DiskStorageManager.
*/
func initDiskStorageManager(dsm *DiskStorageManager) error {

	// Kick off the lockfile watcher

	if dsm.lockfile != nil {
		err := dsm.lockfile.Start()
		if err != nil {
			panic("Could not take ownership of lockfile")
		}
	}

	// Try to open all files and collect all errors

	ce := errorutil.NewCompositeError()

	sf, pager, err := createFileAndPager(
		fmt.Sprintf("%v.%v", dsm.filename, FILE_SUFFIX_PHYSICAL_SLOTS),
		BLOCK_SIZE_PHYSICAL_SLOTs, dsm)

	if err != nil {
		ce.Add(err)
	}

	dsm.physical_slots_sf = sf
	dsm.physical_slots_pager = pager

	sf, pager, err = createFileAndPager(
		fmt.Sprintf("%v.%v", dsm.filename, FILE_SUFFIX_PHYSICAL_FREE_SLOTS),
		BLOCK_SIZE_FREE_SLOTS, dsm)

	if err != nil {
		ce.Add(err)
	}

	dsm.physical_free_slots_sf = sf
	dsm.physical_free_slots_pager = pager

	if !ce.HasErrors() {
		dsm.physical_slot_manager = slotting.NewPhysicalSlotManager(dsm.physical_slots_pager,
			dsm.physical_free_slots_pager, dsm.onlyAppend)
	}

	sf, pager, err = createFileAndPager(
		fmt.Sprintf("%v.%v", dsm.filename, FILE_SUFFIX_LOGICAL_SLOTS),
		BLOCK_SIZE_LOGICAL_SLOTS, dsm)

	if err != nil {
		ce.Add(err)
	}

	dsm.logical_slots_sf = sf
	dsm.logical_slots_pager = pager

	sf, pager, err = createFileAndPager(
		fmt.Sprintf("%v.%v", dsm.filename, FILE_SUFFIX_LOGICAL_FREE_SLOTS),
		BLOCK_SIZE_FREE_SLOTS, dsm)

	if err != nil {
		ce.Add(err)
	}

	dsm.logical_free_slots_sf = sf
	dsm.logical_free_slots_pager = pager

	if !ce.HasErrors() {
		dsm.logical_slot_manager = slotting.NewLogicalSlotManager(dsm.logical_slots_pager,
			dsm.logical_free_slots_pager)
	}

	// If there were any file related errors return at this point

	if ce.HasErrors() {

		// Release the lockfile if there were errors

		if dsm.lockfile != nil {
			dsm.lockfile.Finish()
		}

		return ce
	}

	// Check version

	version := dsm.Root(ROOT_ID_VERSION)
	if version > VERSION {

		// Try to clean up

		dsm.Close()

		panic(fmt.Sprint("Cannot open datastore ", dsm.filename, " - version of disk files is "+
			"newer than supported version. Supported version:", VERSION,
			" Disk files version:", version))
	}

	if version != VERSION {
		dsm.SetRoot(ROOT_ID_VERSION, VERSION)
	}

	return nil
}

/*
createFileAndPager creates a storagefile and a pager.
*/
func createFileAndPager(filename string, recordSize uint32, dsm *DiskStorageManager) (*file.StorageFile, *paging.PagedStorageFile, error) {

	sf, err := file.NewStorageFile(filename, recordSize, dsm.transDisabled)
	if err != nil {
		return nil, nil, err
	}

	pager, err := paging.NewPagedStorageFile(sf)

	return sf, pager, err
}
