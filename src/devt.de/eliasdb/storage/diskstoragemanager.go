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
	"encoding/gob"
	"errors"
	"fmt"
	"io"
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
VERSION constains the version of the storage API
*/
const VERSION = 1

/*
FileSiffixLockfile is the file ending for lockfiles
*/
const FileSiffixLockfile = "lck"

/*
FileSuffixLogicalSlots is the file ending for a logical slot storage
*/
const FileSuffixLogicalSlots = "ix"

/*
FileSuffixLogicalFreeSlots is the file ending for a free logical slot storage
*/
const FileSuffixLogicalFreeSlots = "ixf"

/*
FileSuffixPhysicalSlots is the file ending for a physical slot storage
*/
const FileSuffixPhysicalSlots = "db"

/*
FileSuffixPhysicalFreeSlots is the file ending for a free physical slot storage
*/
const FileSuffixPhysicalFreeSlots = "dbf"

/*
BlockSizePhysicalSlots is the block for a physical slot file. Physical slots will
contain actual data they need to have fairly large block sizes.
*/
const BlockSizePhysicalSlots = 1024 * 8

/*
BlockSizeLogicalSlots is the block for a logical slot file. Logical slots contain only
pointers they only need small blocks.
*/
const BlockSizeLogicalSlots = 1024 * 2

/*
BlockSizeFreeSlots is the block for a free slot files. Files containing only free slot
pointers will always be small. They only need tiny blocks.
*/
const BlockSizeFreeSlots = 1024

/*
ErrReadonly is returned when attempting a write operation on a readonly datastore.
*/
var ErrReadonly = errors.New("Storage is readonly")

/*
DiskStorageManager is a storage manager which can store any gob serializable datastructure.
*/
type DiskStorageManager struct {
	*ByteDiskStorageManager
}

/*
NewDiskStorageManager creates a new disk storage manager with optional
transaction management. If the onlyAppend flag is set then the manager will
not attempt to reuse space once it was released after use. If the
transDisabled flag is set then the storage manager will not support
transactions.
*/
func NewDiskStorageManager(filename string, readonly bool, onlyAppend bool,
	transDisabled bool, lockfileDisabled bool) *DiskStorageManager {

	return &DiskStorageManager{NewByteDiskStorageManager(filename, readonly,
		onlyAppend, transDisabled, lockfileDisabled)}
}

/*
Name returns the name of the StorageManager instance.
*/
func (dsm *DiskStorageManager) Name() string {
	return fmt.Sprint("DiskStorageFile:", dsm.ByteDiskStorageManager.filename)
}

/*
Serialize serializes an object into a byte slice.
*/
func (dsm *DiskStorageManager) Serialize(o interface{}) ([]byte, error) {

	// Request a buffer from the buffer pool

	bb := BufferPool.Get().(*bytes.Buffer)
	defer func() {
		bb.Reset()
		BufferPool.Put(bb)
	}()

	// Serialize the object into a gob bytes stream

	err := gob.NewEncoder(bb).Encode(o)
	if err != nil {
		return nil, err
	}

	return bb.Bytes(), nil
}

/*
Insert inserts an object and return its storage location.
*/
func (dsm *DiskStorageManager) Insert(o interface{}) (uint64, error) {

	b, err := dsm.Serialize(o)

	if err != nil {
		return 0, err
	}

	return dsm.ByteDiskStorageManager.Insert(b)
}

/*
Update updates a storage location.
*/
func (dsm *DiskStorageManager) Update(loc uint64, o interface{}) error {

	b, err := dsm.Serialize(o)

	if err != nil {
		return err
	}

	return dsm.ByteDiskStorageManager.Update(loc, b)
}

/*
Fetch fetches an object from a given storage location and writes it to
a given data container.
*/
func (dsm *DiskStorageManager) Fetch(loc uint64, o interface{}) error {

	// Request a buffer from the buffer pool

	bb := BufferPool.Get().(*bytes.Buffer)
	defer func() {
		bb.Reset()
		BufferPool.Put(bb)
	}()

	if err := dsm.ByteDiskStorageManager.Fetch(loc, bb); err != nil {
		return err
	}

	//  Deserialize the object from a gob bytes stream

	return gob.NewDecoder(bb).Decode(o)
}

/*
ByteDiskStorageManager is a disk storage manager which can only store byte slices.
*/
type ByteDiskStorageManager struct {
	filename      string      // Filename for all managed files
	readonly      bool        // Flag to make the storage readonly
	onlyAppend    bool        // Flag for append-only mode
	transDisabled bool        // Flag if transactions are enabled
	mutex         *sync.Mutex // Mutex to protect actual file operations

	physicalSlotsSf        *file.StorageFile        // StorageFile for physical slots
	physicalSlotsPager     *paging.PagedStorageFile // Pager for physical slots StorageFile
	physicalFreeSlotsSf    *file.StorageFile        // StorageFile for free physical slots
	physicalFreeSlotsPager *paging.PagedStorageFile // Pager for free physical slots StorageFile

	physicalSlotManager *slotting.PhysicalSlotManager // Manager for physical slots

	logicalSlotsSf        *file.StorageFile        // StorageFile for logical slots
	logicalSlotsPager     *paging.PagedStorageFile // Pager for logical slots StorageFile
	logicalFreeSlotsSf    *file.StorageFile        // StorageFile for free logical slots
	logicalFreeSlotsPager *paging.PagedStorageFile // Pager for free logical slots StorageFile

	logicalSlotManager *slotting.LogicalSlotManager // Manager for physical slots

	lockfile *lockutil.LockFile // Lockfile manager
}

/*
NewByteDiskStorageManager creates a new disk storage manager with optional
transaction management which can only store byte slices. If the onlyAppend
flag is set then the manager will not attempt to reuse space once it was
released after use. If the transDisabled flag is set then the storage
manager will not support transactions.
*/
func NewByteDiskStorageManager(filename string, readonly bool, onlyAppend bool,
	transDisabled bool, lockfileDisabled bool) *ByteDiskStorageManager {

	var lf *lockutil.LockFile

	// Create a lockfile which is checked every 50 milliseconds

	if !lockfileDisabled {
		lf = lockutil.NewLockFile(fmt.Sprintf("%v.%v", filename, FileSiffixLockfile),
			time.Duration(50)*time.Millisecond)
	}

	bdsm := &ByteDiskStorageManager{filename, readonly, onlyAppend, transDisabled, &sync.Mutex{}, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, lf}

	err := initByteDiskStorageManager(bdsm)
	if err != nil {
		panic(fmt.Sprintf("Could not initialize DiskStroageManager: %v", filename))
	}

	return bdsm
}

/*
DataFileExist checks if the main datastore file exists.
*/
func DataFileExist(filename string) bool {
	ret, err := fileutil.PathExists(fmt.Sprintf("%v.%v.0", filename,
		FileSuffixPhysicalSlots))

	if err != nil {
		return false
	}

	return ret
}

/*
Name returns the name of the StorageManager instance.
*/
func (bdsm *ByteDiskStorageManager) Name() string {
	return fmt.Sprint("ByteDiskStorageFile:", bdsm.filename)
}

/*
Root returns a root value.
*/
func (bdsm *ByteDiskStorageManager) Root(root int) uint64 {
	bdsm.mutex.Lock()
	defer bdsm.mutex.Unlock()

	bdsm.checkFileOpen()
	return bdsm.physicalSlotsPager.Header().Root(root)
}

/*
SetRoot writes a root value.
*/
func (bdsm *ByteDiskStorageManager) SetRoot(root int, val uint64) {

	// When readonly this operation becomes a NOP

	if bdsm.readonly {
		return
	}

	bdsm.mutex.Lock()
	defer bdsm.mutex.Unlock()

	bdsm.checkFileOpen()
	bdsm.physicalSlotsPager.Header().SetRoot(root, val)
}

/*
Insert inserts an object and return its storage location.
*/
func (bdsm *ByteDiskStorageManager) Insert(o interface{}) (uint64, error) {
	bdsm.checkFileOpen()

	// Fail operation if readonly

	if bdsm.readonly {
		return 0, ErrReadonly
	}

	// Continue single threaded from here on

	bdsm.mutex.Lock()
	defer bdsm.mutex.Unlock()

	// Store the data in a physical slot

	b := o.([]byte)

	ploc, err := bdsm.physicalSlotManager.Insert(b, 0, uint32(len(b)))
	if err != nil {
		return 0, err
	}

	// Get a logical slot for the physical slot

	loc, err := bdsm.logicalSlotManager.Insert(ploc)
	if err != nil {
		return 0, err
	}

	return loc, nil
}

/*
Update updates a storage location.
*/
func (bdsm *ByteDiskStorageManager) Update(loc uint64, o interface{}) error {
	bdsm.checkFileOpen()

	// Fail operation if readonly

	if bdsm.readonly {
		return ErrReadonly
	}

	// Get the physical slot for the given logical slot

	bdsm.mutex.Lock()
	ploc, err := bdsm.logicalSlotManager.Fetch(loc)
	bdsm.mutex.Unlock()
	if err != nil {
		return err
	}

	if ploc == 0 {
		return ErrSlotNotFound.fireError(bdsm, fmt.Sprint("Location:",
			util.LocationRecord(loc), util.LocationOffset(loc)))
	}

	// Continue single threaded from here on

	bdsm.mutex.Lock()
	defer bdsm.mutex.Unlock()

	// Update the physical record

	b := o.([]byte)

	newPloc, err := bdsm.physicalSlotManager.Update(ploc, b, 0, uint32(len(b)))
	if err != nil {
		return err
	}

	// Update the logical slot if the physical slot has changed

	if newPloc != ploc {
		return bdsm.logicalSlotManager.Update(loc, newPloc)
	}

	return nil
}

/*
Fetch fetches an object from a given storage location and writes it to
a given data container.
*/
func (bdsm *ByteDiskStorageManager) Fetch(loc uint64, o interface{}) error {
	bdsm.checkFileOpen()

	// Get the physical slot for the given logical slot

	bdsm.mutex.Lock()
	ploc, err := bdsm.logicalSlotManager.Fetch(loc)
	bdsm.mutex.Unlock()

	if err != nil {
		return err
	}

	if ploc == 0 {
		return ErrSlotNotFound.fireError(bdsm, fmt.Sprint("Location:",
			util.LocationRecord(loc), util.LocationOffset(loc)))
	}

	// Request the stored bytes

	bdsm.mutex.Lock()

	if w, ok := o.(io.Writer); ok {
		err = bdsm.physicalSlotManager.Fetch(ploc, w)
	} else {
		var b bytes.Buffer
		err = bdsm.physicalSlotManager.Fetch(ploc, &b)
		copy(o.([]byte), b.Bytes())
	}

	bdsm.mutex.Unlock()

	return err
}

/*
FetchCached is not implemented for a ByteDiskStorageManager.
Only defined to satisfy the StorageManager interface.
*/
func (bdsm *ByteDiskStorageManager) FetchCached(loc uint64) (interface{}, error) {
	return nil, ErrNotInCache
}

/*
Free frees a storage location.
*/
func (bdsm *ByteDiskStorageManager) Free(loc uint64) error {
	bdsm.checkFileOpen()

	// Fail operation if readonly

	if bdsm.readonly {
		return ErrReadonly
	}

	// Continue single threaded from here on

	bdsm.mutex.Lock()
	defer bdsm.mutex.Unlock()

	// Get the physical slot for the given logical slot

	ploc, err := bdsm.logicalSlotManager.Fetch(loc)
	if err != nil {
		return err
	}

	if ploc == 0 {
		return ErrSlotNotFound.fireError(bdsm, fmt.Sprint("Location:",
			util.LocationRecord(loc), util.LocationOffset(loc)))
	}

	// First try to free the physical slot since here is the data
	// if this fails we don't touch the logical slot

	err = bdsm.physicalSlotManager.Free(ploc)
	if err != nil {
		return err
	}

	// This is very unlikely to fail - either way we can't do anything
	// at this point since the physical slot has already gone away

	return bdsm.logicalSlotManager.Free(loc)
}

/*
Flush writes all pending changes to disk.
*/
func (bdsm *ByteDiskStorageManager) Flush() error {
	bdsm.checkFileOpen()

	// When readonly this operation becomes a NOP

	if bdsm.readonly {
		return nil
	}

	ce := errorutil.NewCompositeError()

	// Continue single threaded from here on

	bdsm.mutex.Lock()
	defer bdsm.mutex.Unlock()

	// Write pending changes

	if err := bdsm.physicalSlotManager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.logicalSlotManager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.physicalSlotsPager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.physicalFreeSlotsPager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.logicalSlotsPager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.logicalFreeSlotsPager.Flush(); err != nil {
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
func (bdsm *ByteDiskStorageManager) Rollback() error {

	// Rollback has no effect if transactions are disabled or when readonly

	if bdsm.transDisabled || bdsm.readonly {
		return nil
	}

	bdsm.checkFileOpen()

	ce := errorutil.NewCompositeError()

	// Continue single threaded from here on

	bdsm.mutex.Lock()
	defer bdsm.mutex.Unlock()

	// Write pending manager changes to transaction log

	if err := bdsm.physicalSlotManager.Flush(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.logicalSlotManager.Flush(); err != nil {
		ce.Add(err)
	}

	// Rollback current transaction

	if err := bdsm.physicalSlotsPager.Rollback(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.physicalFreeSlotsPager.Rollback(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.logicalSlotsPager.Rollback(); err != nil {
		ce.Add(err)
	}

	if err := bdsm.logicalFreeSlotsPager.Rollback(); err != nil {
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
func (bdsm *ByteDiskStorageManager) Close() error {
	bdsm.checkFileOpen()

	ce := errorutil.NewCompositeError()

	// Continue single threaded from here on

	bdsm.mutex.Lock()
	defer bdsm.mutex.Unlock()

	// Try to close all files and collect any errors which are returned

	if err := bdsm.physicalSlotsPager.Close(); err != nil {
		ce.Add(err)
	}
	if err := bdsm.physicalFreeSlotsPager.Close(); err != nil {
		ce.Add(err)
	}
	if err := bdsm.logicalSlotsPager.Close(); err != nil {
		ce.Add(err)
	}
	if err := bdsm.logicalFreeSlotsPager.Close(); err != nil {
		ce.Add(err)
	}

	// Return errors if there were any

	if ce.HasErrors() {
		return ce
	}

	// Release all file related objects

	bdsm.physicalSlotsSf = nil
	bdsm.physicalSlotsPager = nil
	bdsm.physicalFreeSlotsSf = nil
	bdsm.physicalFreeSlotsPager = nil
	bdsm.physicalSlotManager = nil
	bdsm.logicalSlotsSf = nil
	bdsm.logicalSlotsPager = nil
	bdsm.logicalFreeSlotsSf = nil
	bdsm.logicalFreeSlotsPager = nil
	bdsm.logicalSlotManager = nil

	if bdsm.lockfile != nil {
		return bdsm.lockfile.Finish()
	}

	return nil
}

/*
checkFileOpen checks that the files on disk are still open.
*/
func (bdsm *ByteDiskStorageManager) checkFileOpen() {
	if bdsm.physicalSlotsSf == nil {
		panic(fmt.Sprint("Trying to access storage after it was closed: ", bdsm.filename))
	}
	if bdsm.lockfile != nil && !bdsm.lockfile.WatcherRunning() {
		err := bdsm.lockfile.Finish()
		panic(fmt.Sprint("Error while checking lockfile:", err))
	}
}

/*
initByteDiskStorageManager initialises the file managers of a given ByteDiskStorageManager.
*/
func initByteDiskStorageManager(bdsm *ByteDiskStorageManager) error {

	// Kick off the lockfile watcher

	if bdsm.lockfile != nil {

		if err := bdsm.lockfile.Start(); err != nil {
			panic(fmt.Sprintf("Could not take ownership of lockfile %v: %v",
				bdsm.filename, err))
		}
	}

	// Try to open all files and collect all errors

	ce := errorutil.NewCompositeError()

	sf, pager, err := createFileAndPager(
		fmt.Sprintf("%v.%v", bdsm.filename, FileSuffixPhysicalSlots),
		BlockSizePhysicalSlots, bdsm)

	if err != nil {
		ce.Add(err)
	}

	bdsm.physicalSlotsSf = sf
	bdsm.physicalSlotsPager = pager

	sf, pager, err = createFileAndPager(
		fmt.Sprintf("%v.%v", bdsm.filename, FileSuffixPhysicalFreeSlots),
		BlockSizeFreeSlots, bdsm)

	if err != nil {
		ce.Add(err)
	}

	bdsm.physicalFreeSlotsSf = sf
	bdsm.physicalFreeSlotsPager = pager

	if !ce.HasErrors() {
		bdsm.physicalSlotManager = slotting.NewPhysicalSlotManager(bdsm.physicalSlotsPager,
			bdsm.physicalFreeSlotsPager, bdsm.onlyAppend)
	}

	sf, pager, err = createFileAndPager(
		fmt.Sprintf("%v.%v", bdsm.filename, FileSuffixLogicalSlots),
		BlockSizeLogicalSlots, bdsm)

	if err != nil {
		ce.Add(err)
	}

	bdsm.logicalSlotsSf = sf
	bdsm.logicalSlotsPager = pager

	sf, pager, err = createFileAndPager(
		fmt.Sprintf("%v.%v", bdsm.filename, FileSuffixLogicalFreeSlots),
		BlockSizeFreeSlots, bdsm)

	if err != nil {
		ce.Add(err)
	}

	bdsm.logicalFreeSlotsSf = sf
	bdsm.logicalFreeSlotsPager = pager

	if !ce.HasErrors() {
		bdsm.logicalSlotManager = slotting.NewLogicalSlotManager(bdsm.logicalSlotsPager,
			bdsm.logicalFreeSlotsPager)
	}

	// If there were any file related errors return at this point

	if ce.HasErrors() {

		// Release the lockfile if there were errors

		if bdsm.lockfile != nil {
			bdsm.lockfile.Finish()
		}

		return ce
	}

	// Check version

	version := bdsm.Root(RootIDVersion)
	if version > VERSION {

		// Try to clean up

		bdsm.Close()

		panic(fmt.Sprint("Cannot open datastore ", bdsm.filename, " - version of disk files is "+
			"newer than supported version. Supported version:", VERSION,
			" Disk files version:", version))
	}

	if version != VERSION {
		bdsm.SetRoot(RootIDVersion, VERSION)
	}

	return nil
}

/*
createFileAndPager creates a storagefile and a pager.
*/
func createFileAndPager(filename string, recordSize uint32,
	bdsm *ByteDiskStorageManager) (*file.StorageFile, *paging.PagedStorageFile, error) {

	sf, err := file.NewStorageFile(filename, recordSize, bdsm.transDisabled)
	if err != nil {
		return nil, nil, err
	}

	pager, err := paging.NewPagedStorageFile(sf)

	return sf, pager, err
}
