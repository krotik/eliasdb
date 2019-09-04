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
	"os"
	"sync"
	"testing"
	"time"

	"devt.de/krotik/common/lockutil"
	"devt.de/krotik/common/testutil"
	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/slotting/pageview"
	"devt.de/krotik/eliasdb/storage/util"
)

func TestDiskStorageManagerLockFilePanic(t *testing.T) {
	dsm := NewDiskStorageManager(DBDIR+"/test0", false, false, true, false)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Doing an operation with a storagefile with stopped lock watcher goroutine did not cause a panic.")
		}
		// Start the watcher goroutine again and close the file
		dsm.lockfile.Start()
		dsm.Close()
	}()

	file, err := os.OpenFile(DBDIR+"/test0.lck", os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		t.Error(err)
		return
	}
	file.WriteString("t")
	file.Close()

	// Give the watcher goroutine time to die

	time.Sleep(time.Duration(100) * time.Millisecond)

	if dsm.lockfile.WatcherRunning() {
		t.Error("Watcher goroutine did not die")
	}

	// This should cause a panic

	dsm.Free(util.PackLocation(2, 20))
}

func TestDiskStorageManager1(t *testing.T) {
	var res string

	dsm := NewDiskStorageManager(DBDIR+"/test1", false, false, true, false)

	if dsm.Name() != "DiskStorageFile:"+DBDIR+"/test1" {
		t.Error("Unexpected name for DiskStorageManager:", dsm.Name())
		return
	}

	// Make sure that another process which attempts to open the same
	// storage would panic

	time.Sleep(100 * time.Millisecond) // Need some time here otherwise Windows fails sometimes

	testLockfileStartPanic(t)

	// Test simple insert

	loc, err := dsm.Insert("This is a test")
	if err != nil {
		t.Error(err)
	}

	checkLocation(t, loc, 1, pageview.OffsetTransData)

	dsm.Fetch(loc, &res)
	if res != "This is a test" {
		t.Error("Unexpected fetch result:", res)
	}

	// Test case where we give a byte slice

	var res2 string

	bs := make([]byte, 18)
	dsm.ByteDiskStorageManager.Fetch(loc, bs)

	err = gob.NewDecoder(bytes.NewReader(bs)).Decode(&res2)

	if err != nil || string(res2) != "This is a test" {
		t.Error("Unexpected fetch result:", err, res2)
	}

	// Get physical slot for stored data

	ploc, err := dsm.logicalSlotManager.Fetch(loc)
	if err != nil {
		t.Error(err)
		return
	}

	// Test uodate

	// The next update should allocate a new physical record

	err = dsm.Update(loc, "This is another test")
	if err != nil {
		t.Error(err)
	}

	// Get new physical slot for stored data

	newPloc, err := dsm.logicalSlotManager.Fetch(loc)
	if err != nil {
		t.Error(err)
		return
	}

	if ploc == newPloc {
		t.Error("Physical address should have changed")
		return
	}

	dsm.Fetch(loc, &res)
	if res != "This is another test" {
		t.Error("Unexpected fetch result:", res)
	}

	// Test insert error

	_, err = dsm.Insert(&testutil.GobTestObject{Name: "test", EncErr: true, DecErr: false})
	if err == nil {
		t.Error(err)
		return
	}

	psp := dsm.physicalSlotsPager
	fpsp := dsm.physicalFreeSlotsPager
	lsp := dsm.logicalSlotsPager
	flsp := dsm.logicalFreeSlotsPager

	record, err := psp.StorageFile().Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = dsm.Insert(&testutil.GobTestObject{Name: "test", EncErr: false, DecErr: false})
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	psp.StorageFile().ReleaseInUse(record)

	record, err = lsp.StorageFile().Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = dsm.Insert(&testutil.GobTestObject{Name: "test", EncErr: false, DecErr: false})
	if err != file.ErrAlreadyInUse {
		t.Error(err, loc)
	}

	lsp.StorageFile().ReleaseInUse(record)

	rpsp, _ := psp.StorageFile().Get(2)
	rfpsp, _ := fpsp.StorageFile().Get(1)
	rlsp, _ := lsp.StorageFile().Get(1)
	rflsp, _ := flsp.StorageFile().Get(1)

	err = dsm.Flush()
	if err.Error() != "Record is already in-use (storagemanagertest/test1.dbf - Record 1); "+
		"Record is already in-use (storagemanagertest/test1.ixf - Record 1); "+
		"Records are still in-use (storagemanagertest/test1.db - Records 1); "+
		"Records are still in-use (storagemanagertest/test1.dbf - Records 1); "+
		"Records are still in-use (storagemanagertest/test1.ix - Records 1); "+
		"Records are still in-use (storagemanagertest/test1.ixf - Records 1)" {
		t.Error(err)
	}

	err = dsm.Close()
	if err.Error() != "Records are still in-use (storagemanagertest/test1.db - Records 1); "+
		"Records are still in-use (storagemanagertest/test1.dbf - Records 1); "+
		"Records are still in-use (storagemanagertest/test1.ix - Records 1); "+
		"Records are still in-use (storagemanagertest/test1.ixf - Records 1)" {
		t.Error(err)
	}

	psp.StorageFile().ReleaseInUse(rpsp)
	fpsp.StorageFile().ReleaseInUse(rfpsp)
	lsp.StorageFile().ReleaseInUse(rlsp)
	flsp.StorageFile().ReleaseInUse(rflsp)

	_, err = dsm.FetchCached(0)
	if err != ErrNotInCache {
		t.Error("Unexpected FetchCached result:", err)
		return
	}

	if err = dsm.Close(); err != nil {
		t.Error(err)
		return
	}

	if !DataFileExist(DBDIR + "/test1") {
		t.Error("Main disk storage file was not detected.")
		return
	}

	if DataFileExist(DBDIR + "/" + InvalidFileName) {
		t.Error("Main disk storage file with invalid name should not exist.")
		return
	}
}

func testLockfileStartPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Attempting to open the same DiskStorageManager twice did not cause a panic.")
		}
	}()

	dsm := NewDiskStorageManager(DBDIR+"/test1", false, false, false, false)
	dsm.Close()
}

func TestDiskStorageManager2(t *testing.T) {
	var res string

	dsm := NewDiskStorageManager(DBDIR+"/test2", false, false, true, true)

	loc, err := dsm.Insert("This is a test")
	if err != nil {
		t.Error(err)
		return
	}

	checkLocation(t, loc, 1, 18)

	// Insert some filler data

	for i := 1; i < 253; i++ {
		loc2, err := dsm.Insert("1234")
		if err != nil {
			t.Error(err)
			return
		}
		checkLocation(t, loc2, 1, uint16(18+i*8))
	}

	record, _ := dsm.logicalSlotsSf.Get(2)

	_, err = dsm.Insert("This is a test")
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	err = dsm.Fetch(util.PackLocation(2, 18), &res)
	if err != ErrSlotNotFound {
		t.Error(err)
		return
	}

	dsm.logicalSlotsSf.ReleaseInUse(record)

	err = dsm.Fetch(util.PackLocation(3, 18), &res)
	if err != ErrSlotNotFound {
		t.Error(err)
		return
	}

	record, _ = dsm.logicalSlotsSf.Get(1)

	err = dsm.Update(loc, "test")
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	err = dsm.Fetch(loc, &res)
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	dsm.logicalSlotsSf.ReleaseInUse(record)

	err = dsm.Update(util.PackLocation(2, 18), "test")
	if err != ErrSlotNotFound {
		t.Error(err)
		return
	}

	err = dsm.Update(loc, &testutil.GobTestObject{Name: "test", EncErr: true, DecErr: false})
	if err == nil {
		t.Error(err)
		return
	}

	err = dsm.Update(loc, &testutil.GobTestObject{Name: "test", EncErr: false, DecErr: false})
	if err != nil {
		t.Error(err)
		return
	}

	testres := &testutil.GobTestObject{Name: "test", EncErr: false, DecErr: true}
	err = dsm.Fetch(loc, &testres)
	if err == nil {
		t.Error("Unexpected decode result")
		return
	}

	record, _ = dsm.physicalSlotsSf.Get(1)

	var testres2 testutil.GobTestObject
	err = dsm.Fetch(loc, &testres2)
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	// Test a normal update

	err = dsm.Update(loc, "tree")
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	dsm.physicalSlotsSf.ReleaseInUse(record)

	err = dsm.Update(loc, "tree")
	if err != nil {
		t.Error(err)
		return
	}

	err = dsm.Fetch(loc, &res)
	if err != nil {
		t.Error(err)
		return
	}
	if res != "tree" {
		t.Error("Unexpected fetch result:", res)
		return
	}

	pl, _ := dsm.logicalSlotManager.Fetch(loc)
	if util.LocationRecord(pl) != 1 {
		t.Error("Unexpected initial location:", util.LocationRecord(pl))
	}

	_, err = dsm.Insert("test" + string(make([]byte, 10000)) + "test")
	if err != nil {
		t.Error(err)
		return
	}

	// Test reallocation

	err = dsm.Update(loc, "test"+string(make([]byte, 1000))+"test")
	if err != nil {
		t.Error(err)
		return
	}

	err = dsm.Update(loc, "test"+string(make([]byte, 1000))+"test")
	if err != nil {
		t.Error(err)
		return
	}

	pl, _ = dsm.logicalSlotManager.Fetch(loc)
	if util.LocationRecord(pl) != 2 {
		t.Error("Unexpected relocated location:", util.LocationRecord(pl))
	}

	if err = dsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	if err = dsm.Close(); err != nil {
		t.Error(err)
	}

	// Reopen datastore readonly

	dsm = NewDiskStorageManager(DBDIR+"/test2", true, false, true, true)

	// Try write operations

	if l, err := dsm.Insert("Test"); l != 0 || err != ErrReadonly {
		t.Error("Unexpected result:", l, err)
	}

	if err := dsm.Update(loc, "Test"); err != ErrReadonly {
		t.Error("Unexpected result:", err)
	}

	if err := dsm.Free(loc); err != ErrReadonly {
		t.Error("Unexpected result:", err)
	}

	// NOP operations

	dsm.Rollback()
	dsm.SetRoot(1, 5)

	// Try reading

	if dsm.Root(1) != 1 {
		t.Error("Unexpected root:", dsm.Root(1))
		return
	}

	err = dsm.Fetch(util.PackLocation(1, 90), &res)
	if err != nil {
		t.Error(err)
		return
	}
	if res != "1234" {
		t.Error("Unexpected fetch result:", res)
		return
	}

	if dsm.Flush() != nil {
		t.Error("Flushing failed:", err)
		return
	}

	if err = dsm.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskStorageManager3(t *testing.T) {
	var res string

	dsm := NewDiskStorageManager(DBDIR+"/test3", false, false, true, true)

	if dsm.Free(util.PackLocation(2, 18)) != ErrSlotNotFound {
		t.Error("Unexpected free result")
		return
	}

	loc, err := dsm.Insert("This is a test")
	if err != nil {
		t.Error(err)
		return
	}

	dsm.Fetch(loc, &res)
	if res != "This is a test" {
		t.Error("Unexpected fetch result:", res)
	}

	record, _ := dsm.physicalSlotsSf.Get(1)

	if dsm.Free(loc) != file.ErrAlreadyInUse {
		t.Error("Unexpected free result")
		return
	}

	dsm.physicalSlotsSf.ReleaseInUse(record)

	record, _ = dsm.logicalSlotsSf.Get(1)

	if dsm.Free(loc) != file.ErrAlreadyInUse {
		t.Error("Unexpected free result")
		return
	}

	dsm.logicalSlotsSf.ReleaseInUse(record)

	if err := dsm.Free(loc); err != nil {
		t.Error(err)
		return
	}

	// Rollback call should have no effect

	if err := dsm.Rollback(); err != nil {
		t.Error(err)
		return
	}

	if err = dsm.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestDiskStorageManagerRollback(t *testing.T) {
	dsm := NewDiskStorageManager(DBDIR+"/test4", false, false, false, true)

	var res string

	// Test expected behaviour

	loc, err := dsm.Insert("This is a test")
	if err != nil {
		t.Error(err)
	}

	checkLocation(t, loc, 1, pageview.OffsetTransData)

	if err := dsm.Rollback(); err != nil {
		t.Error(err)
		return
	}

	dsm.Fetch(loc, &res)
	if res != "" {
		t.Error("Unexpected fetch result:", res)
	}

	loc, err = dsm.Insert("This is a test")
	if err != nil {
		t.Error(err)
	}

	checkLocation(t, loc, 1, pageview.OffsetTransData)

	if err := dsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	if err := dsm.Rollback(); err != nil {
		t.Error(err)
		return
	}

	dsm.Fetch(loc, &res)
	if res != "This is a test" {
		t.Error("Unexpected fetch result:", res)
	}

	// Test error cases

	if err := dsm.Free(loc); err != nil {
		t.Error(err)
		return
	}

	psp := dsm.physicalSlotsPager
	fpsp := dsm.physicalFreeSlotsPager
	lsp := dsm.logicalSlotsPager
	flsp := dsm.logicalFreeSlotsPager

	rpsp, _ := psp.StorageFile().Get(1)
	rfpsp, _ := fpsp.StorageFile().Get(1)
	rlsp, _ := lsp.StorageFile().Get(1)
	rflsp, _ := flsp.StorageFile().Get(1)

	err = dsm.Rollback()
	if err.Error() != "Record is already in-use (storagemanagertest/test4.dbf - Record 1); "+
		"Record is already in-use (storagemanagertest/test4.ixf - Record 1); "+
		"Records are still in-use (storagemanagertest/test4.db - Records 1); "+
		"Records are still in-use (storagemanagertest/test4.dbf - Records 1); "+
		"Records are still in-use (storagemanagertest/test4.ix - Records 1); "+
		"Records are still in-use (storagemanagertest/test4.ixf - Records 1)" {
		t.Error(err)
	}

	psp.StorageFile().ReleaseInUse(rpsp)
	fpsp.StorageFile().ReleaseInUse(rfpsp)
	lsp.StorageFile().ReleaseInUse(rlsp)
	flsp.StorageFile().ReleaseInUse(rflsp)

	if err := dsm.Close(); err != nil {
		t.Error(err)
		return
	}
}

const InvalidFileName = "**" + string(0x0)

func TestDiskStorageManagerInit(t *testing.T) {
	lockfile := lockutil.NewLockFile(DBDIR+"/"+"lock0.lck", time.Duration(50)*time.Millisecond)
	dsm := &DiskStorageManager{&ByteDiskStorageManager{DBDIR + "/" + InvalidFileName, false, true, true, &sync.Mutex{},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, lockfile}}

	err := initByteDiskStorageManager(dsm.ByteDiskStorageManager)
	if err == nil {
		t.Error("Initialising a DiskStorageManager with an invalid filename should cause an error")
		return
	}

	testCannotInitPanic(t)

	dsm = &DiskStorageManager{&ByteDiskStorageManager{DBDIR + "/test999", false, true, true, &sync.Mutex{},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}}

	err = initByteDiskStorageManager(dsm.ByteDiskStorageManager)
	if err != nil {
		t.Error(err)
		return
	}

	dsm.SetRoot(RootIDVersion, VERSION+1)

	err = dsm.Close()
	if err != nil {
		t.Error(err)
	}

	testClosedPanic(t, dsm)

	testVersionCheckPanic(t)
}

func testCannotInitPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Creating DiskStorageManager with invalid filename did not cause a panic.")
		}
	}()
	NewDiskStorageManager(DBDIR+"/"+InvalidFileName, false, false, true, true)
}

func testClosedPanic(t *testing.T, dsm *DiskStorageManager) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Closing a closed DiskStorageManager did not cause a panic.")
		}
	}()

	dsm.Close()
}

func testVersionCheckPanic(t *testing.T) {
	dsm := &DiskStorageManager{&ByteDiskStorageManager{DBDIR + "/test999", false, true, true, &sync.Mutex{},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Wrong version did not cause a panic.")
		}
	}()

	initByteDiskStorageManager(dsm.ByteDiskStorageManager)
}

func checkLocation(t *testing.T, loc uint64, record uint64, offset uint16) {
	lrecord := util.LocationRecord(loc)
	loffset := util.LocationOffset(loc)
	if lrecord != record || loffset != offset {
		t.Error("Unexpected location. Expected:", record, offset, "Got:", lrecord, loffset)
	}
}
