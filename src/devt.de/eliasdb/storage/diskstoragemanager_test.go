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
	"os"
	"sync"
	"testing"
	"time"

	"devt.de/common/lockutil"
	"devt.de/common/testutil"
	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/slotting/pageview"
	"devt.de/eliasdb/storage/util"
)

func TestDiskStorageManagerLockFilePanic(t *testing.T) {
	dsm := NewDiskStorageManager(DBDIR+"/test0", false, true, false)
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

	dsm := NewDiskStorageManager(DBDIR+"/test1", false, true, false)

	if dsm.Name() != "DiskStorageFile:"+DBDIR+"/test1" {
		t.Error("Unexpected name for DiskStorageManager:", dsm.Name())
		return
	}

	// Make sure that another process which attempts to open the same
	// storage would panic

	testLockfileStartPanic(t)

	// Test simple insert

	loc, err := dsm.Insert("This is a test")
	if err != nil {
		t.Error(err)
	}

	checkLocation(t, loc, 1, pageview.OFFSET_TRANS_DATA)

	dsm.Fetch(loc, &res)
	if res != "This is a test" {
		t.Error("Unexpected fetch result:", res)
	}

	// Get physical slot for stored data

	ploc, err := dsm.logical_slot_manager.Fetch(loc)
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

	new_ploc, err := dsm.logical_slot_manager.Fetch(loc)
	if err != nil {
		t.Error(err)
		return
	}

	if ploc == new_ploc {
		t.Error("Physical address should have changed")
		return
	}

	dsm.Fetch(loc, &res)
	if res != "This is another test" {
		t.Error("Unexpected fetch result:", res)
	}

	// Test insert error

	_, err = dsm.Insert(&testutil.GobTestObject{"test", true, false})
	if err == nil {
		t.Error(err)
		return
	}

	psp := dsm.physical_slots_pager
	fpsp := dsm.physical_free_slots_pager
	lsp := dsm.logical_slots_pager
	flsp := dsm.logical_free_slots_pager

	record, err := psp.StorageFile().Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = dsm.Insert(&testutil.GobTestObject{"test", false, false})
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

	_, err = dsm.Insert(&testutil.GobTestObject{"test", false, false})
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

	if !StorageFileExist(DBDIR + "/test1") {
		t.Error("Main disk storage file was not detected.")
		return
	}

	if StorageFileExist(DBDIR + "/" + INVALID_FILE_NAME) {
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

	NewDiskStorageManager(DBDIR+"/test1", false, false, false)
}

func TestDiskStorageManager2(t *testing.T) {
	var res string

	dsm := NewDiskStorageManager(DBDIR+"/test2", false, true, true)

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

	record, err := dsm.logical_slots_sf.Get(2)

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

	dsm.logical_slots_sf.ReleaseInUse(record)

	err = dsm.Fetch(util.PackLocation(3, 18), &res)
	if err != ErrSlotNotFound {
		t.Error(err)
		return
	}

	record, err = dsm.logical_slots_sf.Get(1)

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

	dsm.logical_slots_sf.ReleaseInUse(record)

	err = dsm.Update(util.PackLocation(2, 18), "test")
	if err != ErrSlotNotFound {
		t.Error(err)
		return
	}

	err = dsm.Update(loc, &testutil.GobTestObject{"test", true, false})
	if err == nil {
		t.Error(err)
		return
	}

	err = dsm.Update(loc, &testutil.GobTestObject{"test", false, false})
	if err != nil {
		t.Error(err)
		return
	}

	testres := &testutil.GobTestObject{"test", false, true}
	err = dsm.Fetch(loc, &testres)
	if err == nil {
		t.Error("Unexpected decode result")
		return
	}

	record, err = dsm.physical_slots_sf.Get(1)

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

	dsm.physical_slots_sf.ReleaseInUse(record)

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

	pl, _ := dsm.logical_slot_manager.Fetch(loc)
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

	pl, _ = dsm.logical_slot_manager.Fetch(loc)
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
}

func TestDiskStorageManager3(t *testing.T) {
	var res string

	dsm := NewDiskStorageManager(DBDIR+"/test3", false, true, true)

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

	record, _ := dsm.physical_slots_sf.Get(1)

	if dsm.Free(loc) != file.ErrAlreadyInUse {
		t.Error("Unexpected free result")
		return
	}

	dsm.physical_slots_sf.ReleaseInUse(record)

	record, _ = dsm.logical_slots_sf.Get(1)

	if dsm.Free(loc) != file.ErrAlreadyInUse {
		t.Error("Unexpected free result")
		return
	}

	dsm.logical_slots_sf.ReleaseInUse(record)

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
	dsm := NewDiskStorageManager(DBDIR+"/test4", false, false, true)

	var res string

	// Test expected behaviour

	loc, err := dsm.Insert("This is a test")
	if err != nil {
		t.Error(err)
	}

	checkLocation(t, loc, 1, pageview.OFFSET_TRANS_DATA)

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

	checkLocation(t, loc, 1, pageview.OFFSET_TRANS_DATA)

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

	psp := dsm.physical_slots_pager
	fpsp := dsm.physical_free_slots_pager
	lsp := dsm.logical_slots_pager
	flsp := dsm.logical_free_slots_pager

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

const INVALID_FILE_NAME = "**" + string(0x0)

func TestDiskStorageManagerInit(t *testing.T) {
	lockfile := lockutil.NewLockFile(DBDIR+"/"+"lock0.lck", time.Duration(50)*time.Millisecond)
	dsm := &DiskStorageManager{DBDIR + "/" + INVALID_FILE_NAME, true, true, &sync.Mutex{},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, lockfile}

	err := initDiskStorageManager(dsm)
	if err == nil {
		t.Error("Initialising a DiskStorageManager with an invalid filename should cause an error")
		return
	}

	testCannotInitPanic(t)

	dsm = &DiskStorageManager{DBDIR + "/test999", true, true, &sync.Mutex{},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}

	err = initDiskStorageManager(dsm)
	if err != nil {
		t.Error(err)
		return
	}

	dsm.SetRoot(ROOT_ID_VERSION, VERSION+1)

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
	NewDiskStorageManager(DBDIR+"/"+INVALID_FILE_NAME, false, true, true)
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
	dsm := &DiskStorageManager{DBDIR + "/test999", true, true, &sync.Mutex{},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Wrong version did not cause a panic.")
		}
	}()

	initDiskStorageManager(dsm)
}

func checkLocation(t *testing.T, loc uint64, record uint64, offset uint16) {
	lrecord := util.LocationRecord(loc)
	loffset := util.LocationOffset(loc)
	if lrecord != record || loffset != offset {
		t.Error("Unexpected location. Expected:", record, offset, "Got:", lrecord, loffset)
	}
}
