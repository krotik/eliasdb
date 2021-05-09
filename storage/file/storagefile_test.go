/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package file

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"devt.de/krotik/common/fileutil"
)

const DBDir = "storagefiletest"

const InvalidFileName = "**" + "\x00"

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup
	if res, _ := fileutil.PathExists(DBDir); res {
		os.RemoveAll(DBDir)
	}

	err := os.Mkdir(DBDir, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests
	res := m.Run()

	// Teardown
	err = os.RemoveAll(DBDir)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)
}

func TestStorageFileInitialisation(t *testing.T) {

	// \0 and / are the only illegal characters for filenames in unix

	sf, err := NewDefaultStorageFile(DBDir+"/"+InvalidFileName, true)
	if err == nil {
		t.Error("Invalid name should cause an error")
		return
	}

	sf, err = NewDefaultStorageFile(DBDir+"/test1", true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if sf.Name() != DBDir+"/test1" {
		t.Error("Unexpected name of StorageFile:", sf.Name())
		return
	}

	if sf.RecordSize() != DefaultRecordSize {
		t.Error("Unexpected record size:", sf.RecordSize())
		return
	}

	defer sf.Close()

	res, err := fileutil.PathExists(DBDir + "/test1.0")
	if err != nil {
		t.Error(err)
		return
	}
	if !res {
		t.Error("Expected db file test1.0 does not exist")
		return
	}

	if len(sf.files) != 1 {
		t.Error("Unexpected number of files in StorageFile:", sf.files)
		return
	}
}

func TestGetFile(t *testing.T) {
	sf := &StorageFile{DBDir + "/test2", true, 10, 10, nil, nil, nil, nil,
		make([]*os.File, 0), nil}
	defer sf.Close()

	file, err := sf.getFile(0)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if file.Name() != DBDir+"/test2.0" {
		t.Error("Unexpected file from getFile")
		return
	}
	checkFilesArray(t, sf, 1, 0, DBDir+"/test2.0")

	file, err = sf.getFile(42)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if file.Name() != DBDir+"/test2.4" {
		t.Error("Unexpected file from getFile")
		return
	}
	checkFilesArray(t, sf, 5, 0, DBDir+"/test2.0")
	checkFilesArray(t, sf, 5, 1, "")
	checkFilesArray(t, sf, 5, 2, "")
	checkFilesArray(t, sf, 5, 3, "")
	checkFilesArray(t, sf, 5, 4, DBDir+"/test2.4")

	file, err = sf.getFile(25)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if file.Name() != DBDir+"/test2.2" {
		t.Error("Unexpected file from getFile")
		return
	}
	checkFilesArray(t, sf, 5, 0, DBDir+"/test2.0")
	checkFilesArray(t, sf, 5, 1, "")
	checkFilesArray(t, sf, 5, 2, DBDir+"/test2.2")
	checkFilesArray(t, sf, 5, 3, "")
	checkFilesArray(t, sf, 5, 4, DBDir+"/test2.4")

	file, err = sf.getFile(11)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if file.Name() != DBDir+"/test2.1" {
		t.Error("Unexpected file from getFile")
		return
	}
	checkFilesArray(t, sf, 5, 0, DBDir+"/test2.0")
	checkFilesArray(t, sf, 5, 1, DBDir+"/test2.1")
	checkFilesArray(t, sf, 5, 2, DBDir+"/test2.2")
	checkFilesArray(t, sf, 5, 3, "")
	checkFilesArray(t, sf, 5, 4, DBDir+"/test2.4")

	file, err = sf.getFile(49)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if file.Name() != DBDir+"/test2.4" {
		t.Error("Unexpected file from getFile")
		return
	}
	checkFilesArray(t, sf, 5, 0, DBDir+"/test2.0")
	checkFilesArray(t, sf, 5, 1, DBDir+"/test2.1")
	checkFilesArray(t, sf, 5, 2, DBDir+"/test2.2")
	checkFilesArray(t, sf, 5, 3, "")
	checkFilesArray(t, sf, 5, 4, DBDir+"/test2.4")
}

func checkFilesArray(t *testing.T, sf *StorageFile, explen int, pos int, name string) {
	if len(sf.files) != explen {
		t.Error("Unexpected files array:", sf.files, " expected size:", explen)
	}

	file := sf.files[pos]

	if name == "" && file != nil {
		t.Error("Unexpected file at pos:", pos, " name:", file.Name())
	} else if name != "" && file == nil {
		t.Error("Unexpected nil pointer at pos:", pos, " expected name:", name)
	} else if file != nil && name != file.Name() {
		t.Error("Unexpected file at pos:", pos, " name:", file.Name(), " expected name:", name)
	}
}

func TestLowLevelReadWrite(t *testing.T) {

	// Create a new record and write it

	sf, err := NewDefaultStorageFile(DBDir+"/test3", true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record := sf.createRecord(1)
	record.WriteSingleByte(5, 0x42)

	oldfiles := sf.files
	sf.name = DBDir + "/" + InvalidFileName
	sf.files = make([]*os.File, 0)

	_, err = sf.Get(1)

	if err == nil {
		t.Error("Invalid filename should cause an error")
		return
	}

	err = sf.writeRecord(record)

	if err == nil {
		t.Error("Invalid filename should cause an error")
		return
	}

	sf.name = DBDir + "/test3"
	sf.files = oldfiles

	err = sf.writeRecord(record)

	if err != nil {
		t.Error("Writing with a correct name should succeed", err)
		return
	}

	sf.Close()

	sf, err = NewDefaultStorageFile(DBDir+"/test3", true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record = sf.createRecord(1)

	oldfiles = sf.files
	sf.name = DBDir + "/" + InvalidFileName
	sf.files = make([]*os.File, 0)

	record.data = nil

	err = sf.readRecord(record)

	if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrNilData {
		t.Error("Nil pointer in record data should cause an error")
		return
	}

	record.ClearData()

	err = sf.readRecord(record)

	if err == nil {
		t.Error("Invalid filename should cause an error")
		return
	}

	sf.name = DBDir + "/test3"
	sf.files = oldfiles

	oldrecordSize := sf.recordSize
	sf.recordSize = DefaultRecordSize - 1

	testReadRecordPanic(t, sf, record)

	sf.recordSize = oldrecordSize

	err = sf.readRecord(record)

	if err != nil {
		t.Error("Reading with a correct name should succeed")
		return
	}

	sf.Close()

	if record.ReadSingleByte(5) != 0x42 {
		t.Error("Couldn't read byte which was written before.")
		return
	}
}

func testReadRecordPanic(t *testing.T, sf *StorageFile, r *Record) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Changing of the record size did not cause a panic.")
		}
	}()

	sf.readRecord(r)
}

func TestHighLevelGetRelease(t *testing.T) {

	// Create some records and write to them

	sf, err := NewDefaultStorageFile(DBDir+"/test4", true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	// Get records and check that the expected files are there

	record1, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	checkPath(t, "test4.0")
	checkMap(t, &sf.inUse, record1.ID(), true, "Record1", "in use")

	record2, err := sf.Get((DefaultFileSize/DefaultRecordSize)*4 + 5)
	if err != nil {
		t.Error(err)
		return
	}

	checkPath(t, "test4.4")
	checkMap(t, &sf.inUse, record2.ID(), true, "Record2", "in use")

	record3, err := sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	checkPath(t, "test4.0")

	// Make sure the retrieved records are marked in use

	checkMap(t, &sf.inUse, record3.ID(), true, "Record3", "in use")

	checkMap(t, &sf.free, record1.ID(), false, "Record1", "in use")
	checkMap(t, &sf.free, record2.ID(), false, "Record2", "in use")
	checkMap(t, &sf.free, record3.ID(), false, "Record3", "in use")

	// Now use the records and release them

	record1.WriteUInt16(2, 0x4268)
	record1.WriteUInt16(10, 0x66)

	record2.WriteInt32(11, -0x7654321)

	sf.ReleaseInUse(record2)

	// A rollback should have no consequences
	sf.Rollback()

	// Check that the records have been released and scheduled for write
	// (i.e. they are in the dirty table)

	checkMap(t, &sf.dirty, record2.ID(), true, "Record2", "dirty")
	checkMap(t, &sf.inUse, record2.ID(), false, "Record2", "in use")

	_, err = sf.Get((DefaultFileSize/DefaultRecordSize)*4 + 5)
	if err != nil {
		t.Error(err)
		return
	}
	checkMap(t, &sf.dirty, record2.ID(), false, "Record2", "dirty")
	checkMap(t, &sf.inUse, record2.ID(), true, "Record2", "in use")

	sf.ReleaseInUse(record1)
	checkMap(t, &sf.dirty, record1.ID(), true, "Record1", "dirty")
	checkMap(t, &sf.inUse, record1.ID(), false, "Record1", "in use")
	checkMap(t, &sf.dirty, record2.ID(), false, "Record2", "dirty")
	checkMap(t, &sf.inUse, record2.ID(), true, "Record2", "in use")
	checkMap(t, &sf.dirty, record3.ID(), false, "Record3", "dirty")
	checkMap(t, &sf.inUse, record3.ID(), true, "Record3", "in use")

	sf.ReleaseInUseID(record2.ID(), true)
	checkMap(t, &sf.dirty, record1.ID(), true, "Record1", "dirty")
	checkMap(t, &sf.inUse, record1.ID(), false, "Record1", "in use")
	checkMap(t, &sf.dirty, record2.ID(), true, "Record2", "dirty")
	checkMap(t, &sf.inUse, record2.ID(), false, "Record2", "in use")
	checkMap(t, &sf.dirty, record3.ID(), false, "Record3", "dirty")
	checkMap(t, &sf.inUse, record3.ID(), true, "Record3", "in use")

	err = sf.Flush()

	if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrInUse {
		t.Error("StorageFile should complain about records being in use")
	}

	record4, err := sf.Get(5)
	if err != nil {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record3)
	checkMap(t, &sf.dirty, record1.ID(), true, "Record1", "dirty")
	checkMap(t, &sf.inUse, record1.ID(), false, "Record1", "in use")
	checkMap(t, &sf.dirty, record2.ID(), true, "Record2", "dirty")
	checkMap(t, &sf.inUse, record2.ID(), false, "Record2", "in use")

	// Check that a record which has not been written to is put into the
	// free map

	checkMap(t, &sf.free, record3.ID(), true, "Record3", "free")
	checkMap(t, &sf.dirty, record3.ID(), false, "Record3", "dirty")
	checkMap(t, &sf.inUse, record3.ID(), false, "Record3", "in use")

	// Test string representation of the StorageFile

	if sf.String() != "Storage File: storagefiletest/test4 "+
		"(transDisabled:true recordSize:4096 maxFileSize:9999998976)\n"+
		"====\n"+
		"Free Records: 2\n"+
		"InUse Records: 5\n"+
		"InTrans Records: \n"+
		"Dirty Records: 1, 9765629\n"+
		"Open files: storagefiletest/test4.0 (0), storagefiletest/test4.4 (4)\n"+
		"====\n" {
		t.Error("Unexpected string representation of StorageFile:", sf.String())
	}

	sf.ReleaseInUse(record4)

	// Check that after the changes have been written to disk that
	// all records are in the free map

	sf.Flush()

	checkMap(t, &sf.dirty, record1.ID(), false, "Record1", "dirty")
	checkMap(t, &sf.inUse, record1.ID(), false, "Record1", "in use")
	checkMap(t, &sf.free, record1.ID(), true, "Record1", "in use")
	checkMap(t, &sf.dirty, record2.ID(), false, "Record2", "dirty")
	checkMap(t, &sf.inUse, record2.ID(), false, "Record2", "in use")
	checkMap(t, &sf.free, record2.ID(), true, "Record2", "in use")
	checkMap(t, &sf.dirty, record3.ID(), false, "Record3", "dirty")
	checkMap(t, &sf.inUse, record3.ID(), false, "Record3", "in use")
	checkMap(t, &sf.free, record3.ID(), true, "Record3", "in use")

	_, err = sf.Get((DefaultFileSize/DefaultRecordSize)*4 + 5)
	if err != nil {
		t.Error(err)
		return
	}
	checkMap(t, &sf.free, record2.ID(), false, "Record2", "free")
	checkMap(t, &sf.inUse, record2.ID(), true, "Record2", "in use")

	sf.ReleaseInUse(record2)
	checkMap(t, &sf.free, record2.ID(), true, "Record2", "free")
	checkMap(t, &sf.inUse, record2.ID(), false, "Record2", "in use")

	sf.Close()

	// Open the storage file again with a different object and
	// try to read back the written

	sf, err = NewDefaultStorageFile(DBDir+"/test4", true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record1, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	// Test that requesting a record twice without releasing it causes an error.

	_, err = sf.Get(1)
	if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrAlreadyInUse {
		t.Error("Requesting a record which is already in use should cause an error")
	}

	if err.Error() != "Record is already in-use (storagefiletest/test4 - Record 1)" {
		t.Error("Unexpected error string:", err)
		return
	}

	checkMap(t, &sf.inUse, record1.ID(), true, "Record1", "in use")

	record2, err = sf.Get((DefaultFileSize/DefaultRecordSize)*4 + 5)
	if err != nil {
		t.Error(err)
		return
	}
	checkMap(t, &sf.inUse, record2.ID(), true, "Record2", "in use")

	// Check that we can read back the written data

	if d := record1.ReadUInt16(2); d != 0x4268 {
		t.Error("Expected value in record1 not found")
		return
	}
	if d := record1.ReadUInt16(10); d != 0x66 {
		t.Error("Expected value in record1 not found")
		return
	}
	if d := record2.ReadInt32(11); d != -0x7654321 {
		t.Error("Expected value in record1 not found", d)
		return
	}

	sf.ReleaseInUse(record1)

	// Since record3 was just created and is empty it should not be in use

	record3 = sf.createRecord(5)
	checkMap(t, &sf.inUse, record3.ID(), false, "Record3", "in use")

	// Record2 has been used

	checkMap(t, &sf.inUse, record2.ID(), true, "Record2", "in use")

	// An attempt to close the file should return an error

	err = sf.Close()

	if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrInUse {
		t.Error("Attempting to close a StorageFile with records in use should " +
			"return an error")
		return
	}

	sf.ReleaseInUse(record2)

	err = sf.Close()
	if err != nil {
		t.Error(err)
		return
	}
}

func checkPath(t *testing.T, path string) {
	res, err := fileutil.PathExists(DBDir + "/" + path)
	if err != nil {
		t.Error(err)
	}
	if !res {
		t.Error("Expected db file", path, "does not exist")
	}
}

func checkMap(t *testing.T, mapvar *map[uint64]*Record, id uint64, expected bool,
	name string, mapname string) {

	if _, ok := (*mapvar)[id]; expected != ok {
		if expected {
			t.Error(name, "should be", mapname)
		} else {
			t.Error(name, "should not be", mapname)
		}
	}
}

func TestFlushingClosing(t *testing.T) {

	sf, err := NewDefaultStorageFile(DBDir+"/test5", true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if sf.Flush() != nil {
		t.Error("Flushing an unused file should not cause an error")
		return
	}

	record, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}
	record.WriteSingleByte(0, 0)

	err = sf.Flush()
	if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrInUse {
		t.Error("Flushing should not be allowed while records are in use")
		return
	}

	sf.ReleaseInUse(nil) // This should not cause a panic
	err = sf.ReleaseInUseID(5000, true)
	if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrNotInUse {
		t.Error("It should not be possible to release records which are not in use")
		return
	}
	record.ClearDirty()
	sf.ReleaseInUseID(1, true)

	if !record.Dirty() {
		t.Error("Record should be marked as dirty after it was released as dirty")
		return
	}

	testReleasePanic(t, sf, record)

	// Modifying the id at this point causes unspecified behaviour. Once a
	// record was released it should not be modified.
	record.data = nil

	err = sf.Flush()
	if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrNilData {
		t.Error("It should not be possible to flush a record with an invalid id to disk")
		return
	}

	checkMap(t, &sf.dirty, 1, true, "Record1", "dirty")

	// Get the record again and discard it

	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	checkMap(t, &sf.inUse, 1, true, "Record1", "in use")

	// Need to correct the id otherwise the discarding will not work
	record.SetID(1)

	sf.Discard(nil) // This should not cause a panic
	sf.Discard(record)

	checkMap(t, &sf.dirty, 1, false, "Record1", "dirty")
	checkMap(t, &sf.inUse, 1, false, "Record1", "in use")

	sf.Sync() // This should just complete and not cause a panic

	record, err = sf.Get(5)
	if err != nil {
		t.Error(err)
		return
	}

	// This should be possible even if the record is not dirty at all
	sf.ReleaseInUseID(record.ID(), true)

	checkMap(t, &sf.dirty, 5, true, "Record1", "dirty")

	recordData := record.data
	record.data = nil

	err = sf.Close()
	if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrNilData {
		t.Error("Closing with a dirty record with negative id should not be possible", err)
		return
	}

	record.data = recordData

	err = sf.Close()
	if err != nil {
		t.Error(err)
		return
	}

	// Make sure the close call did flush dirty records

	checkMap(t, &sf.dirty, 5, false, "Record1", "dirty")
}

func testReleasePanic(t *testing.T, sf *StorageFile, r *Record) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Releaseing a record multiple times without using it " +
				"did not cause a panic.")
		}
	}()
	sf.ReleaseInUse(r)
}
