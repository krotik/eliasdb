/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package graphstorage

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/eliasdb/storage"
)

const diskGraphStorageTestDBDir = "diskgraphstoragetest1"
const diskGraphStorageTestDBDir2 = "diskgraphstoragetest2"

var dbdirs = []string{diskGraphStorageTestDBDir, diskGraphStorageTestDBDir2}

const invalidFileName = "**" + "\x00"

// Main function for all tests in this package

func TestMain(m *testing.M) {
	flag.Parse()

	for _, dbdir := range dbdirs {
		if res, _ := fileutil.PathExists(dbdir); res {
			if err := os.RemoveAll(dbdir); err != nil {
				fmt.Print("Could not remove test directory:", err.Error())
			}
		}
	}

	// Run the tests

	res := m.Run()

	// Teardown

	for _, dbdir := range dbdirs {
		if res, _ := fileutil.PathExists(dbdir); res {
			if err := os.RemoveAll(dbdir); err != nil {
				fmt.Print("Could not remove test directory:", err.Error())
			}
		}
	}

	os.Exit(res)
}

func TestDiskGraphStorage(t *testing.T) {
	dgsnew, err := NewDiskGraphStorage(diskGraphStorageTestDBDir, false)
	if err != nil {
		t.Error(err)
		return
	}

	if res := dgsnew.Name(); res != diskGraphStorageTestDBDir {
		t.Error("Unexpected name:", res)
		return
	}

	// Check that the storage directory exists

	if res, _ := fileutil.PathExists(diskGraphStorageTestDBDir); !res {
		t.Error("Storage directory does not exist")
		return
	}

	if res, _ := fileutil.PathExists(diskGraphStorageTestDBDir + "/" + FilenameNameDB); !res {
		t.Error("Name DB does not exist")
		return
	}

	// Get a storage file

	sm1 := dgsnew.StorageManager("store1.nodes", true)

	if sm1 == nil {
		t.Error("Unexpected result")
		return
	}

	if res, _ := fileutil.PathExists(diskGraphStorageTestDBDir + "/store1.nodes.db.0"); !res {
		t.Error("Storage file does not exist")
		return
	}

	sm2 := dgsnew.StorageManager("store2.nodes", false)

	if res, _ := fileutil.PathExists(diskGraphStorageTestDBDir + "/store2.nodes.db.0"); res {
		t.Error("Storage file should not have been created")
		return
	}

	if sm2 != nil {
		t.Error("Unexpected result")
		return
	}

	m := dgsnew.MainDB()
	m["test1"] = "test1value"
	dgsnew.FlushMain()
	dgsnew.RollbackMain()

	if err := dgsnew.FlushAll(); err != nil {
		t.Error("Unexpected error return:", err)
	}

	if err := dgsnew.Close(); err != nil {
		t.Error(err)
		return
	}

	// Open the storage again to make sure we can load it

	dgs, err := NewDiskGraphStorage(diskGraphStorageTestDBDir, false)
	if err != nil {
		t.Error(err)
		return
	}

	if res := dgs.MainDB()["test1"]; res != "test1value" {
		t.Error("Unexpected value in mainDB value:", res)
		return
	}

	// Check readonly mode

	dgs.(*DiskGraphStorage).readonly = true

	if err := dgs.RollbackMain(); err.Error() != "GraphError: Failed write to readonly storage (Cannot rollback main db)" {
		t.Error("Unexpected error return:", err)
	}

	if err := dgs.FlushMain(); err.Error() != "GraphError: Failed write to readonly storage (Cannot flush main db)" {
		t.Error("Unexpected error return:", err)
	}

	if err := dgs.FlushAll(); err != nil {
		t.Error("Unexpected error return:", err)
	}

	if err := dgs.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestDiskGraphStorageErrors(t *testing.T) {
	_, err := NewDiskGraphStorage(invalidFileName, false)
	if err == nil {
		t.Error("Unexpected new disk graph storage result")
		return
	}

	// Test names map error case

	old := FilenameNameDB
	FilenameNameDB = invalidFileName

	_, err = NewDiskGraphStorage(diskGraphStorageTestDBDir2, false)
	if err == nil {
		t.Error("Unexpected new disk graph storage result")
		FilenameNameDB = old
		return
	}

	_, err = NewDiskGraphStorage(diskGraphStorageTestDBDir2, false)
	if err == nil {
		t.Error("Unexpected new disk graph storage result")
		FilenameNameDB = old
		return
	}

	FilenameNameDB = old

	dgs := &DiskGraphStorage{invalidFileName, false, nil,
		make(map[string]storage.Manager)}
	pm, _ := datautil.NewPersistentStringMap(invalidFileName)
	dgs.mainDB = pm

	msm := storage.NewMemoryStorageManager("test")
	dgs.storagemanagers["test"] = msm
	storage.MsmRetFlush = errors.New("TestError")
	storage.MsmRetClose = errors.New("TestError")

	if err := dgs.RollbackMain(); err == nil {
		t.Error("Unexpected flush result")
		return
	}

	if err := dgs.FlushMain(); err == nil {
		t.Error("Unexpected flush result")
		return
	}

	if err := dgs.FlushAll(); err == nil {
		t.Error("Unexpected flush all result")
		return
	}

	if err := dgs.Close(); err == nil {
		t.Error("Unexpected close result")
		return
	}
}
