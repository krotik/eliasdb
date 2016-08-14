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

	"devt.de/common/datautil"
	"devt.de/common/fileutil"
	"devt.de/eliasdb/storage"
)

const DISKGRAPHSTORAGE_TEST_DBDIR = "diskgraphstoragetest1"
const DISKGRAPHSTORAGE_TEST_DBDIR2 = "diskgraphstoragetest2"

var DBDIRS = []string{DISKGRAPHSTORAGE_TEST_DBDIR, DISKGRAPHSTORAGE_TEST_DBDIR2}

const INVALID_FILE_NAME = "**" + string(0x0)

// Main function for all tests in this package

func TestMain(m *testing.M) {
	flag.Parse()

	for _, dbdir := range DBDIRS {
		if res, _ := fileutil.PathExists(dbdir); res {
			if err := os.RemoveAll(dbdir); err != nil {
				fmt.Print("Could not remove test directory:", err.Error())
			}
		}
	}

	// Run the tests

	res := m.Run()

	// Teardown

	for _, dbdir := range DBDIRS {
		if res, _ := fileutil.PathExists(dbdir); res {
			if err := os.RemoveAll(dbdir); err != nil {
				fmt.Print("Could not remove test directory:", err.Error())
			}
		}
	}

	os.Exit(res)
}

func TestDiskGraphStorage(t *testing.T) {
	dgsnew, err := NewDiskGraphStorage(DISKGRAPHSTORAGE_TEST_DBDIR)
	if err != nil {
		t.Error(err)
		return
	}

	if res := dgsnew.Name(); res != DISKGRAPHSTORAGE_TEST_DBDIR {
		t.Error("Unexpected name:", res)
		return
	}

	// Check that the storage directory exists

	if res, _ := fileutil.PathExists(DISKGRAPHSTORAGE_TEST_DBDIR); !res {
		t.Error("Storage directory does not exist")
		return
	}

	if res, _ := fileutil.PathExists(DISKGRAPHSTORAGE_TEST_DBDIR + "/" + FILENAME_NAME_DB); !res {
		t.Error("Name DB does not exist")
		return
	}

	// Get a storage file

	sm1 := dgsnew.StorageManager("store1.nodes", true)

	if sm1 == nil {
		t.Error("Unexpected result")
		return
	}

	if res, _ := fileutil.PathExists(DISKGRAPHSTORAGE_TEST_DBDIR + "/store1.nodes.db.0"); !res {
		t.Error("Storage file does not exist")
		return
	}

	sm2 := dgsnew.StorageManager("store2.nodes", false)

	if res, _ := fileutil.PathExists(DISKGRAPHSTORAGE_TEST_DBDIR + "/store2.nodes.db.0"); res {
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

	if err := dgsnew.Close(); err != nil {
		t.Error(err)
		return
	}

	// Open the storage again to make sure we can load it

	dgs, err := NewDiskGraphStorage(DISKGRAPHSTORAGE_TEST_DBDIR)
	if err != nil {
		t.Error(err)
		return
	}

	if res := dgs.MainDB()["test1"]; res != "test1value" {
		t.Error("Unexpected value in mainDB value")
		return
	}

	if err := dgs.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestDiskGraphStorageErrors(t *testing.T) {
	_, err := NewDiskGraphStorage(INVALID_FILE_NAME)
	if err == nil {
		t.Error("Unexpected new disk graph storage result")
		return
	}

	// Test names map error case

	old := FILENAME_NAME_DB
	FILENAME_NAME_DB = INVALID_FILE_NAME

	_, err = NewDiskGraphStorage(DISKGRAPHSTORAGE_TEST_DBDIR2)
	if err == nil {
		t.Error("Unexpected new disk graph storage result")
		FILENAME_NAME_DB = old
		return
	}

	_, err = NewDiskGraphStorage(DISKGRAPHSTORAGE_TEST_DBDIR2)
	if err == nil {
		t.Error("Unexpected new disk graph storage result")
		FILENAME_NAME_DB = old
		return
	}

	FILENAME_NAME_DB = old

	dgs := &DiskGraphStorage{INVALID_FILE_NAME, nil, make(map[string]storage.StorageManager)}
	pm, _ := datautil.NewPersistentMap(INVALID_FILE_NAME)
	dgs.mainDB = pm

	msm := storage.NewMemoryStorageManager("test")
	dgs.storagemanagers["test"] = msm
	storage.MsmRetClose = errors.New("TestError")

	if err := dgs.RollbackMain(); err == nil {
		t.Error("Unexpected flush result")
		return
	}

	if err := dgs.FlushMain(); err == nil {
		t.Error("Unexpected flush result")
		return
	}

	if err := dgs.Close(); err == nil {
		t.Error("Unexpected close result")
		return
	}
}
