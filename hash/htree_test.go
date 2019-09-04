/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package hash

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/eliasdb/storage"
	"devt.de/krotik/eliasdb/storage/file"
)

const DBDIR = "htreetest"

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup
	if res, _ := fileutil.PathExists(DBDIR); res {
		os.RemoveAll(DBDIR)
	}

	err := os.Mkdir(DBDIR, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests
	res := m.Run()

	// Teardown
	err = os.RemoveAll(DBDIR)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)
}

func TestHTreeSerialization(t *testing.T) {
	sm := storage.NewDiskStorageManager(DBDIR+"/test1", false, false, false, false)

	htree, err := NewHTree(sm)
	if err != nil {
		t.Error(err)
		return
	}

	loc := htree.Location()

	htree.Put([]byte("test"), "testvalue1")

	sm.Close()

	sm2 := storage.NewDiskStorageManager(DBDIR+"/test1", false, false, false, false)

	htree2, _ := LoadHTree(sm2, loc)

	if res, err := htree2.Get([]byte("test")); res != "testvalue1" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	sm2.Close()
}

func TestHTree(t *testing.T) {
	sm := storage.NewMemoryStorageManager("testsm")

	sm.AccessMap[1] = storage.AccessInsertError

	if _, err := NewHTree(sm); err != file.ErrAlreadyInUse {
		t.Error("Unexpected new tree result:", err)
		return
	}

	delete(sm.AccessMap, 1)

	htree, _ := NewHTree(sm)

	page := htree.Root

	if loc := page.Location(); loc != 1 {
		t.Error("Unexpected root location:", loc)
		return
	}

	// Fill up the tree

	page.Put([]byte("testkey1"), "test1")
	page.Put([]byte("testkey2"), "test2")
	page.Put([]byte("testkey3"), "test3")
	page.Put([]byte("testkey4"), "test4")
	page.Put([]byte("testkey5"), "test5")
	page.Put([]byte("testkey6"), "test6")
	page.Put([]byte("testkey7"), "test7")
	page.Put([]byte("testkey8"), "test8")
	page.Put([]byte("testkey9"), "test9")
	page.Put([]byte("testkey10"), "test10")

	if page.Location() != 1 {
		t.Error("Unexpected location for tree")
		return
	}

	// Reload HTree

	htreeCached, _ := LoadHTree(sm, page.Location())

	sm.AccessMap[1] = storage.AccessCacheAndFetchError

	if _, err := LoadHTree(sm, page.Location()); err != storage.ErrSlotNotFound {
		t.Error("Unexpected tree load result:", err)
		return
	}

	delete(sm.AccessMap, 1)

	sm.AccessMap[1] = storage.AccessNotInCache
	htreeFetched, _ := LoadHTree(sm, page.Location())

	delete(sm.AccessMap, 1)

	if htreeCached.Location() != htreeFetched.Location() {
		t.Error("Trees have different locations:", htreeCached.Location(), htreeFetched.Location())
		return
	}

	sm.AccessMap[8] = storage.AccessNotInCache

	if res, err := htree.Get([]byte("testkey5")); res != "test5" || err != nil {
		t.Error("Unexpected get result:", res, err)
		return
	}

	if res, loc, err := htree.GetValueAndLocation([]byte("testkey5")); res != "test5" || loc == 0 || err != nil {
		t.Error("Unexpected get result:", res, loc, err)
		return
	}

	if res, loc, err := htree.GetValueAndLocation([]byte("testkey99")); res != nil || loc != 0 || err != nil {
		t.Error("Unexpected get result:", res, loc, err)
		return
	}

	delete(sm.AccessMap, 1)

	// Test proxy functions

	if res, err := htree.Exists([]byte("testkey6")); res != true || err != nil {
		t.Error("Unexpected exist result:", res, err)
		return
	}

	if res, err := htree.Put([]byte("testkey6"), "test7"); res != "test6" || err != nil {
		t.Error("Unexpected exist result:", res, err)
		return
	}

	if res, err := htree.Remove([]byte("testkey6")); res != "test7" || err != nil {
		t.Error("Unexpected exist result:", res, err)
		return
	}

	if res, err := htree.Exists([]byte("testkey6")); res != false || err != nil {
		t.Error("Unexpected exist result:", res, err)
		return
	}
}
