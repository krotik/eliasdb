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
	"fmt"
	"testing"

	"devt.de/eliasdb/storage"
	"devt.de/eliasdb/storage/file"
)

func TestIterator(t *testing.T) {

	// Do a very simple case

	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := NewHTree(sm)
	page := htree.Root

	if loc := page.Location(); loc != 1 {
		t.Error("Unexpected root location:", loc)
		return
	}

	// Fill up the tree

	page.Put([]byte("testkey1"), "test1")
	page.Put([]byte("testkey8"), "test8")
	page.Put([]byte("testkey5"), "test5")

	it := NewHTreeIterator(htree)

	if k, v := it.Next(); string(k) != "testkey1" || v != "test1" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if k, v := it.Next(); string(k) != "testkey8" || v != "test8" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if k, v := it.Next(); string(k) != "testkey5" || v != "test5" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if it.HasNext() {
		t.Error("Iterator should be finished")
		return
	}

	if k, v := it.Next(); k != nil || v != nil {
		t.Error("Return values should be nil")
		return
	}

	// Create a new iterator and try the same steps again

	it = NewHTreeIterator(htree)

	if k, v := it.Next(); string(k) != "testkey1" || v != "test1" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if k, v := it.Next(); string(k) != "testkey8" || v != "test8" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	// But before the iterator finishes we change the tree behind its back

	page.Put([]byte("testkey2"), "test2")
	page.Put([]byte("testkey3"), "test3")
	page.Put([]byte("testkey4"), "test4")
	page.Put([]byte("testkey6"), "test6")
	page.Put([]byte("testkey7"), "test7")
	page.Put([]byte("testkey9"), "test9")

	page.Put([]byte("testkey100"), "test100")

	page.Put([]byte("abba1"), "song1")

	page.Put([]byte("tfst"), "zzzz")

	// The tree has changed behind the iterator's back.

	if k, v := it.Next(); string(k) != "testkey5" || v != "test5" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	// We get now all the items in the Children array of the root page which
	// have been inserted after the current location. The iterator then finishes
	// normally.

	if k, v := it.Next(); string(k) != "tfst" || v != "zzzz" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if it.HasNext() {
		t.Error("Iterator should be finished")
		return
	}

	if k, v := it.Next(); k != nil || v != nil {
		t.Error("Return values should be nil")
		return
	}

	// Create a new iterator and see that we can iterate everything

	it = NewHTreeIterator(htree)

	if k, v := it.Next(); string(k) != "testkey100" || v != "test100" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if k, v := it.Next(); string(k) != "abba1" || v != "song1" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if k, v := it.Next(); string(k) != "testkey1" || v != "test1" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if k, v := it.Next(); string(k) != "testkey8" || v != "test8" {
		t.Error("Unexpected next result:", k, v)
		return
	}
	if k, v := it.Next(); string(k) != "testkey5" || v != "test5" {
		t.Error("Unexpected next result:", k, v)
		return
	}
	if k, v := it.Next(); string(k) != "testkey2" || v != "test2" {
		t.Error("Unexpected next result:", k, v)
		return
	}
	if k, v := it.Next(); string(k) != "testkey3" || v != "test3" {
		t.Error("Unexpected next result:", k, v)
		return
	}
	if k, v := it.Next(); string(k) != "testkey4" || v != "test4" {
		t.Error("Unexpected next result:", k, v)
		return
	}
	if k, v := it.Next(); string(k) != "testkey6" || v != "test6" {
		t.Error("Unexpected next result:", k, v)
		return
	}
	if k, v := it.Next(); string(k) != "testkey7" || v != "test7" {
		t.Error("Unexpected next result:", k, v)
		return
	}
	if k, v := it.Next(); string(k) != "testkey9" || v != "test9" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if k, v := it.Next(); string(k) != "tfst" || v != "zzzz" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if it.HasNext() {
		t.Error("Iterator should be finished")
		return
	}

	if k, v := it.Next(); k != nil || v != nil {
		t.Error("Return values should be nil")
		return
	}

	// Test error case

	it = NewHTreeIterator(htree)

	if k, v := it.Next(); string(k) != "testkey100" || v != "test100" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	sm.AccessMap[3] = storage.AccessCacheAndFetchSeriousError

	if k, v := it.Next(); string(k) != "abba1" || v != "song1" {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if k, v := it.Next(); k != nil || v != nil {
		t.Error("Unexpected next result:", k, v)
		return
	}

	if it.LastError != file.ErrAlreadyInUse {
		t.Error("Unexpected last error pointer of iterator")
		return
	}

	delete(sm.AccessMap, 3)

	if fmt.Sprint(it) != "HTree Iterator (tree: 1)\n"+
		"  path: []\n"+
		"  indices: []\n"+
		"  next: [] / <nil>\n" {
		t.Error("Unexpected tree string representation:", it)
		return
	}
}
