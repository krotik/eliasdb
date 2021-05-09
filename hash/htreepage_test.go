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

	"devt.de/krotik/eliasdb/storage"
	"devt.de/krotik/eliasdb/storage/file"
)

func TestHTreePageFetchExists(t *testing.T) {
	sm := storage.NewMemoryStorageManager("testsm")
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

	// Test negative cases

	if res, err := page.Exists([]byte("testkey40")); res != false || err != nil {
		t.Error("Unexpected exists result:", res, err)
		return
	}
	if res, _, err := page.Get([]byte("testkey40")); res != nil || err != nil {
		t.Error("Unexpected get result:", res, err)
		return
	}

	// Test positive cases

	if res, err := page.Exists([]byte("testkey4")); res != true || err != nil {
		t.Error("Unexpected exists result:", res, err)
		return
	}
	if res, _, err := page.Get([]byte("testkey4")); res != "test4" || err != nil {
		t.Error("Unexpected get result:", res, err)
		return
	}

	// Test error cases

	sm.AccessMap[8] = storage.AccessCacheAndFetchError

	if res, err := page.Exists([]byte("testkey4")); res != false || err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected exists result:", res, err)
		return
	}
	if res, _, err := page.Get([]byte("testkey4")); res != nil || err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected get result:", res, err)
		return
	}

	delete(sm.AccessMap, 8)
}

func TestHTreePageInsert(t *testing.T) {
	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := NewHTree(sm)
	page := htree.Root

	// Empty page operations

	if val, _, err := page.Get([]byte("test")); val != nil || err != nil {
		t.Error("Unexpected get result:", val, err)
		return
	}

	// Test error cases

	sm.AccessMap[2] = storage.AccessInsertError

	_, err := page.Put([]byte("testkey1"), "test1")
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected put result:", err)
		return
	}

	delete(sm.AccessMap, 2)

	sm.AccessMap[1] = storage.AccessUpdateError

	if _, err := page.Put([]byte("testkey1"), "test1"); err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected put result:", err)
		return
	}

	delete(sm.AccessMap, 1)

	// Fill the tree

	page.Put([]byte("testkey1"), "test1")

	// Test another error case

	sm.AccessMap[2] = storage.AccessCacheAndFetchError

	if _, err := page.Put([]byte("testkey2"), "test2"); err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected put result:", err)
		return
	}

	delete(sm.AccessMap, 2)

	page.Put([]byte("testkey2"), "test2")
	page.Put([]byte("testkey3"), "test3")
	page.Put([]byte("testkey4"), "test4")
	page.Put([]byte("testkey5"), "test5")
	page.Put([]byte("testkey6"), "test6")
	page.Put([]byte("testkey7"), "test7")
	page.Put([]byte("testkey8"), "test8")

	// Check we have one full bucket

	if cc := countChildren(page); cc != 1 {
		t.Error("Unexpected number of children:", cc)
		return
	}

	// Now insert more data and see that the bucket is split

	// First test some error cases

	sm.AccessMap[3] = storage.AccessInsertError

	_, err = page.Put([]byte("testkey9"), "test9")
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected put result:", err)
		return
	}

	delete(sm.AccessMap, 3)

	sm.AccessMap[1] = storage.AccessUpdateError

	if _, err := page.Put([]byte("testkey9"), "test9"); err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected put result:", err)
		return
	}

	// Cleanup LocCount

	sm.LocCount--

	delete(sm.AccessMap, 1)

	// This puts the first bucket on the lowest level and extends it by 1

	page.Put([]byte("testkey9"), "test9")

	// Test sanity check for cache

	testMaxDepthExceededPanic(t, page, sm)

	// This creates a new bucket under the root

	page.Put([]byte("testkey10"), "test10")

	if cc := countChildren(page); cc != 2 {
		t.Error("Unexpected number of children:", cc)
		return
	}

	total, pages, buckets := countNodes(sm)
	if total != 6 || pages != 4 || buckets != 2 {
		t.Error("Unexpected number of nodes:", total, pages, buckets)
		return
	}

	// Test printing

	if out := htree.String(); out != "HTree: testsm (1)\n"+
		"HashPage 1 (depth: 0)\n"+
		"  Hash 000000B9 (loc: 3)\n"+
		"  HashPage 3 (depth: 1)\n"+
		"    Hash 000000B0 (loc: 5)\n"+
		"    HashPage 5 (depth: 2)\n"+
		"      Hash 00000069 (loc: 7)\n"+
		"      HashPage 7 (depth: 3)\n"+
		"        Hash 000000AD (loc: 8)\n"+
		"        HashBucket (9 elements, depth: 4)\n"+
		"        [116 101 115 116 107 101 121 49] - test1\n"+
		"        [116 101 115 116 107 101 121 50] - test2\n"+
		"        [116 101 115 116 107 101 121 51] - test3\n"+
		"        [116 101 115 116 107 101 121 52] - test4\n"+
		"        [116 101 115 116 107 101 121 53] - test5\n"+
		"        [116 101 115 116 107 101 121 54] - test6\n"+
		"        [116 101 115 116 107 101 121 55] - test7\n"+
		"        [116 101 115 116 107 101 121 56] - test8\n"+
		"        [116 101 115 116 107 101 121 57] - test9\n"+
		"  Hash 000000DE (loc: 9)\n"+
		"  HashBucket (1 element, depth: 1)\n"+
		"  [116 101 115 116 107 101 121 49 48] - test10\n"+
		"  [] - <nil>\n"+
		"  [] - <nil>\n"+
		"  [] - <nil>\n"+
		"  [] - <nil>\n"+
		"  [] - <nil>\n"+
		"  [] - <nil>\n"+
		"  [] - <nil>\n" {
		t.Error("Unexpected htree output:", out)
	}

	if existing, err := page.Put([]byte("testkey10"), "test11"); existing != "test10" {
		t.Error("Unexpected put result", existing, err)
		return
	}
}

func TestHTreePageRemove(t *testing.T) {
	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := NewHTree(sm)
	page := htree.Root

	if val, err := page.Remove(nil); val != nil || err != nil {
		t.Error("Unexpected get result:", val, err)
		return
	}

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

	// Check the numbers

	if cc := countChildren(page); cc != 2 {
		t.Error("Unexpected number of children:", cc)
		return
	}

	total, pages, buckets := countNodes(sm)
	if total != 6 || pages != 4 || buckets != 2 {
		t.Error("Unexpected number of nodes:", total, pages, buckets)
		return
	}

	// Remove testkey10 which should remove a bucket

	res, _ := page.Put([]byte("testkey10"), nil)
	if res != "test10" {
		t.Error("Unexpected put result", res)
		return
	}

	if cc := countChildren(page); cc != 1 {
		t.Error("Unexpected number of children:", cc)
		return
	}

	total, pages, buckets = countNodes(sm)
	if total != 5 || pages != 4 || buckets != 1 {
		t.Error("Unexpected number of nodes:", total, pages, buckets)
		return
	}

	res, _ = page.Remove([]byte("testkey9"))
	if res != "test9" {
		t.Error("Unexpected remove result", res)
		return
	}

	_, err := page.Remove([]byte("testkey8"))
	if err != nil {
		t.Error(err)
		return
	}

	page.Remove([]byte("testkey7"))
	page.Remove([]byte("testkey6"))
	page.Remove([]byte("testkey5"))
	page.Remove([]byte("testkey4"))
	page.Remove([]byte("testkey3"))
	page.Remove([]byte("testkey2"))

	// Test error output when printing the tree

	sm.AccessMap[3] = storage.AccessCacheAndFetchError

	if out := htree.String(); out != "HTree: testsm (1)\n"+
		"HashPage 1 (depth: 0)\n"+
		"  Hash 000000B9 (loc: 3)\n"+
		"Slot not found (testsm - Location:3)\n" {
		t.Error("Unexpected tree representation:", out)
		return
	}

	delete(sm.AccessMap, 3)

	// Check that the tree has the expected values

	if out := htree.String(); out != "HTree: testsm (1)\n"+
		"HashPage 1 (depth: 0)\n"+
		"  Hash 000000B9 (loc: 3)\n"+
		"  HashPage 3 (depth: 1)\n"+
		"    Hash 000000B0 (loc: 5)\n"+
		"    HashPage 5 (depth: 2)\n"+
		"      Hash 00000069 (loc: 7)\n"+
		"      HashPage 7 (depth: 3)\n"+
		"        Hash 000000AD (loc: 8)\n"+
		"        HashBucket (1 element, depth: 4)\n"+
		"        [116 101 115 116 107 101 121 49] - test1\n"+
		"        [] - <nil>\n"+
		"        [] - <nil>\n"+
		"        [] - <nil>\n"+
		"        [] - <nil>\n"+
		"        [] - <nil>\n"+
		"        [] - <nil>\n"+
		"        [] - <nil>\n"+
		"        [] - <nil>\n" {
		t.Error("Unexpected tree representation:", out)
		return
	}

	res, err = page.Remove([]byte("testkey1"))
	if err != nil {
		t.Error(err)
		return
	}
	if out := htree.String(); out != "HTree: testsm (1)\n"+
		"HashPage 1 (depth: 0)\n" {
		t.Error("Unexpected tree representation:", out)
		return
	}

	// Error cases

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

	sm.AccessMap[16] = storage.AccessCacheAndFetchError

	if _, err := page.Remove([]byte("testkey1")); err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected remove result", res)
		return
	}

	delete(sm.AccessMap, 16)

	sm.AccessMap[1] = storage.AccessUpdateError

	if _, err := page.Remove([]byte("testkey10")); err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected remove result", res)
		return
	}

	delete(sm.AccessMap, 1)

	page.Remove([]byte("testkey1"))
	page.Remove([]byte("testkey8"))
	page.Remove([]byte("testkey7"))
	page.Remove([]byte("testkey6"))
	page.Remove([]byte("testkey5"))
	page.Remove([]byte("testkey4"))
	page.Remove([]byte("testkey3"))
	page.Remove([]byte("testkey2"))

	sm.AccessMap[1] = storage.AccessUpdateError

	if _, err := page.Remove([]byte("testkey9")); err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected remove result", res)
		//return
	}

	delete(sm.AccessMap, 1)

	// We arrive to the same result even with all the errors
	// the tree is kept consistent - however there are now
	// orphaned data bits in the storage

	if out := htree.String(); out != "HTree: testsm (1)\n"+
		"HashPage 1 (depth: 0)\n" {
		t.Error("Unexpected tree representation:", out)
		return
	}
}

func testMaxDepthExceededPanic(t *testing.T, page *htreePage, sm *storage.MemoryStorageManager) {
	fn := &htreeNode{}
	fn.sm = sm
	node, _ := fn.fetchNode(8)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Having a bucket on the lowest level which reports that it is too full should cause a panic.")
		}
		node.Depth = 4
	}()

	// The full bucket is on the lowest level - by setting the depth to 1 it
	// will report that it is too full even if it is located on the lowest level

	node.Depth = 1

	page.Put([]byte("testkey0"), "test9")
}

// Returns node numbers: total, pages, buckets
//
func countNodes(sm *storage.MemoryStorageManager) (int, int, int) {
	var total, pages, buckets int
	var i uint64

	fn := &htreeNode{}
	fn.sm = sm

	for i = 0; i < sm.LocCount; i++ {
		node, _ := fn.fetchNode(uint64(i))
		if node != nil {
			if node.Children != nil {
				total++
				pages++
			} else {
				total++
				buckets++
			}
		}
	}

	return total, pages, buckets
}

func countChildren(n *htreePage) int {
	if len(n.Children) == 0 {
		return 0
	}

	var count int

	for _, child := range n.Children {

		if child != 0 {
			count++
		}
	}

	return count
}

func TestHash(t *testing.T) {
	htp := &htreePage{&htreeNode{}}
	test := []byte{0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5}

	hash, _ := MurMurHashData(test, 0, len(test)-1, 42)
	hashstr := fmt.Sprintf("%08X", hash)

	if hashstr != "ACFFB715" {
		t.Error("Unexpected hash output:", hashstr)
		return
	}

	// Now check that on each level of the tree we get the right bit sequence

	htp.Depth = 3
	res := htp.hashKey(test)
	resstr := fmt.Sprintf("%08X", res)

	if resstr != "00000015" {
		t.Error("Unexpected hash output:", resstr)
		return
	}

	htp.Depth = 2
	res = htp.hashKey(test)
	resstr = fmt.Sprintf("%08X", res)

	if resstr != "000000B7" {
		t.Error("Unexpected hash output:", resstr)
		return
	}

	htp.Depth = 1
	res = htp.hashKey(test)
	resstr = fmt.Sprintf("%08X", res)

	if resstr != "000000FF" {
		t.Error("Unexpected hash output:", resstr)
		return
	}

	htp.Depth = 0
	res = htp.hashKey(test)
	resstr = fmt.Sprintf("%08X", res)

	if resstr != "000000AC" {
		t.Error("Unexpected hash output:", resstr)
		return
	}
}
