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
)

func TestHashBucket(t *testing.T) {
	tree := &HTree{}

	// Create a top level bucket

	treebucket := newHTreeBucket(tree, 1)

	if treebucket.Size() != 0 {
		t.Error("Newly created bucket should be empty")
		return
	}
	if !treebucket.HasRoom() {
		t.Error("Newly created bucket should have room")
		return
	}
	if treebucket.IsLeaf() {
		t.Error("Newly created top level bucket should not be a leaf")
		return
	}

	// Test nil operations and operations on an empty bucket

	if res := treebucket.Remove([]byte{1, 2, 3}); res != nil {
		t.Error("Unexpected remove result:", res)
		return
	}

	if res := treebucket.Put(nil, "tester"); res != nil {
		t.Error("Unexpected add result:", res)
		return
	}

	if res := treebucket.Get(nil); res != nil {
		t.Error("Unexpected add result:", res)
		return
	}

	if res := treebucket.Get([]byte{1, 2, 3}); res != nil {
		t.Error("Unexpected add result:", res)
		return
	}

	// Simple insert case

	treebucket.Put([]byte{1, 2, 3}, "test1")
	treebucket.Put([]byte{1, 2, 4}, "test2")
	treebucket.Put([]byte{1, 2, 5}, "test3")

	if len(treebucket.Keys) != 8 || len(treebucket.Values) != 8 {
		t.Error("Unexpected bucket content:", treebucket.String())
		return
	}

	if fmt.Sprint(treebucket.Keys) != "[[1 2 3] [1 2 4] [1 2 5] [] [] [] [] []]" {
		t.Error("Unexpected keys content:", treebucket.Keys)
		return
	}

	if treebucket.Size() != 3 {
		t.Error("Unexpected size:", treebucket.Size())
		return
	}

	// Remove case

	if res := treebucket.Remove([]byte{9, 9, 9}); res != nil {
		t.Error("Unexpected remove result:", res)
		return
	}

	if res := treebucket.Remove([]byte{1, 2, 3}); res != "test1" {
		t.Error("Unexpected remove result:", res)
		return
	}

	if fmt.Sprint(treebucket.Keys) != "[[1 2 5] [1 2 4] [] [] [] [] [] []]" {
		t.Error("Unexpected keys content:", treebucket.Keys)
		return
	}

	if treebucket.Size() != 2 {
		t.Error("Unexpected size:", treebucket.Size())
		return
	}

	// Insert again (overwrite)

	treebucket.Put([]byte{1, 1, 1}, "test4")

	if fmt.Sprint(treebucket.Keys) != "[[1 2 5] [1 2 4] [1 1 1] [] [] [] [] []]" {
		t.Error("Unexpected keys content:", treebucket.Keys)
		return
	}
	if fmt.Sprint(treebucket.Values) != "[test3 test2 test4 <nil> <nil> <nil> <nil> <nil>]" {
		t.Error("Unexpected VALUES content:", treebucket.Values)
		return
	}

	// Update case

	treebucket.Put([]byte{1, 1, 1}, "test5")

	if fmt.Sprint(treebucket.Keys) != "[[1 2 5] [1 2 4] [1 1 1] [] [] [] [] []]" {
		t.Error("Unexpected keys content:", treebucket.Keys)
		return
	}
	if fmt.Sprint(treebucket.Values) != "[test3 test2 test5 <nil> <nil> <nil> <nil> <nil>]" {
		t.Error("Unexpected VALUES content:", treebucket.Values)
		return
	}

	// Get / Exists case

	if treebucket.Exists([]byte{1, 1, 2}) {
		t.Error("Unexpected exists result")
		return
	}

	if treebucket.Exists(nil) {
		t.Error("Unexpected exists result")
		return
	}

	if !treebucket.Exists([]byte{1, 1, 1}) {
		t.Error("Unexpected exists result")
		return
	}

	if res := treebucket.Get([]byte{1, 1, 1}); res != "test5" {
		t.Error("Unexpected get result:", res)
		return
	}

	if res := treebucket.Get([]byte{1, 1, 2}); res != nil {
		t.Error("Unexpected get result:", res)
		return
	}

	// Fill up the bucket

	treebucket.Put([]byte{1, 3, 1}, "test1")
	treebucket.Put([]byte{1, 3, 2}, "test2")
	treebucket.Put([]byte{1, 3, 3}, "test3")
	treebucket.Put([]byte{1, 3, 4}, "test4")
	treebucket.Put([]byte{1, 3, 5}, "test5")

	if treebucket.HasRoom() {
		t.Error("Full bucket should have no more room")
		return
	}

	testOverflowPanic(t, treebucket)

	// Check functionality on leaf buckets

	treebucket.Depth = 4

	if !treebucket.IsLeaf() {
		t.Error("Bucket should be now a leaf bucket")
		return
	}

	// Test bucket expansion

	treebucket.Put([]byte{1, 3, 6}, "test6")

	if fmt.Sprint(treebucket.Keys) != "[[1 2 5] [1 2 4] [1 1 1] [1 3 1] [1 3 2] [1 3 3] [1 3 4] [1 3 5] [1 3 6]]" {
		t.Error("Unexpected keys content:", treebucket.Keys)
		return
	}
	if fmt.Sprint(treebucket.Values) != "[test3 test2 test5 test1 test2 test3 test4 test5 test6]" {
		t.Error("Unexpected VALUES content:", treebucket.Values)
		return
	}

	// Test string output

	if res := fmt.Sprint(treebucket); res != "        HashBucket (9 elements, depth: 4)\n"+
		"        [1 2 5] - test3\n"+
		"        [1 2 4] - test2\n"+
		"        [1 1 1] - test5\n"+
		"        [1 3 1] - test1\n"+
		"        [1 3 2] - test2\n"+
		"        [1 3 3] - test3\n"+
		"        [1 3 4] - test4\n"+
		"        [1 3 5] - test5\n"+
		"        [1 3 6] - test6\n" {
		t.Error("Unexpected string output:", res)
	}
}

func testOverflowPanic(t *testing.T, treebucket *htreeBucket) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Inserting into a full non-leaf bucket did not cause a panic.")
		}
	}()

	treebucket.Put([]byte{1, 3, 6}, "test6")
}
