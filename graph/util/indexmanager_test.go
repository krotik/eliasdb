/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package util

import (
	"fmt"
	"strings"
	"testing"

	"devt.de/krotik/common/bitutil"
	"devt.de/krotik/eliasdb/hash"
	"devt.de/krotik/eliasdb/storage"
)

func TestIndexManager(t *testing.T) {
	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := hash.NewHTree(sm)

	im := NewIndexManager(htree)

	obj1 := make(map[string]string)
	obj1["aaa"] = "DDD voldaaa ddd"
	obj1["bbb"] = "vbbb"

	im.Index("testkey", obj1)

	if res, _ := im.LookupWord("aaa", "ddd"); fmt.Sprint(res) != "map[testkey:[1 3]]" {
		t.Error("Unexpected lookup result:", res)
		return
	}

	CaseSensitiveWordIndex = true

	sm = storage.NewMemoryStorageManager("testsm")
	htree, _ = hash.NewHTree(sm)

	im = NewIndexManager(htree)

	im.Index("testkey", obj1)

	if res, _ := im.LookupWord("aaa", "ddd"); fmt.Sprint(res) != "map[testkey:[3]]" {
		t.Error("Unexpected lookup result:", res)
		return
	}

	if res, _ := im.Count("aaa", "ddd"); res != 1 {
		t.Error("Unexpected count result:", res)
		return
	}

	CaseSensitiveWordIndex = false

	sm = storage.NewMemoryStorageManager("testsm")
	htree, _ = hash.NewHTree(sm)

	im = NewIndexManager(htree)

	im.Index("testkey", obj1)

	if res, _ := im.LookupWord("aaa", "ddd"); fmt.Sprint(res) != "map[testkey:[1 3]]" {
		t.Error("Unexpected lookup result:", res)
		return
	}

	for i := 0; i < 6; i++ {
		sm.AccessMap[uint64(i)] = storage.AccessCacheAndFetchError
	}

	if _, err := im.LookupWord("aaa", "ddd"); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected lookup result:", err)
		return
	}

	if err := im.Index("testkey", obj1); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected index result:", err)
		return
	}

	if err := im.Deindex("testkey", obj1); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected index result:", err)
		return
	}

	for i := 0; i < 6; i++ {
		delete(sm.AccessMap, uint64(i))
	}

	obj2 := make(map[string]string)
	obj2["aaa"] = "ddd vnewaaa"
	obj2["ccc"] = "ccc"

	if err := im.Reindex("testkey", obj2, obj1); err != nil {
		t.Error(err)
		return
	}

	if res := countChildren(htree); res != 5 {
		t.Error("Unexpected number of children:", res)
		return
	}

	if res, err := im.LookupWord("aaa", "DdD"); fmt.Sprint(res) != "map[testkey:[1]]" {
		t.Error("Unexpected lookup result:", res, err)
		return
	}

	if res, _ := im.Count("aaa", "dDD"); res != 1 {
		t.Error("Unexpected count result:", res)
		return
	}

	if res, _ := im.Count("aab", "dDD"); res != 0 {
		t.Error("Unexpected count result:", res)
		return
	}

	if err := im.Deindex("testkey", obj2); err != nil {
		t.Error(err)
		return
	}

	if res := countChildren(htree); res != 0 {
		t.Error("Unexpected number of children:", res)
		return
	}
}

func TestPhraseSearch(t *testing.T) {

	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := hash.NewHTree(sm)

	im := NewIndexManager(htree)

	obj1 := make(map[string]string)
	obj1["aaa"] = "the Is xxx The grass Is grEEn zzz"

	im.Index("testkey", obj1)

	obj2 := make(map[string]string)
	obj2["aaa"] = "the Is xxx The grass Is graY zzz"

	im.Index("testkey2", obj2)

	obj3 := make(map[string]string)
	obj3["aaa"] = "green grass is green zzz"

	im.Index("testkey3", obj3)

	obj4 := make(map[string]string)
	obj4["aaa"] = "green grass is so green zzz"
	obj4["bbb"] = "test"

	im.Index("testkey4", obj4)

	res, err := im.LookupPhrase("aaa", "grass is green")

	if fmt.Sprint(res) != "[testkey testkey3]" || err != nil {
		t.Error("Unexpected lookup result:", res, err)
	}

	res, err = im.LookupPhrase("aaa", "zzz")

	if fmt.Sprint(res) != "[testkey testkey2 testkey3 testkey4]" || err != nil {
		t.Error("Unexpected lookup result:", res, err)
	}

	res, err = im.LookupPhrase("bbb", "test")

	if fmt.Sprint(res) != "[testkey4]" || err != nil {
		t.Error("Unexpected lookup result:", res, err)
	}

	// Test empty return cases

	res, err = im.LookupPhrase("bbb", "")
	if res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
	}

	res, err = im.LookupPhrase("bbb", "b")
	if res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
	}

	for i := 0; i < 10; i++ {
		sm.AccessMap[uint64(i)] = storage.AccessCacheAndFetchError
	}

	res, err = im.LookupPhrase("aaa", "grass is green")
	if res != nil || !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected result:", res, err)
	}

	if _, err := im.Count("aaa", "grass"); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected count result:", err)
		return
	}

	for i := 0; i < 10; i++ {
		delete(sm.AccessMap, uint64(i))
	}
}

func TestUpdateIndex(t *testing.T) {

	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := hash.NewHTree(sm)

	im := NewIndexManager(htree)

	obj1 := make(map[string]string)
	obj1["aaa"] = "vnewaaa ddd"
	obj1["bbb"] = "vbbb"

	obj2 := make(map[string]string)
	obj2["aaa"] = "ddd voldaaa"
	obj2["ccc"] = "ccc"

	// Insert into the index

	im.updateIndex("123", obj2, nil)

	// Check that the entries exist

	entry, _ := htree.Get([]byte(PrefixAttrWord + "aaa" + "ddd"))
	pos := entry.(*indexEntry).WordPos["123"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[1]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	entry, _ = htree.Get([]byte(PrefixAttrWord + "aaa" + "voldaaa"))
	pos = entry.(*indexEntry).WordPos["123"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[2]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	entry, _ = htree.Get([]byte(PrefixAttrWord + "ccc" + "ccc"))
	pos = entry.(*indexEntry).WordPos["123"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[1]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	if res := countChildren(htree); res != 5 {
		t.Error("Unexpected number of children:", res)
		return
	}

	// Update the index

	im.updateIndex("123", obj1, obj2)

	if res := countChildren(htree); res != 5 {
		t.Error("Unexpected number of children:", res)
		return
	}

	// Check that the entries exist

	entry, _ = htree.Get([]byte(PrefixAttrWord + "aaa" + "ddd"))
	pos = entry.(*indexEntry).WordPos["123"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[2]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	entry, _ = htree.Get([]byte(PrefixAttrWord + "aaa" + "voldaaa"))

	if entry != nil {
		t.Error("Unexpected result")
		return
	}

	entry, _ = htree.Get([]byte(PrefixAttrWord + "aaa" + "vnewaaa"))
	pos = entry.(*indexEntry).WordPos["123"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[1]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	entry, _ = htree.Get([]byte(PrefixAttrWord + "bbb" + "vbbb"))
	pos = entry.(*indexEntry).WordPos["123"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[1]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	// Delete from the index

	im.updateIndex("123", nil, obj1)

	if res := countChildren(htree); res != 0 {
		t.Error("Unexpected number of children:", res)
		return
	}
}

func countChildren(tree *hash.HTree) int {

	var count int

	it := hash.NewHTreeIterator(tree)

	for it.HasNext() {
		it.Next()
		count++
	}

	return count
}

func TestAddRemoveIndexHashEntry(t *testing.T) {
	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := hash.NewHTree(sm)

	im := NewIndexManager(htree)

	oldsetting := CaseSensitiveWordIndex

	CaseSensitiveWordIndex = false

	im.addIndexHashEntry("mykey2", "myattr", "testvalue")
	im.addIndexHashEntry("mykey3", "myattr", "testvalue")

	sm.AccessMap[2] = storage.AccessCacheAndFetchError

	if err := im.addIndexHashEntry("mykey2", "myattr", "testvalue"); err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error(err)
		return
	}
	if err := im.removeIndexHashEntry("mykey2", "myattr", "testvalue"); err.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error(err)
		return
	}

	if _, err := im.LookupValue("myattr", "testvalue"); !strings.Contains(err.Error(), "Slot not found") {
		t.Error(err)
		return
	}

	delete(sm.AccessMap, 2)

	// Test lookup

	res, _ := im.LookupValue("myattr", "testvalue")
	if fmt.Sprint(res) != "[mykey2 mykey3]" {
		t.Error("Unexpected lookup value result:", res)
		return
	}

	res, _ = im.LookupValue("myattr", "testvalue2")
	if fmt.Sprint(res) != "[]" {
		t.Error("Unexpected lookup value result:", res)
		return
	}

	im.removeIndexHashEntry("mykey2", "myattr", "testvalue")
	im.removeIndexHashEntry("mykey3", "myattr", "testvalue")

	if count := countChildren(htree); count != 0 {
		t.Error("Unexpected child count:", count)
		return
	}

	// Check handling of non-existent entries

	if res := im.removeIndexHashEntry("mykey4", "myattr", "testvalue"); res != nil {
		t.Error("Unexpected result:", res)
		return
	}

	// Test case sensitive case

	CaseSensitiveWordIndex = true

	im.addIndexHashEntry("mykey2", "myattr", "testValue")
	im.addIndexHashEntry("mykey3", "myattr", "testValue")

	if count := countChildren(htree); count != 1 {
		t.Error("Unexpected child count:", count)
		return
	}

	res, _ = im.LookupValue("myattr", "testvalue")
	if fmt.Sprint(res) != "[]" {
		t.Error("Unexpected lookup value result:", res)
		return
	}

	res, _ = im.LookupValue("myattr", "testValue")
	if fmt.Sprint(res) != "[mykey2 mykey3]" {
		t.Error("Unexpected lookup value result:", res)
		return
	}

	im.removeIndexHashEntry("mykey2", "myattr", "testvalue")
	im.removeIndexHashEntry("mykey3", "myattr", "testvalue")

	if count := countChildren(htree); count != 1 {
		t.Error("Unexpected child count:", count)
		return
	}

	im.removeIndexHashEntry("mykey2", "myattr", "testValue")
	im.removeIndexHashEntry("mykey3", "myattr", "testValue")

	if count := countChildren(htree); count != 0 {
		t.Error("Unexpected child count:", count)
		return
	}

	CaseSensitiveWordIndex = oldsetting
}

func TestAddRemoveIndexEntry(t *testing.T) {

	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := hash.NewHTree(sm)

	im := NewIndexManager(htree)

	im.addIndexEntry("mykey", "myattr", "myword", []uint64{1, 5, 7})
	im.addIndexEntry("mykey2", "myattr", "myword", []uint64{10, 12, 80})

	entry, _ := htree.Get([]byte(PrefixAttrWord + "myattr" + "myword"))

	pos := entry.(*indexEntry).WordPos["mykey"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[1 5 7]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	pos = entry.(*indexEntry).WordPos["mykey2"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[10 12 80]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	testAddIndexPanic(t, im)

	sm.AccessMap[2] = storage.AccessCacheAndFetchError

	if res := im.addIndexEntry("mykey2", "myattr", "myword", []uint64{10, 12, 80}); res.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected result:", res)
		return
	}

	if res := im.removeIndexEntry("mykey2", "myattr", "myword", []uint64{10, 12, 80}); res.(*storage.ManagerError).Type != storage.ErrSlotNotFound {
		t.Error("Unexpected result:", res)
		return
	}

	delete(sm.AccessMap, 2)

	im.removeIndexEntry("mykey", "myattr", "myword", []uint64{1, 5, 7})

	entry, _ = htree.Get([]byte(PrefixAttrWord + "myattr" + "myword"))

	if res := len(entry.(*indexEntry).WordPos); res != 1 {
		t.Error("Unexpected length:", res)
		return
	}

	pos = entry.(*indexEntry).WordPos["mykey2"]

	if fmt.Sprint(bitutil.UnpackList(pos)) != "[10 12 80]" {
		t.Error("Unexpected result:", fmt.Sprint(bitutil.UnpackList(pos)))
		return
	}

	if im.removeIndexEntry("mykey3", "myattr", "myword", []uint64{10, 12, 80}) != nil {
		t.Error("Unexpected result")
		return
	}

	entry, _ = htree.Get([]byte(PrefixAttrWord + "myattr" + "myword"))

	if len(entry.(*indexEntry).WordPos) != 1 {
		t.Error("Unexpected length")
		return
	}

	if im.removeIndexEntry("mykey2", "myattr2", "myword", []uint64{10, 12, 80}) != nil {
		t.Error("Unexpected result")
		return
	}

	entry, _ = htree.Get([]byte(PrefixAttrWord + "myattr" + "myword"))

	if len(entry.(*indexEntry).WordPos) != 1 {
		t.Error("Unexpected length")
		return
	}

	if posres := fmt.Sprint(bitutil.UnpackList(entry.(*indexEntry).WordPos["mykey2"])); posres != "[10 12 80]" {
		t.Error("Unexpected pos list:", posres)
		return
	}

	// Test adding in non standard order

	if im.addIndexEntry("mykey2", "myattr", "myword", []uint64{45, 13}) != nil {
		t.Error("Unexpected result")
		return
	}

	if posres := fmt.Sprint(bitutil.UnpackList(entry.(*indexEntry).WordPos["mykey2"])); posres != "[10 12 13 45 80]" {
		t.Error("Unexpected pos list:", posres)
		return
	}

	// Test removal in non standard order

	if im.removeIndexEntry("mykey2", "myattr", "myword", []uint64{10, 80, 45, 13}) != nil {
		t.Error("Unexpected result")
		return
	}

	if posres := fmt.Sprint(bitutil.UnpackList(entry.(*indexEntry).WordPos["mykey2"])); posres != "[12]" {
		t.Error("Unexpected pos list:", posres)
		return
	}
}

func TestIndexManagerHashErrors(t *testing.T) {
	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := hash.NewHTree(sm)

	im := NewIndexManager(htree)

	obj1 := make(map[string]string)
	obj1["aaa"] = "DDD voldaaa ddd"

	obj2 := make(map[string]string)
	obj2["aaa"] = "DDDe voldaaa ddd"

	im.Index("testkey", obj1)

	sm.AccessMap[4] = storage.AccessCacheAndFetchError
	if err := im.Index("testkey", obj1); err == nil {
		t.Error("Error expected")
		return
	}
	if err := im.Reindex("testkey", obj1, obj1); err == nil {
		t.Error("Error expected")
		return
	}
	if err := im.Deindex("testkey", obj1); err == nil {
		t.Error("Error expected")
		return
	}
	sm.AccessMap[5] = storage.AccessUpdateError
	if err := im.Reindex("testkey", obj1, obj2); err == nil {
		t.Error("Error expected")
		return
	}
	delete(sm.AccessMap, 4)
}

func testAddIndexPanic(t *testing.T, in *IndexManager) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Adding empty pos list did not cause a panic.")
		}
	}()

	in.addIndexEntry("", "", "", []uint64{})
}

func TestExtractWords(t *testing.T) {

	oldsetting := CaseSensitiveWordIndex

	CaseSensitiveWordIndex = false

	ws := extractWords("   aaa BBB  ;,   ccc...aaa ddd-bbb xxxx aaaa    test1\n" +
		"test2 xxxx bbb")

	if res := ws.String(); res != "WordSet:\n"+
		"    aaa [1 4]\n"+
		"    aaaa [8]\n"+
		"    bbb [2 6 12]\n"+
		"    ccc [3]\n"+
		"    ddd [5]\n"+
		"    test1 [9]\n"+
		"    test2 [10]\n"+
		"    xxxx [7 11]\n" {
		t.Error("Unexpected WordSet string result:", res)
	}

	CaseSensitiveWordIndex = true

	ws = extractWords("   aaa BBB     ccc aaa ddd bbb xxxx aaaa    test1\n" +
		"test2 xxxx bbb")

	if res := ws.String(); res != "WordSet:\n"+
		"    BBB [2]\n"+
		"    aaa [1 4]\n"+
		"    aaaa [8]\n"+
		"    bbb [6 12]\n"+
		"    ccc [3]\n"+
		"    ddd [5]\n"+
		"    test1 [9]\n"+
		"    test2 [10]\n"+
		"    xxxx [7 11]\n" {
		t.Error("Unexpected WordSet string result:", res)
	}

	CaseSensitiveWordIndex = oldsetting
}

func TestWordSet(t *testing.T) {
	ws := newWordSet(1)
	ws2 := newWordSet(1)
	ws3 := newWordSet(1)

	ws.Add("aaa", 1)
	ws.Add("bbb", 2)
	ws2.Add("bbb", 3)
	ws3.Add("ccc", 4)

	if !ws.Has("bbb") || ws.Has("ccc") {
		t.Error("Unexpected has result")
		return
	}

	if res := ws.String(); res != "WordSet:\n"+
		"    aaa [1]\n"+
		"    bbb [2]\n" {
		t.Error("Unexpected string result:", res)
		return
	}

	ws.AddAll(ws2)

	if res := ws2.String(); res != "WordSet:\n"+
		"    bbb [3]\n" {
		t.Error("Unexpected string result:", res)
		return
	}
	if res := ws.String(); res != "WordSet:\n"+
		"    aaa [1]\n"+
		"    bbb [2 3]\n" {
		t.Error("Unexpected string result:", res)
		return
	}

	ws.Add("bbb", 1)

	ws.AddAll(ws3)

	if res := ws.String(); res != "WordSet:\n"+
		"    aaa [1]\n"+
		"    bbb [1 2 3]\n"+
		"    ccc [4]\n" {
		t.Error("Unexpected string result:", res)
		return
	}

	ws.Remove("aaa", 1)
	ws.Remove("bbb", 2)
	ws.Remove("bbb", 1)

	if res := ws.String(); res != "WordSet:\n"+
		"    bbb [3]\n"+
		"    ccc [4]\n" {
		t.Error("Unexpected string result:", res)
		return
	}

	ws.Add("bbb", 4)
	ws.Add("bbb", 8)
	ws.Add("bbb", 10)

	ws4 := newWordSet(1)
	ws4.Add("bbb", 2)
	ws4.Add("bbb", 3)
	ws4.Add("bbb", 4)
	ws4.Add("bbb", 8)

	if res := ws.String(); res != "WordSet:\n"+
		"    bbb [3 4 8 10]\n"+
		"    ccc [4]\n" {
		t.Error("Unexpected string result:", res)
		return
	}

	ws.RemoveAll(ws4)

	if res := ws.String(); res != "WordSet:\n"+
		"    bbb [10]\n"+
		"    ccc [4]\n" {
		t.Error("Unexpected string result:", res)
		return
	}

	if res := fmt.Sprint(ws4.Pos("bbb")); res != "[2 3 4 8]" {
		t.Error("Unexpected pos result:", res)
		return
	}
	if res := ws4.Pos("abb"); res != nil {
		t.Error("Unexpected pos result:", res)
		return
	}

	// Test double entries

	ws.Add("ccc", 3)
	ws.Add("ccc", 5)
	ws.Add("ccc", 4)

	if res := ws.String(); res != "WordSet:\n"+
		"    bbb [10]\n"+
		"    ccc [3 4 5]\n" {
		t.Error("Unexpected string result:", res)
		return
	}
}

func TestRemoveDuplicates(t *testing.T) {

	if res := fmt.Sprint(removeDuplicates([]uint64{1, 2, 2, 3})); res != "[1 2 3]" {
		t.Error("Unexpected remove duplicates result:", res)
		return
	}

	if res := fmt.Sprint(removeDuplicates([]uint64{})); res != "[]" {
		t.Error("Unexpected remove duplicates result:", res)
		return
	}
}

func TestIndexManagerString(t *testing.T) {
	sm := storage.NewMemoryStorageManager("testsm")
	htree, _ := hash.NewHTree(sm)

	im := NewIndexManager(htree)

	obj1 := make(map[string]string)
	obj1["aaa"] = "bbb"

	im.Index("testkey", obj1)

	if res := im.String(); res != "IndexManager: 1\n"+
		"    1\"aaa\\b\\xf8\\xe0&\\fdA\\x85\\x10\\xce\\xfb+\\x06\\xee\\xe5\\xcd\" map[testkey:[]]\n"+
		"    1\"aaabbb\" map[testkey:[1]]\n" && res != "IndexManager: 1\n"+
		"    1\"aaabbb\" map[testkey:[1]]\n"+
		"    2\"aaa\\b\\xf8\\xe0&\\fdA\\x85\\x10\\xce\\xfb+\\x06\\xee\\xe5\\xcd\" map[testkey:[]]\n" {
		t.Error("Unexpected string output:", res)
		return
	}
}
