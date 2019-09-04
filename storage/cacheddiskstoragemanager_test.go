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
	"testing"

	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/slotting/pageview"
)

type cachetestobj struct {
	Val1 int
	Val2 string
}

func TestCachedDiskStorageManager(t *testing.T) {

	dsm := NewDiskStorageManager(DBDIR+"/ctest1", false, false, true, true)

	if dsm.Name() != "DiskStorageFile:"+DBDIR+"/ctest1" {
		t.Error("Unexpected name for DiskStorageManager:", dsm.Name())
		return
	}

	cdsm := NewCachedDiskStorageManager(dsm, 10)

	// Test the getter and setter fields

	if cdsm.Name() != dsm.Name() {
		t.Error("Unexpected result asking for the name")
		return
	}

	if cdsm.Root(RootIDVersion) != dsm.Root(RootIDVersion) || dsm.Root(RootIDVersion) == 0 {
		t.Error("Unexpected result asking for the version")
		return
	}

	cdsm.SetRoot(5, 20)
	if cdsm.Root(5) != 20 || dsm.Root(5) != 20 {
		t.Error("Unexpected result asking for a root")
		return
	}

	// Test the insert which is not cached

	testObj1 := &cachetestobj{1, "This is a test"}
	testObj2 := &cachetestobj{1, "This is a 7e57"}

	loc, err := cdsm.Insert(testObj1)
	if err != nil {
		t.Error(err)
		return
	}

	// Test getting non-existent entry from cache

	if _, err := cdsm.FetchCached(loc + 1); err != ErrNotInCache {
		t.Error("Unexpected FetchCached result:", err)
		return
	}

	// Make sure rollback has no effect if the transactions are disabled

	cdsm.Rollback()

	checkLocation(t, loc, 1, pageview.OffsetTransData)

	if _, ok := cdsm.cache[loc]; !ok {
		t.Error("Cache entry should not be empty")
		return
	}

	var ret1, ret2 cachetestobj
	err = cdsm.Fetch(loc, &ret1)

	if ret1.Val2 != "This is a test" || err != nil {
		t.Error("Unexpected fetch result:", ret1, err)
		return
	}

	// Check cache entry

	if e, ok := cdsm.cache[loc]; !ok || ret1 == e.object ||
		e.object.(*cachetestobj).Val2 != "This is a test" {

		t.Error("Update should store a copy")
		return
	}

	err = dsm.Fetch(loc, &ret2)
	if ret2.Val2 != "This is a test" || err != nil {
		t.Error("Unexpected fetch result:", ret2, err)
		return
	}

	// Test the update which is cached

	if err := cdsm.Update(loc, testObj2); err != nil {
		t.Error(err)
		return
	}

	if testObj2 != cdsm.cache[loc].object {
		t.Error("Cache should contain object which was given by update")
		return
	}

	// Check that we have a cache entry

	err = cdsm.Fetch(loc, &ret2)
	if ret2.Val2 != "This is a 7e57" || err != nil {
		t.Error("Unexpected fetch result:", ret2, err)
		return
	}

	// Test writing a different type

	if err := cdsm.Update(loc, "bla"); err != nil {
		t.Error(err)
		return
	}

	var ret3 string

	err = dsm.Fetch(loc, &ret3)
	if ret3 != "bla" || err != nil {
		t.Error("Unexpected fetch result:", ret3, err)
		return
	}

	// Run update on something which is unknown

	loc, err = cdsm.Insert("test66")
	if err != nil {
		t.Error(err)
		return
	}

	// Here the update should create the cache entry

	err = cdsm.Update(loc, "test77")
	if err != nil {
		t.Error(err)
		return
	}

	var obj interface{}

	obj, _ = cdsm.FetchCached(loc)
	if obj.(string) != "test77" {
		t.Error("Unexpected FetchCached result:", obj)
		return
	}

	err = dsm.Fetch(loc, &ret3)
	if ret3 != "test77" || err != nil {
		t.Error("Unexpected fetch result:", ret3, err)
		return
	}

	loc, err = dsm.Insert("test88")
	if err != nil {
		t.Error(err)
		return
	}

	record, err := dsm.physicalSlotsSf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	err = cdsm.Fetch(loc, &ret3)
	if err != file.ErrAlreadyInUse {
		t.Error("Unexpected fetch result:", ret3, err)
		return
	}

	if err := cdsm.Update(loc, "test99"); err != file.ErrAlreadyInUse {
		t.Error("Unexpected update result:", err)
		return
	}

	if err := cdsm.Flush(); err != nil && err.Error() != "Records are still in-use (storagemanagertest/ctest1.db - Records 1)" {
		t.Error("Unexpected flush result:", err)
		return
	}

	dsm.physicalSlotsSf.ReleaseInUse(record)

	// Flush should now succeed

	if err := cdsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	if err = cdsm.Close(); err != nil {
		t.Error(err)
	}
}

func TestCachedDiskStorageManagerTransactions(t *testing.T) {

	dsm := NewDiskStorageManager(DBDIR+"/ctest2", false, false, false, true)

	if dsm.Name() != "DiskStorageFile:"+DBDIR+"/ctest2" {
		t.Error("Unexpected name for DiskStorageManager:", dsm.Name())
		return
	}

	cdsm := NewCachedDiskStorageManager(dsm, 10)

	loc, err := cdsm.Insert("test1")
	if err != nil {
		t.Error(err)
		return
	}

	if err := cdsm.Rollback(); err != nil {
		t.Error(err)
		return
	}

	if _, ok := cdsm.cache[loc]; ok {
		t.Error("Cache entry should be empty")
		return
	}

	var ret string
	if err := cdsm.Fetch(loc, &ret); err != ErrSlotNotFound ||
		err.Error() != "Slot not found (ByteDiskStorageFile:storagemanagertest/ctest2 - Location:1 18)" {

		t.Error("Unexpected fetch result:", err)
		return
	}

	// Put it back

	loc2, err := cdsm.Insert("test1")
	if err != nil {
		t.Error(err)
		return
	}

	if loc != loc2 {
		t.Error("Unexpected insert position:", loc, loc2)
		return
	}

	// Check free error

	record, err := dsm.physicalSlotsSf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	if err := cdsm.Free(loc2); err != file.ErrAlreadyInUse {
		t.Error("Unexpected free result:", err)
		return
	}

	dsm.physicalSlotsSf.ReleaseInUse(record)

	// Check that nothing was lost

	if err := cdsm.Fetch(loc2, &ret); err != nil || ret != "test1" {
		t.Error("Unexpected fetch result:", ret, err)
		return
	}

	if err := cdsm.Free(loc2); err != nil {
		t.Error(err)
		return
	}

	if err := cdsm.Fetch(loc2, &ret); err != ErrSlotNotFound {
		t.Error("Unexpected fetch result:", err)
		return
	}

	if err = cdsm.Close(); err != nil {
		t.Error(err)
	}
}

func TestCachedDiskStorageManagerCacheManagement(t *testing.T) {

	var ret string

	dsm := NewDiskStorageManager(DBDIR+"/ctest3", false, false, true, true)

	cdsm := NewCachedDiskStorageManager(dsm, 3)

	// Event though the cache is empty make sure we can still retrieve empty entries

	entry := cdsm.removeOldestFromCache()
	if entry == nil {
		t.Error("Unexpected removeOldestFromCache result:", entry)
		return
	}

	// Insert values

	loc1, _ := cdsm.Insert("test1")
	loc2, _ := cdsm.Insert("test2")
	loc3, _ := cdsm.Insert("test3")
	loc4, _ := cdsm.Insert("test4")

	// Load all entries into the cache

	cdsm.Fetch(loc1, &ret)
	cdsm.Fetch(loc2, &ret)
	cdsm.Fetch(loc3, &ret)

	// Make sure all cache entries are there

	if _, ok := cdsm.cache[loc1]; !ok {
		t.Error("Cache entry should be available")
		return
	}

	if _, ok := cdsm.cache[loc2]; !ok {
		t.Error("Cache entry should be available")
		return
	}

	if _, ok := cdsm.cache[loc3]; !ok {
		t.Error("Cache entry should be available")
		return
	}

	// Now fetch one more and see that the oldest entry gets removed

	cdsm.Fetch(loc4, &ret)

	if _, ok := cdsm.cache[loc1]; ok {
		t.Error("Cache entry should not be available")
		return
	}

	// Check that the last accessed entry is on the last position in the list

	if cdsm.lastentry.location != loc4 {
		t.Error("Unexpected last entry:", cdsm.firstentry.location)
		return
	}

	cdsm.Fetch(loc2, &ret)

	if cdsm.lastentry.location != loc2 {
		t.Error("Unexpected last entry:", cdsm.firstentry.location)
		return
	}

	if cdsm.firstentry.location != loc3 {
		t.Error("Unexpected first entry:", cdsm.firstentry.location)
		return
	}

	record, err := dsm.physicalSlotsSf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	// Check we get an error when attempting to update the physical record
	// because we want to remove an entry from the list

	cdsm.Update(loc3, "test9")
	cdsm.Update(loc2, "test9")
	cdsm.Update(loc4, "test9")

	if cdsm.firstentry.location != loc3 {
		t.Error("Unexpected first entry:", cdsm.firstentry.location)
		return
	}

	dsm.physicalSlotsSf.ReleaseInUse(record)

	entry = cdsm.removeOldestFromCache()
	if entry.location != loc3 {
		t.Error("Unexpected removeOldestFromCache result:", entry, err)
		return
	}

	if err = cdsm.Close(); err != nil {
		t.Error(err)
	}
}
