/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package slotting

import (
	"testing"

	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging"
	"devt.de/eliasdb/storage/slotting/pageview"
	"devt.de/eliasdb/storage/util"
)

func TestLogicalSlotManager(t *testing.T) {
	sf, err := file.NewDefaultStorageFile(DBDIR+"/test8_data", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := paging.NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	fsf, err := file.NewDefaultStorageFile(DBDIR+"/test8_free", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	fpsf, err := paging.NewPagedStorageFile(fsf)
	if err != nil {
		t.Error(err)
		return
	}

	lsm := NewLogicalSlotManager(psf, fpsf)

	if lsm.ElementsPerPage() != 509 {
		t.Error("Unexpected elements per page:", lsm.ElementsPerPage())
		return
	}

	// Check return value when fetching from a logical slot which doesn't yet exist

	slotinfo, err := lsm.Fetch(util.PackLocation(1, pageview.OffsetTransData))
	if slotinfo != 0 || err != nil {
		t.Error("Unexpected fetch result:", slotinfo, err)
	}

	// Test insertion error

	record, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := lsm.Insert(util.PackLocation(10, 11)); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Test insertion error when allocating free slots

	record, err = fsf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := lsm.Insert(util.PackLocation(10, 11)); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	fsf.ReleaseInUse(record)

	// Insert a locations

	loc, err := lsm.Insert(util.PackLocation(10, 11))
	if err != nil {
		t.Error(err)
		return
	}

	checkLocation(t, loc, 1, pageview.OffsetTransData)

	// Test error checking when force inserting a location (page does not exist)

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	if err = lsm.ForceInsert(util.PackLocation(2, 2), util.PackLocation(12, 13)); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Force insert a location

	if err = lsm.ForceInsert(util.PackLocation(2, 2), util.PackLocation(12, 13)); err != nil {
		t.Error(err)
		return
	}

	// Test error checking when force inserting a location (this time the page exists)

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	if err = lsm.ForceInsert(util.PackLocation(2, 2), util.PackLocation(12, 13)); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	testForceInsertPanic(t, lsm)

	// Check that the physical slot infos have been stored in the logical slots

	_testLogicalSlot(t, sf, 1, pageview.OffsetTransData, 10, 11)
	_testLogicalSlot(t, sf, 2, 2, 12, 13)

	// Free a slot

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	if err = lsm.Free(util.PackLocation(2, 2)); err != file.ErrAlreadyInUse {
		t.Error("Unexpected free result:", err)
		return
	}

	sf.ReleaseInUse(record)

	if err = lsm.Free(util.PackLocation(2, 2)); err != nil {
		t.Error(err)
		return
	}

	// Check error when fetching from the free manager (the free manager has
	// nothing stored but should fail since it can't look on page 1)

	record, err = fsf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err = lsm.Insert(util.PackLocation(12, 13)); err != file.ErrAlreadyInUse {
		t.Error("Unexpected free result:", err)
		return
	}

	fsf.ReleaseInUse(record)

	// Check error when updating a logical slot

	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	if err = lsm.Update(util.PackLocation(1, 3), util.PackLocation(12, 13)); err != file.ErrAlreadyInUse {
		t.Error("Unexpected update result:", err)
		return
	}

	sf.ReleaseInUse(record)

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}

	if err := fpsf.Close(); err != nil {
		t.Error(err)
		return
	}
}

func testForceInsertPanic(t *testing.T, lsm *LogicalSlotManager) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Force inserting to an existing location did not cause a panic.")
		}
	}()

	lsm.ForceInsert(util.PackLocation(2, 2), util.PackLocation(12, 13))
}

func _testLogicalSlot(t *testing.T, sf *file.StorageFile,
	logicalRecord uint64, logicalOffset uint16,
	physicalRecord uint64, physicalOffset uint16) {

	record, err := sf.Get(logicalRecord)
	if err != nil {
		t.Error(err)
		return
	}

	slotinfo := record.ReadUInt64(int(logicalOffset))
	if slotinfo != util.PackLocation(physicalRecord, physicalOffset) {

		t.Error("Unexpected physical location was stored in logical slot:",
			logicalRecord, logicalOffset, " expected:", physicalRecord, physicalOffset,
			"got:", util.LocationRecord(slotinfo), util.LocationOffset(slotinfo))

	}

	sf.ReleaseInUse(record)
}
