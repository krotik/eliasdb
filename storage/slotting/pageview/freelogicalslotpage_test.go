/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package pageview

import (
	"testing"

	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging/view"
	"devt.de/krotik/eliasdb/storage/util"
)

func TestFreeLogicalSlotPage(t *testing.T) {
	r := file.NewRecord(123, make([]byte, 44))

	testCheckFreeLogicalSlotPageMagicPanic(t, r)

	// Make sure the record has a correct magic

	view.NewPageView(r, view.TypeFreeLogicalSlotPage)

	flsp := NewFreeLogicalSlotPage(r)

	maxSlots := flsp.MaxSlots()

	if maxSlots != 3 {
		t.Error("Unexpected number of maxSlots:", maxSlots)
		return
	}

	slotinfoID := flsp.FirstFreeSlotInfo()

	if slotinfoID != 0 {
		t.Error("Unexpected first free slot:", slotinfoID)
		return
	}

	offset := flsp.AllocateSlotInfo(0)

	if !flsp.isAllocatedSlot(0) {
		t.Error("Slot 0 not allocated")
		return
	}

	flsp.SetSlotInfo(offset, 5, 0x22)

	if flsp.SlotInfoRecord(offset) != 5 {
		t.Error("Unexpected slotinfo record")
		return
	}

	if flsp.SlotInfoOffset(offset) != 0x22 {
		t.Error("Unexpected slotinfo offset")
		return
	}

	loc := flsp.SlotInfoLocation(0)

	if util.LocationRecord(loc) != 5 {
		t.Error("Unexpected slotinfo record")
		return
	}

	if util.LocationOffset(loc) != 0x22 {
		t.Error("Unexpected slotinfo offset")
		return
	}

	if !flsp.isAllocatedSlot(0) {
		t.Error("Slot 0 not allocated")
		return
	}

	if flsp.isAllocatedSlot(1) {
		t.Error("Slot 1 should not be allocated")
		return
	}

	if flsp.FirstFreeSlotInfo() != 1 {
		t.Error("Unexpected first free result", flsp.FirstFreeSlotInfo())
		return
	}

	flsp.AllocateSlotInfo(1)

	if fsi := flsp.FirstFreeSlotInfo(); fsi != 2 {
		t.Error("Unexpected first allocatable slot", fsi)
		return
	}

	flsp.AllocateSlotInfo(2)

	if flsp.FirstFreeSlotInfo() != -1 {
		t.Error("Unexpected first free result", flsp.FirstFreeSlotInfo())
		return
	}

	flsp.ReleaseSlotInfo(1)

	if flsp.FirstFreeSlotInfo() != 1 {
		t.Error("Unexpected first free result", flsp.FirstFreeSlotInfo())
		return
	}

	flsp.AllocateSlotInfo(1)

	if flsp.FirstFreeSlotInfo() != -1 {
		t.Error("Unexpected first free result", flsp.FirstFreeSlotInfo())
		return
	}

	flsp.AllocateSlotInfo(2)

	flsp.AllocateSlotInfo(1)
	flsp.ReleaseSlotInfo(0)

	if flsp.isAllocatedSlot(0) {
		t.Error("Slot 0 should no longer be allocated")
		return
	}

	if flsp.FirstAllocatedSlotInfo() != 1 {
		t.Error("Unexpected first allocated result", flsp.FirstFreeSlotInfo())
		return
	}

	if flsp.FirstFreeSlotInfo() != 0 {
		t.Error("Unexpected first free result", flsp.FirstFreeSlotInfo())
		return
	}

	if flsp.prevFoundAllocatedSlot != 1 {
		t.Error("Unexpected to previous found allocated slot:",
			flsp.prevFoundAllocatedSlot)
	}

	flsp.AllocateSlotInfo(0)

	if flsp.prevFoundAllocatedSlot != 0 {
		t.Error("Unexpected to previous found allocated slot:",
			flsp.prevFoundAllocatedSlot)
	}

	flsp.ReleaseSlotInfo(0)
	flsp.ReleaseSlotInfo(1)
	flsp.ReleaseSlotInfo(2)

	if flsp.FirstAllocatedSlotInfo() != -1 {
		t.Error("Unexpected first allocated result", flsp.FirstFreeSlotInfo())
		return
	}
}

func testCheckFreeLogicalSlotPageMagicPanic(t *testing.T, r *file.Record) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Checking magic should fail.")
		}
	}()

	checkFreeLogicalSlotPageMagic(r)
}
