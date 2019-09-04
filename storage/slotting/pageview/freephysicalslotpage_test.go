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

func TestFreePhysicalSlotPage(t *testing.T) {
	r := file.NewRecord(123, make([]byte, 44))

	testCheckFreePhysicalSlotPageMagicPanic(t, r)

	// Make sure the record has a correct magic

	view.NewPageView(r, view.TypeFreePhysicalSlotPage)

	fpsp := NewFreePhysicalSlotPage(r)

	maxSlots := fpsp.MaxSlots()

	if maxSlots != 2 {
		t.Error("Unexpected number of maxSlots:", maxSlots)
		return
	}

	slotinfoID := fpsp.FirstFreeSlotInfo()

	if slotinfoID != 0 {
		t.Error("Unexpected first free slot:", slotinfoID)
		return
	}

	offset := fpsp.AllocateSlotInfo(0)

	if !fpsp.isAllocatedSlot(0) {
		t.Error("Slot 0 not allocated")
		return
	}

	fpsp.SetSlotInfo(offset, 5, 0x22)
	fpsp.SetFreeSlotSize(offset, 0x123)

	if fpsp.SlotInfoRecord(offset) != 5 {
		t.Error("Unexpected slotinfo record")
		return
	}

	if fpsp.SlotInfoOffset(offset) != 0x22 {
		t.Error("Unexpected slotinfo offset")
		return
	}

	loc := fpsp.SlotInfoLocation(0)

	if util.LocationRecord(loc) != 5 {
		t.Error("Unexpected slotinfo record")
		return
	}

	if util.LocationOffset(loc) != 0x22 {
		t.Error("Unexpected slotinfo offset")
		return
	}

	if fpsp.FreeSlotSize(offset) != 0x123 {
		t.Error("Unexpected slot size")
		return
	}

	if !fpsp.isAllocatedSlot(0) {
		t.Error("Slot 0 not allocated")
		return
	}

	if fpsp.isAllocatedSlot(1) {
		t.Error("Slot 1 should not be allocated")
		return
	}

	if fpsp.FirstFreeSlotInfo() != 1 {
		t.Error("Unexpected first free result", fpsp.FirstFreeSlotInfo())
		return
	}

	fpsp.AllocateSlotInfo(1)

	if fpsp.FirstFreeSlotInfo() != -1 {
		t.Error("Unexpected first free result", fpsp.FirstFreeSlotInfo())
		return
	}

	fpsp.ReleaseSlotInfo(1)
	fpsp.ReleaseSlotInfo(0)

	if fpsp.isAllocatedSlot(0) {
		t.Error("Slot 0 should no longer be allocated")
		return
	}

	if fpsp.FirstFreeSlotInfo() != 0 {
		t.Error("Unexpected first free result", fpsp.FirstFreeSlotInfo())
		return
	}
}

func TestFreePhysicalSlotPageAllocation(t *testing.T) {
	r := file.NewRecord(123, make([]byte, 4096))

	view.NewPageView(r, view.TypeFreePhysicalSlotPage)

	fpsp := NewFreePhysicalSlotPage(r)

	maxSlots := fpsp.MaxSlots()

	if maxSlots != 339 {
		t.Error("Unexpected number of maxSlots:", maxSlots)
		return
	}
	if fpsp.maxAcceptableWaste != 1024 {
		t.Error("Unexpected max accpectable waste:", fpsp.maxAcceptableWaste)
		return
	}

	// Allocate some free physical flats

	offset := fpsp.AllocateSlotInfo(3)
	fpsp.SetSlotInfo(offset, 0x22, 0x22)
	fpsp.SetFreeSlotSize(offset, 100)

	offset = fpsp.AllocateSlotInfo(5)
	fpsp.SetSlotInfo(offset, 0x22, 0x22)
	fpsp.SetFreeSlotSize(offset, 5024)

	offset = fpsp.AllocateSlotInfo(7)
	fpsp.SetSlotInfo(offset, 0x22, 0x22)
	fpsp.SetFreeSlotSize(offset, 2000)

	offset = fpsp.AllocateSlotInfo(9)
	fpsp.SetSlotInfo(offset, 0x22, 0x22)
	fpsp.SetFreeSlotSize(offset, 500)

	offset = fpsp.AllocateSlotInfo(19)
	fpsp.SetSlotInfo(offset, 0x22, 0x22)
	fpsp.SetFreeSlotSize(offset, 50000)

	if slot := fpsp.FindSlot(500); slot != 9 {
		t.Error("Unexpected found slot:", slot)
		return
	}

	if slot := fpsp.FindSlot(499); slot != 9 {
		t.Error("Unexpected found slot:", slot)
		return
	}

	// Test not found slots

	if slot := fpsp.FindSlot(4000); slot != -50000 {
		t.Error("Unexpected found slot:", slot)
		return
	}

	if slot := fpsp.FindSlot(4001); slot != 5 {
		t.Error("Unexpected found slot:", slot)
		return
	}
}

func testCheckFreePhysicalSlotPageMagicPanic(t *testing.T, r *file.Record) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Checking magic should fail.")
		}
	}()

	checkFreePhysicalSlotPageMagic(r)
}
