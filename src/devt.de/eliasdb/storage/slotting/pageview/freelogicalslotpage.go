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
	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging/view"
	"devt.de/eliasdb/storage/util"
)

// OFFSET_COUNT / OFFSET_DATA declared in freephysicalslotpage

/*
FreeLogicalSlotPage data structure
*/
type FreeLogicalSlotPage struct {
	*SlotInfoPage
	maxSlots               uint16 // Max number of slots
	prevFoundFreeSlot      uint16 // Previous found free slot
	prevFoundAllocatedSlot uint16 // Previous allocated slot
}

/*
NewFreeLogicalSlotPage creates a new page which can manage free slots.
*/
func NewFreeLogicalSlotPage(record *file.Record) *FreeLogicalSlotPage {
	checkFreeLogicalSlotPageMagic(record)

	maxSlots := (len(record.Data()) - OffsetData) / util.LocationSize

	return &FreeLogicalSlotPage{NewSlotInfoPage(record), uint16(maxSlots), 0, 0}
}

/*
checkFreeLogicalSlotPageMagic checks if the magic number at the beginning of
the wrapped record is valid.
*/
func checkFreeLogicalSlotPageMagic(record *file.Record) bool {
	magic := record.ReadInt16(0)

	if magic == view.ViewPageHeader+view.TypeFreeLogicalSlotPage {
		return true
	}
	panic("Unexpected header found in FreeLogicalSlotPage")
}

/*
MaxSlots returns the maximum number of slots which can be allocated.
*/
func (flsp *FreeLogicalSlotPage) MaxSlots() uint16 {
	return flsp.maxSlots
}

/*
FreeSlotCount returns the number of free slots on this page.
*/
func (flsp *FreeLogicalSlotPage) FreeSlotCount() uint16 {
	return flsp.Record.ReadUInt16(OffsetCount)
}

/*
SlotInfoLocation returns contents of a stored slotinfo as a location. Lookup is via a
given slotinfo id.
*/
func (flsp *FreeLogicalSlotPage) SlotInfoLocation(slotinfo uint16) uint64 {
	offset := flsp.slotinfoToOffset(slotinfo)
	return util.PackLocation(flsp.SlotInfoRecord(offset), flsp.SlotInfoOffset(offset))
}

/*
AllocateSlotInfo allocates a place for a slotinfo and returns the offset for it.
*/
func (flsp *FreeLogicalSlotPage) AllocateSlotInfo(slotinfo uint16) uint16 {
	offset := flsp.slotinfoToOffset(slotinfo)

	// Set slotinfo to initial values

	flsp.SetSlotInfo(offset, 1, 1)

	// Increase counter for allocated slotinfos

	flsp.Record.WriteUInt16(OffsetCount, flsp.FreeSlotCount()+1)

	// Update prevFoundAllocatedSlot if necessary

	if slotinfo < flsp.prevFoundAllocatedSlot {
		flsp.prevFoundAllocatedSlot = slotinfo
	}

	return offset
}

/*
ReleaseSlotInfo releases a place for a slotinfo and return its offset.
*/
func (flsp *FreeLogicalSlotPage) ReleaseSlotInfo(slotinfo uint16) uint16 {
	offset := flsp.slotinfoToOffset(slotinfo)

	// Set slotinfo to empty values

	flsp.SetSlotInfo(offset, 0, 0)

	// Decrease counter for allocated slotinfos

	flsp.Record.WriteUInt16(OffsetCount, flsp.FreeSlotCount()-1)

	// Update prevFoundFreeSlot if necessary

	if slotinfo < flsp.prevFoundFreeSlot {
		flsp.prevFoundFreeSlot = slotinfo
	}

	return offset
}

/*
FirstFreeSlotInfo returns the id for the first available slotinfo or -1 if
nothing is available.
*/
func (flsp *FreeLogicalSlotPage) FirstFreeSlotInfo() int {
	for flsp.prevFoundFreeSlot < flsp.maxSlots {
		if !flsp.isAllocatedSlot(flsp.prevFoundFreeSlot) {
			return int(flsp.prevFoundFreeSlot)
		}
		flsp.prevFoundFreeSlot++
	}
	return -1
}

/*
FirstAllocatedSlotInfo returns the id for the first allocated slotinfo or -1 if
nothing is allocated.
*/
func (flsp *FreeLogicalSlotPage) FirstAllocatedSlotInfo() int {
	for flsp.prevFoundAllocatedSlot < flsp.maxSlots {
		if flsp.isAllocatedSlot(flsp.prevFoundAllocatedSlot) {
			return int(flsp.prevFoundAllocatedSlot)
		}
		flsp.prevFoundAllocatedSlot++
	}
	return -1
}

/*
isAllocatedSlot checks if a given slotinfo is allocated.
*/
func (flsp *FreeLogicalSlotPage) isAllocatedSlot(slotinfo uint16) bool {
	offset := flsp.slotinfoToOffset(slotinfo)
	return flsp.SlotInfoRecord(offset) != 0 || flsp.SlotInfoOffset(offset) != 0
}

/*
slotinfoToOffset converts a slotinfo number into an offset on the record.
*/
func (flsp *FreeLogicalSlotPage) slotinfoToOffset(slotinfo uint16) uint16 {
	return OffsetData + slotinfo*util.LocationSize
}
