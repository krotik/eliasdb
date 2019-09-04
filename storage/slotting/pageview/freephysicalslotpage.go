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
	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging/view"
	"devt.de/krotik/eliasdb/storage/util"
)

/*
OffsetCount is the number of free slots which are stored on this page
*/
const OffsetCount = view.OffsetData

/*
OffsetData is the offset for slot information
*/
const OffsetData = OffsetCount + file.SizeShort

/*
SlotInfoSize is the size of a single free slot info
*/
const SlotInfoSize = util.LocationSize + util.SizeInfoSize

/*
OptimalWasteMargin is the max amount of allowed allocation waste. When searching a slot
on this page we should strife to find a slot which doesn't waste more than
OptimalWasteMargin bytes
*/
const OptimalWasteMargin = 128

/*
FreePhysicalSlotPage data structure
*/
type FreePhysicalSlotPage struct {
	*SlotInfoPage
	maxSlots           uint16   // Max number of slots
	maxAcceptableWaste uint32   // Max acceptable waste for a slot allocation
	sizeCache          []uint32 // Cache for slot sizes
}

/*
NewFreePhysicalSlotPage creates a new page which can manage free slots.
*/
func NewFreePhysicalSlotPage(record *file.Record) *FreePhysicalSlotPage {
	checkFreePhysicalSlotPageMagic(record)

	maxSlots := (len(record.Data()) - OffsetData) / SlotInfoSize
	maxAcceptableWaste := len(record.Data()) / 4

	return &FreePhysicalSlotPage{NewSlotInfoPage(record), uint16(maxSlots),
		uint32(maxAcceptableWaste), make([]uint32, maxSlots, maxSlots)}
}

/*
checkFreePhysicalSlotPageMagic checks if the magic number at the beginning of
the wrapped record is valid.
*/
func checkFreePhysicalSlotPageMagic(record *file.Record) bool {
	magic := record.ReadInt16(0)

	if magic == view.ViewPageHeader+view.TypeFreePhysicalSlotPage {
		return true
	}
	panic("Unexpected header found in FreePhysicalSlotPage")
}

/*
MaxSlots returns the maximum number of slots which can be allocated.
*/
func (fpsp *FreePhysicalSlotPage) MaxSlots() uint16 {
	return fpsp.maxSlots
}

/*
FreeSlotCount returns the number of free slots on this page.
*/
func (fpsp *FreePhysicalSlotPage) FreeSlotCount() uint16 {
	return fpsp.Record.ReadUInt16(OffsetCount)
}

/*
SlotInfoLocation returns contents of a stored slotinfo as a location. Lookup is via a
given slotinfo id.
*/
func (fpsp *FreePhysicalSlotPage) SlotInfoLocation(slotinfo uint16) uint64 {
	offset := fpsp.slotinfoToOffset(slotinfo)
	return util.PackLocation(fpsp.SlotInfoRecord(offset), fpsp.SlotInfoOffset(offset))
}

/*
FreeSlotSize returns the size of a free slot. Lookup is via offset.
*/
func (fpsp *FreePhysicalSlotPage) FreeSlotSize(offset uint16) uint32 {
	slotinfo := fpsp.offsetToSlotinfo(offset)
	if fpsp.sizeCache[slotinfo] == 0 {
		fpsp.sizeCache[slotinfo] = fpsp.Record.ReadUInt32(int(offset + util.LocationSize))
	}
	return fpsp.sizeCache[slotinfo]
}

/*
SetFreeSlotSize sets the size of a free slot. Lookup is via offset.
*/
func (fpsp *FreePhysicalSlotPage) SetFreeSlotSize(offset uint16, size uint32) {
	slotinfo := fpsp.offsetToSlotinfo(offset)
	fpsp.sizeCache[slotinfo] = size
	fpsp.Record.WriteUInt32(int(offset+util.LocationSize), size)
}

/*
AllocateSlotInfo allocates a place for a slotinfo and returns the offset for it.
*/
func (fpsp *FreePhysicalSlotPage) AllocateSlotInfo(slotinfo uint16) uint16 {
	offset := fpsp.slotinfoToOffset(slotinfo)

	// Set slotinfo to initial values

	fpsp.SetFreeSlotSize(offset, 1)
	fpsp.SetSlotInfo(offset, 1, 1)

	// Increase counter for allocated slotinfos

	fpsp.Record.WriteUInt16(OffsetCount, fpsp.FreeSlotCount()+1)

	return offset
}

/*
ReleaseSlotInfo releases a place for a slotinfo and return its offset.
*/
func (fpsp *FreePhysicalSlotPage) ReleaseSlotInfo(slotinfo uint16) uint16 {
	offset := fpsp.slotinfoToOffset(slotinfo)

	// Set slotinfo to empty values

	fpsp.SetFreeSlotSize(offset, 0)
	fpsp.SetSlotInfo(offset, 0, 0)

	// Decrease counter for allocated slotinfos

	fpsp.Record.WriteUInt16(OffsetCount, fpsp.FreeSlotCount()-1)

	return offset
}

/*
FirstFreeSlotInfo returns the id for the first available slotinfo for allocation
or -1 if nothing is available.
*/
func (fpsp *FreePhysicalSlotPage) FirstFreeSlotInfo() int {
	var i uint16
	for i = 0; i < fpsp.maxSlots; i++ {
		if !fpsp.isAllocatedSlot(i) {
			return int(i)
		}
	}
	return -1
}

/*
FindSlot finds a slot which is suitable for a given amount of data but which is also not
too big to avoid wasting space.
*/
func (fpsp *FreePhysicalSlotPage) FindSlot(minSize uint32) int {

	var i uint16

	bestSlot := -1
	bestSlotWaste := fpsp.maxAcceptableWaste + 1

	var maxSize uint32

	for i = 0; i < fpsp.maxSlots; i++ {

		slotinfoOffset := fpsp.slotinfoToOffset(i)

		slotinfoSize := fpsp.FreeSlotSize(slotinfoOffset)

		if slotinfoSize > maxSize {
			maxSize = slotinfoSize
		}

		// Calculate the wasted space

		waste := slotinfoSize - minSize

		// Test if the block would fit

		if waste >= 0 {

			if waste < OptimalWasteMargin {

				// In the ideal case we can minimise the produced waste

				return int(i)

			} else if bestSlotWaste > waste {

				// Too much for optimal waste margin but may still be OK if
				// we don't find anything better

				bestSlot = int(i)
				bestSlotWaste = waste
			}
		}
	}

	if bestSlot != -1 {

		// We found a block but its waste was above the optimal waste margin
		// check if it is still acceptable

		// Note: It must be below the MAX_AVAILABLE_SIZE_DIFFERENCE as a row
		// stores the current size as the difference to the available size.
		// This difference must fit in an unsigned short.

		if bestSlotWaste < fpsp.maxAcceptableWaste &&
			bestSlotWaste < util.MaxAvailableSizeDifference {

			return bestSlot
		}
	}

	return -int(maxSize)
}

/*
isAllocatedSlot checks if a given slotinfo is allocated.
*/
func (fpsp *FreePhysicalSlotPage) isAllocatedSlot(slotinfo uint16) bool {
	offset := fpsp.slotinfoToOffset(slotinfo)
	return fpsp.FreeSlotSize(offset) != 0
}

/*
slotinfoToOffset converts a slotinfo number into an offset on the record.
*/
func (fpsp *FreePhysicalSlotPage) slotinfoToOffset(slotinfo uint16) uint16 {
	return OffsetData + slotinfo*SlotInfoSize
}

/*
offsetToSlotinfo converts an offset into a slotinfo number.
*/
func (fpsp *FreePhysicalSlotPage) offsetToSlotinfo(offset uint16) uint16 {
	return (offset - OffsetData) / SlotInfoSize
}
