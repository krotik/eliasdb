/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
Package slotting contains managers which deal with slots on pages.

FreeLogicalSlotManager

FreeLogicalSlotManager is a list manager for free logical slots. This manager
object is used by the LogicalSlotManager.

FreePhysicalSlotManager

FreePhysicalSlotManager is a list manager for free physical slots. This manager
object is used by the PhysicalSlotManager.

LogicalSlotManager

LogicalSlotManager is a list manager for logical slots. Logical slots are stored
on translation pages which store just pointers to physical slots.

PhysicalSlotManager

PhysicalSlotManager is a list manager for physical slots.
*/
package slotting

import (
	"fmt"

	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging"
	"devt.de/eliasdb/storage/paging/view"
	"devt.de/eliasdb/storage/slotting/pageview"
	"devt.de/eliasdb/storage/util"
)

/*
FreeLogicalSlotManager data structure
*/
type FreeLogicalSlotManager struct {
	storagefile *file.StorageFile        // StorageFile which is wrapped
	pager       *paging.PagedStorageFile // Pager for StorageFile
	slots       []uint64                 // List of free slots
}

/*
NewFreeLogicalSlotManager creates a new object to manage free logical slots.
*/
func NewFreeLogicalSlotManager(psf *paging.PagedStorageFile) *FreeLogicalSlotManager {
	return &FreeLogicalSlotManager{psf.StorageFile(), psf, make([]uint64, 0)}
}

/*
Get gets a free slot.
*/
func (flsm *FreeLogicalSlotManager) Get() (uint64, error) {

	// Try to get entry from the slots list

	if len(flsm.slots) > 0 {
		freeSlot := flsm.slots[len(flsm.slots)-1]
		flsm.slots = flsm.slots[:len(flsm.slots)-1]
		return freeSlot, nil
	}

	cursor := paging.NewPageCursor(flsm.pager, view.TypeFreeLogicalSlotPage, 0)

	// No need for error checking on cursor next since all pages will be opened
	// via Get calls in the loop.

	page, _ := cursor.Next()
	for page != 0 {

		record, err := flsm.storagefile.Get(page)

		if err != nil {
			return 0, err
		}

		flsp := pageview.NewFreeLogicalSlotPage(record)

		slot := flsp.FirstAllocatedSlotInfo()

		if slot != -1 {

			// Return a found slot and free the free page if necessary

			loc := flsp.SlotInfoLocation(uint16(slot))

			// Release the slot

			flsp.ReleaseSlotInfo(uint16(slot))

			if flsp.FreeSlotCount() == 0 {

				// Free the page if no free row id slot is left

				flsm.storagefile.ReleaseInUseID(page, false)

				flsm.pager.FreePage(page)

			} else {

				flsm.storagefile.ReleaseInUseID(page, true)
			}

			return loc, nil
		}

		flsm.storagefile.ReleaseInUseID(page, false)

		page, _ = cursor.Next()
	}

	return 0, nil
}

/*
Add adds a slot to the free slot set.
*/
func (flsm *FreeLogicalSlotManager) Add(loc uint64) {
	if loc == 0 {

		// The bit pattern for the 0 location is used to mark free slots

		panic("Illigal free logical slot pattern: 0x0")
	}

	flsm.slots = append(flsm.slots, loc)
}

/*
Flush writes all added slotinfos to FreeLogicalSlotPages.
*/
func (flsm *FreeLogicalSlotManager) Flush() error {
	cursor := paging.NewPageCursor(flsm.pager, view.TypeFreeLogicalSlotPage, 0)
	index := 0

	// Go through all free logical slot pages

	// No need for error checking on cursor next since all pages will be opened
	// via Get calls in the loop.

	page, _ := cursor.Next()

	for page != 0 {

		// Need to declare err here otherwise index becomes a local for
		// the "for" block

		var err error

		index, err = flsm.doFlush(page, index)
		if err != nil {
			return err
		}

		if index >= len(flsm.slots) {
			break
		}

		page, _ = cursor.Next()
	}

	// Allocate new free logical slot pages if all present ones are full
	// and we have still slots to process

	for index < len(flsm.slots) {

		allocPage, err := flsm.pager.AllocatePage(view.TypeFreeLogicalSlotPage)
		if err != nil {
			return err
		}

		index, err = flsm.doFlush(allocPage, index)
		if err != nil {

			// Try to free the allocated page if there was an error
			// ignore any error of the FreePage call

			flsm.pager.FreePage(allocPage)

			return err
		}
	}

	// Clear lists after all slots information have been written

	flsm.slots = make([]uint64, 0)

	return nil
}

/*
doFlush writes all added slotinfos to a FreeLogicalSlotPage. Stop if the page is full.
*/
func (flsm *FreeLogicalSlotManager) doFlush(page uint64, index int) (int, error) {
	r, err := flsm.storagefile.Get(page)

	if err != nil {
		return index, err
	}

	flsp := pageview.NewFreeLogicalSlotPage(r)

	// Iterate all page slots (stop if the page has no more available slots
	// or we reached the end of the page)

	slot := flsp.FirstFreeSlotInfo()

	for ; slot != -1 && index < len(flsm.slots); index++ {

		loc := flsm.slots[index]

		offset := flsp.AllocateSlotInfo(uint16(slot))

		flsp.SetSlotInfo(offset, util.LocationRecord(loc), util.LocationOffset(loc))

		slot = flsp.FirstFreeSlotInfo()
	}

	flsm.storagefile.ReleaseInUseID(page, true)

	return index, nil
}

/*
Returns a string representation of this FreeLogicalSlotManager.
*/
func (flsm *FreeLogicalSlotManager) String() string {
	return fmt.Sprintf("FreeLogicalSlotManager: %v\nIds  :%v\n",
		flsm.storagefile.Name(), flsm.slots)
}
