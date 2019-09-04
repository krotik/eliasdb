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
	"fmt"

	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging"
	"devt.de/krotik/eliasdb/storage/paging/view"
	"devt.de/krotik/eliasdb/storage/slotting/pageview"
	"devt.de/krotik/eliasdb/storage/util"
)

/*
FreePhysicalSlotManager data structure
*/
type FreePhysicalSlotManager struct {
	storagefile     *file.StorageFile        // StorageFile which is wrapped
	pager           *paging.PagedStorageFile // Pager for StorageFile
	onlyAppend      bool                     // Flag for append-only mode
	lastMaxSlotSize int                      // Last max slot size
	slots           []uint64                 // List of free slots
	sizes           []uint32                 // List of free slot sizes
}

/*
NewFreePhysicalSlotManager creates a new object to manage free physical slots.
*/
func NewFreePhysicalSlotManager(psf *paging.PagedStorageFile, onlyAppend bool) *FreePhysicalSlotManager {
	return &FreePhysicalSlotManager{psf.StorageFile(), psf, onlyAppend, 0,
		make([]uint64, 0), make([]uint32, 0)}
}

/*
Get searches for a free location with the given size.
*/
func (fpsm *FreePhysicalSlotManager) Get(size uint32) (uint64, error) {

	// Return always nothing found if we are in only-append mode

	if fpsm.onlyAppend {
		return 0, nil
	}

	// Return nothing if all previous found pages were too small

	if fpsm.lastMaxSlotSize != 0 && int(size) > fpsm.lastMaxSlotSize {
		return 0, nil
	}

	cursor := paging.NewPageCursor(fpsm.pager, view.TypeFreePhysicalSlotPage, 0)

	// No need for error checking on cursor next since all pages will be opened
	// via Get calls in the loop.

	page, _ := cursor.Next()
	for page != 0 {

		record, err := fpsm.storagefile.Get(page)

		if err != nil {

			// Reset the lastMaxSlotSize since we didn't visit all
			// FreePhysicalSlotPages

			fpsm.lastMaxSlotSize = 0

			return 0, err
		}

		fpsp := pageview.NewFreePhysicalSlotPage(record)

		slot := fpsp.FindSlot(size)

		// If a slot was found (Important: a slot can be >= 0)

		if slot >= 0 {

			// Return a found slot and free the free page if necessary

			fpsm.lastMaxSlotSize = 0
			loc := fpsp.SlotInfoLocation(uint16(slot))

			// Release slot

			fpsp.ReleaseSlotInfo(uint16(slot))

			if fpsp.FreeSlotCount() == 0 {

				// Free the page if no free slot is stored

				fpsm.storagefile.ReleaseInUseID(page, false)
				fpsm.pager.FreePage(page)

			} else {

				fpsm.storagefile.ReleaseInUseID(page, false)
			}

			return loc, nil
		}

		if fpsm.lastMaxSlotSize < -slot {
			fpsm.lastMaxSlotSize = -slot
		}

		fpsm.storagefile.ReleaseInUseID(page, false)

		page, _ = cursor.Next()
	}

	return 0, nil
}

/*
Add adds a slotinfo to the free slot set.
*/
func (fpsm *FreePhysicalSlotManager) Add(loc uint64, size uint32) {
	if size > 0 {
		fpsm.slots = append(fpsm.slots, loc)
		fpsm.sizes = append(fpsm.sizes, size)
	}
}

/*
Flush writes all added slotinfos to FreePhysicalSlotPages.
*/
func (fpsm *FreePhysicalSlotManager) Flush() error {

	cursor := paging.NewPageCursor(fpsm.pager, view.TypeFreePhysicalSlotPage, 0)
	index := 0

	// Go through all free physical row ID pages

	// No need for error checking on cursor next since all pages will be opened
	// via Get calls in the loop.

	page, _ := cursor.Next()
	for page != 0 {

		// Need to declare err here otherwise index becomes a local for
		// the "for" block

		var err error

		index, err = fpsm.doFlush(page, index)
		if err != nil {
			return err
		}

		if index >= len(fpsm.slots) {
			break
		}

		page, _ = cursor.Next()
	}

	// Allocate new free physical slot pages if all present ones are full
	// and we have still slots to process

	for index < len(fpsm.slots) {

		allocPage, err := fpsm.pager.AllocatePage(view.TypeFreePhysicalSlotPage)
		if err != nil {
			return err
		}

		index, err = fpsm.doFlush(allocPage, index)
		if err != nil {

			// Try to free the allocated page if there was an error
			// ignore any error of the FreePage call

			fpsm.pager.FreePage(allocPage)

			return err
		}
	}

	// Clear lists after all slots information have been written

	fpsm.slots = make([]uint64, 0)
	fpsm.sizes = make([]uint32, 0)

	return nil
}

/*
doFlush writes all added slotinfos to a FreePhysicalSlotPage. Stop if the page is full.
*/
func (fpsm *FreePhysicalSlotManager) doFlush(page uint64, index int) (int, error) {
	r, err := fpsm.storagefile.Get(page)

	if err != nil {
		return index, err
	}

	fpsp := pageview.NewFreePhysicalSlotPage(r)

	// Iterate all page slots (stop if the page has no more available slots
	// or we reached the end of the page)

	slot := fpsp.FirstFreeSlotInfo()

	for ; slot != -1 && index < len(fpsm.slots); index++ {

		loc := fpsm.slots[index]
		size := fpsm.sizes[index]

		if size > 0 {
			offset := fpsp.AllocateSlotInfo(uint16(slot))
			fpsp.SetSlotInfo(offset, util.LocationRecord(loc), util.LocationOffset(loc))
			fpsp.SetFreeSlotSize(offset, size)

			slot = fpsp.FirstFreeSlotInfo()
		}
	}

	fpsm.storagefile.ReleaseInUseID(page, true)

	return index, nil
}

/*
String returns a string representation of this FreePhysicalSlotManager.
*/
func (fpsm *FreePhysicalSlotManager) String() string {
	return fmt.Sprintf("FreePhysicalSlotManager: %v (onlyAppend:%v lastMaxSlotSize:%v)\nIds  :%v\nSizes:%v",
		fpsm.storagefile.Name(), fpsm.onlyAppend, fpsm.lastMaxSlotSize, fpsm.slots, fpsm.sizes)
}
