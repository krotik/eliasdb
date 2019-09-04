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
LogicalSlotManager data structure
*/
type LogicalSlotManager struct {
	storagefile     *file.StorageFile        // StorageFile which is wrapped
	pager           *paging.PagedStorageFile // Pager for StorageFile
	freeManager     *FreeLogicalSlotManager  // Manager for free slots
	recordSize      uint32                   // Size of records
	elementsPerPage uint16                   // Available elements per page
}

/*
NewLogicalSlotManager creates a new object to manage logical slots. This
factory function requires two PagedStorageFiles the first will hold the actual
logical slots, the second is used to manage free logical slots.
*/
func NewLogicalSlotManager(lsf *paging.PagedStorageFile,
	flsf *paging.PagedStorageFile) *LogicalSlotManager {

	sf := lsf.StorageFile()

	freeManager := NewFreeLogicalSlotManager(flsf)
	recordSize := sf.RecordSize()

	return &LogicalSlotManager{sf, lsf, freeManager,
		recordSize, uint16((recordSize - pageview.OffsetTransData) / util.LocationSize)}
}

/*
ElementsPerPage returns the available elements per page.
*/
func (lsm *LogicalSlotManager) ElementsPerPage() uint16 {
	return lsm.elementsPerPage
}

/*
Insert inserts a given physical slot info and returns a logical slot for it.
*/
func (lsm *LogicalSlotManager) Insert(location uint64) (uint64, error) {

	// Try to get a free slot from the FreeLogicalSlotManager

	slot, err := lsm.freeManager.Get()
	if err != nil {
		return 0, err
	}

	if slot == 0 {

		// Allocate a new page and give all its rows to the free manager

		allocPage, err := lsm.pager.AllocatePage(view.TypeTranslationPage)
		if err != nil {
			return 0, err
		}

		offset := uint16(pageview.OffsetTransData)

		var i uint16
		for i = 0; i < lsm.elementsPerPage; i++ {
			lsm.freeManager.Add(util.PackLocation(allocPage, offset))
			offset += util.LocationSize
		}

		err = lsm.Flush()
		if err != nil {

			// Try to clean up if something goes wrong

			// Make the freeManager forget that he received anything

			lsm.freeManager.slots = make([]uint64, 0)

			// Free the allocated page again

			lsm.pager.FreePage(allocPage)

			return 0, err
		}

		// Now get the first slot from the newly allocated page - no need for
		// error checking since we just flushed the page and all is well

		slot, _ = lsm.freeManager.Get()
	}

	// Write physical slot data to translation page

	return slot, lsm.Update(slot, location)
}

/*
ForceInsert inserts a given physical slot info at a given logical slot.
*/
func (lsm *LogicalSlotManager) ForceInsert(logicalSlot uint64, location uint64) error {
	page := lsm.pager.Last(view.TypeTranslationPage)
	targetPage := util.LocationRecord(logicalSlot)

	// If the target page hasn't been allocated yet then create new pages
	// until the target page is available and we can force insert into the
	// requested slot

	for page < targetPage {
		var err error

		page, err = lsm.pager.AllocatePage(view.TypeTranslationPage)
		if err != nil {
			return err
		}
	}

	slot, err := lsm.Fetch(logicalSlot)
	if err != nil {
		return err
	}
	if slot != 0 {
		panic(fmt.Sprintf("Cannot force insert into slot %v because it already exists",
			logicalSlot))
	}

	return lsm.Update(logicalSlot, location)
}

/*
Update updates a given logical slot with a physical slot info.
*/
func (lsm *LogicalSlotManager) Update(logicalSlot uint64, location uint64) error {
	recordID := util.LocationRecord(logicalSlot)

	record, err := lsm.storagefile.Get(recordID)
	if err != nil {
		return err
	}

	page := pageview.NewTransPage(record)

	page.SetSlotInfo(util.LocationOffset(logicalSlot), util.LocationRecord(location),
		util.LocationOffset(location))

	lsm.storagefile.ReleaseInUseID(recordID, true)

	return nil
}

/*
Free frees a given logical slot. The given slot is given to the FreeLogicalSlotManager.
*/
func (lsm *LogicalSlotManager) Free(logicalSlot uint64) error {
	recordID := util.LocationRecord(logicalSlot)

	record, err := lsm.storagefile.Get(recordID)
	if err != nil {
		return err
	}

	page := pageview.NewTransPage(record)

	page.SetSlotInfo(util.LocationOffset(logicalSlot), util.LocationRecord(0),
		util.LocationOffset(0))

	return lsm.storagefile.ReleaseInUseID(recordID, true)
}

/*
Fetch looks up a physical slot using a given logical slot.
*/
func (lsm *LogicalSlotManager) Fetch(logicalSlot uint64) (uint64, error) {

	recordID := util.LocationRecord(logicalSlot)
	offset := util.LocationOffset(logicalSlot)

	if lastPage := lsm.pager.Last(view.TypeTranslationPage); lastPage < recordID {

		// Return if the requested page doesn't exist yet

		return 0, nil
	}

	record, err := lsm.storagefile.Get(recordID)
	if err != nil {
		return 0, err
	}

	page := pageview.NewTransPage(record)

	slot := util.PackLocation(page.SlotInfoRecord(offset), page.SlotInfoOffset(offset))

	lsm.storagefile.ReleaseInUseID(recordID, false)

	return slot, nil
}

/*
Flush writes all pending changes.
*/
func (lsm *LogicalSlotManager) Flush() error {
	return lsm.freeManager.Flush()
}
