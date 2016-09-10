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
	"io"

	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging"
	"devt.de/eliasdb/storage/paging/view"
	"devt.de/eliasdb/storage/slotting/pageview"
	"devt.de/eliasdb/storage/util"
)

/*
AllocationRoundUpThreshold is used to decide if a slot size should be rounded up. If an
allocation would leave less than AllocationRoundUpThreshold + 1 left on the page then
the allocation size is rounded up to fit the page
*/
const AllocationRoundUpThreshold = 16

/*
PhysicalSlotManager data structure
*/
type PhysicalSlotManager struct {
	storagefile         *file.StorageFile        // StorageFile which is wrapped
	pager               *paging.PagedStorageFile // Pager for StorageFile
	freeManager         *FreePhysicalSlotManager // Manager for free slots
	recordSize          uint32                   // Size of records
	availableRecordSize uint32                   // Available space on records
}

/*
NewPhysicalSlotManager creates a new object to manage physical slots. This
factory function requires two PagedStorageFiles the first will hold the actual
physical slots, the second is used to manage free physical slots.
*/
func NewPhysicalSlotManager(psf *paging.PagedStorageFile,
	fpsf *paging.PagedStorageFile, onlyAppend bool) *PhysicalSlotManager {

	sf := psf.StorageFile()

	freeManager := NewFreePhysicalSlotManager(fpsf, onlyAppend)
	recordSize := sf.RecordSize()

	return &PhysicalSlotManager{sf, psf, freeManager,
		recordSize, recordSize - pageview.OffsetData}
}

/*
Insert inserts a new piece of data.
*/
func (psm *PhysicalSlotManager) Insert(data []byte, start uint32, length uint32) (uint64, error) {

	if length == 0 {
		panic("Cannot insert 0 bytes of data")
	}

	location, err := psm.allocate(length)
	if err != nil {
		return 0, err
	}

	err = psm.write(location, data, start, length)
	if err != nil {

		// Since the write operation failed declare the previous allocated space
		// as free

		psm.freeManager.Add(location, length)

		return 0, err
	}

	return location, nil
}

/*
Update updates the data in a slot.
*/
func (psm *PhysicalSlotManager) Update(location uint64, data []byte, start uint32, length uint32) (uint64, error) {

	record, err := psm.storagefile.Get(util.LocationRecord(location))

	if err != nil {
		return 0, err
	}

	offset := util.LocationOffset(location)

	availableSize := util.AvailableSize(record, int(offset))

	psm.storagefile.ReleaseInUse(record)

	if length > availableSize || availableSize-length > util.MaxAvailableSizeDifference {

		// Reallocate if the new data is too big for the old slot or if the
		// data is much smaller than the available space in the slot (i.e.
		// there would be a lot of waste)

		// Error handling for free call is done by the first Get call of
		// this function.

		psm.Free(location)

		location, err = psm.allocate(length)
		if err != nil {
			return 0, err
		}
	}

	err = psm.write(location, data, start, length)
	if err != nil {
		return 0, err
	}

	return location, nil
}

/*
Fetch fetches data from a specified location.
*/
func (psm *PhysicalSlotManager) Fetch(location uint64, writer io.Writer) error {

	cursor := paging.NewPageCursor(psm.pager, view.TypeDataPage, util.LocationRecord(location))

	record, err := psm.storagefile.Get(cursor.Current())
	if err != nil {
		return err
	}

	length := util.CurrentSize(record, int(util.LocationOffset(location)))
	if length == 0 {

		// Return at this point if there is nothing to read

		psm.storagefile.ReleaseInUseID(cursor.Current(), false)
		return nil
	}

	// Read now the bytes

	restSize := length
	recordOffset := uint32(util.LocationOffset(location) + util.SizeInfoSize)

	for restSize > 0 {

		// Calculate how much data should be read

		toCopy := psm.recordSize - uint32(recordOffset)

		if restSize < toCopy {

			// If the record can contain more than restSize just
			// read restSize

			toCopy = restSize
		}

		// Read the data

		writer.Write(record.Data()[recordOffset : recordOffset+toCopy])

		// Calculate the rest size and new offset

		restSize -= toCopy

		psm.storagefile.ReleaseInUseID(cursor.Current(), false)

		// Go to the next record

		if restSize > 0 {

			// Error handling is done by surrounding Get calls

			next, _ := cursor.Next()

			record, err = psm.storagefile.Get(next)
			if err != nil {
				return err
			}

			recordOffset = pageview.OffsetData
		}
	}

	return nil
}

/*
Free frees a given physical slot. The given slot is given to the FreePhysicalSlotManager.
*/
func (psm *PhysicalSlotManager) Free(location uint64) error {
	slotRecord := util.LocationRecord(location)
	slotOffset := int(util.LocationOffset(location))

	record, err := psm.storagefile.Get(slotRecord)
	if err != nil {
		return err
	}

	util.SetCurrentSize(record, slotOffset, 0)

	psm.storagefile.ReleaseInUseID(slotRecord, true)

	psm.freeManager.Add(location, util.AvailableSize(record, slotOffset))

	return nil
}

/*
Flush writes all pending changes.
*/
func (psm *PhysicalSlotManager) Flush() error {
	return psm.freeManager.Flush()
}

/*
write writes data to a location. Should an error occurs, then the already written data
is not cleaned up.
*/
func (psm *PhysicalSlotManager) write(location uint64, data []byte, start uint32, length uint32) error {

	cursor := paging.NewPageCursor(psm.pager, view.TypeDataPage, util.LocationRecord(location))

	record, err := psm.storagefile.Get(cursor.Current())
	if err != nil {
		return err
	}

	util.SetCurrentSize(record, int(util.LocationOffset(location)), length)
	if length == 0 {

		// Return at this point if there is nothing to write

		psm.storagefile.ReleaseInUseID(cursor.Current(), true)
		return nil
	}

	// Write now the bytes

	restSize := length
	dataOffset := start
	recordOffset := uint32(util.LocationOffset(location) + util.SizeInfoSize)

	for restSize > 0 {

		// Calculate how much data should be written

		toCopy := psm.recordSize - uint32(recordOffset)

		if restSize < toCopy {

			// If the record can contain more than restSize just
			// write restSize

			toCopy = restSize
		}

		// Write the data

		dataOffset2 := dataOffset + toCopy
		recordOffset2 := recordOffset + toCopy

		copy(record.Data()[recordOffset:recordOffset2], data[dataOffset:dataOffset2])

		// Calculate the rest size and new offset

		restSize -= toCopy
		dataOffset += toCopy

		psm.storagefile.ReleaseInUseID(cursor.Current(), true)

		// Go to the next record

		if restSize > 0 {

			// Error handling is done by surrounding Get calls

			next, _ := cursor.Next()

			record, err = psm.storagefile.Get(next)
			if err != nil {
				return err
			}

			recordOffset = pageview.OffsetData
		}
	}

	return nil
}

/*
allocate allocates a new slot of a given size.
*/
func (psm *PhysicalSlotManager) allocate(size uint32) (uint64, error) {

	// Normalize slot size

	normalizedSize := util.NormalizeSlotSize(size)

	// Try to find a free slot which was previously allocated

	loc, err := psm.freeManager.Get(normalizedSize)

	if err != nil {
		return 0, err
	}

	// If nothing of the right size was previously allocated then allocate
	// something new

	if loc == 0 {
		lastpage := psm.pager.Last(view.TypeDataPage)

		loc, err = psm.allocateNew(normalizedSize, lastpage)
		if err != nil {
			return 0, err
		}
	} else {

		// IF a location was found in the freeManager then try
		// to access it to make sure it is available - revert otherwise

		slotRecord := util.LocationRecord(loc)
		slotOffset := int(util.LocationOffset(loc))

		record, err := psm.storagefile.Get(slotRecord)
		if err != nil {

			// Revert back - the size may now be wrong but this is
			// still better than losing the whole record

			psm.freeManager.Add(loc, normalizedSize)
			return 0, err
		}

		util.SetCurrentSize(record, slotOffset, 0)

		psm.storagefile.ReleaseInUseID(slotRecord, true)
	}

	return loc, nil
}

/*
allocateNew allocates a new slot in the PagedStorageFile. Errors during this function might
cause the allocation of empty pages. The last allocated page pointers might
get out of sync with the actual data pages.
*/
func (psm *PhysicalSlotManager) allocateNew(size uint32, startPage uint64) (uint64, error) {

	var record *file.Record
	var pv *pageview.DataPage
	var offset uint32
	var header int
	var err error

	if startPage == 0 {

		// Create a new page if there is no start page

		startPage, err = psm.pager.AllocatePage(view.TypeDataPage)
		if err != nil {
			return 0, err
		}

		// Get the newly allocated page - all error checking was
		// done in the previous AllocatePage call

		record, _ = psm.storagefile.Get(startPage)

		pv = pageview.NewDataPage(record)
		pv.SetOffsetFirst(pageview.OffsetData)

		util.SetCurrentSize(record, pageview.OffsetData, 0)
		util.SetAvailableSize(record, pageview.OffsetData, 0)

	} else {

		record, err = psm.storagefile.Get(startPage)
		if err != nil {
			return 0, err
		}

		pv = pageview.NewDataPage(record)
	}

	offset = uint32(pv.OffsetFirst())

	if offset == 0 {

		// Take care of the special case if the current page was filled
		// exactly by the previous row

		psm.storagefile.ReleaseInUse(record)
		return psm.allocateNew(size, 0)
	}

	// Check if the last existing page is full - in that case just allocate
	// a new page

	header = int(offset)

	if offset == psm.recordSize || offset > psm.recordSize-util.SizeInfoSize {

		// Go to next page

		psm.storagefile.ReleaseInUse(record)
		return psm.allocateNew(size, 0)
	}

	slotsize := util.AvailableSize(record, header)

	// Loop over the slots and update the header and offset pointer - stop
	// if there is an empty space or we reach the end of the page

	for slotsize != 0 && offset < psm.recordSize {

		offset += slotsize + util.SizeInfoSize

		if offset == psm.recordSize || offset > psm.recordSize-util.SizeInfoSize {

			// Go to next page

			psm.storagefile.ReleaseInUse(record)
			return psm.allocateNew(size, 0)
		}

		header = int(offset)

		slotsize = util.AvailableSize(record, header)
	}

	// At this point we have the location for the new row

	loc := util.PackLocation(startPage, uint16(offset))

	// Calculate the remaining free space for the current page

	rspace := psm.recordSize - offset - util.SizeInfoSize

	if rspace < size {

		// If the remaining space is not enough we must allocate new pages

		// Increase the size if after the allocation only
		// ALLOCATION_ROUND_UP_THRESHOLD bytes would remain
		// on the record

		freeSpaceLastRecord := (size - rspace) % psm.availableRecordSize

		if (psm.availableRecordSize - freeSpaceLastRecord) <=
			(AllocationRoundUpThreshold + util.SizeInfoSize) {

			newsize := size
			newsize += (psm.availableRecordSize - freeSpaceLastRecord)
			nnewsize := util.NormalizeSlotSize(newsize)

			// Only do so if the new value is a valid normalized value

			if newsize == nnewsize {
				size = newsize
			}
		}

		// Write row header

		util.SetAvailableSize(record, header, size)
		psm.storagefile.ReleaseInUseID(startPage, true)

		// Calculate the rest size which needs to be allocated

		allocSize := size - rspace

		// Now allocate whole pages

		for allocSize >= psm.availableRecordSize {

			startPage, err = psm.pager.AllocatePage(view.TypeDataPage)
			if err != nil {
				return 0, err
			}

			// Error checking was done in previous AllocatePage call

			record, _ = psm.storagefile.Get(startPage)

			pv = pageview.NewDataPage(record)

			// Since this page contains only data there is no first row
			// offset

			pv.SetOffsetFirst(0)

			psm.storagefile.ReleaseInUseID(startPage, true)
			allocSize -= psm.availableRecordSize
		}

		// If there is still a rest left allocate one more page but reserve
		// only a part of it for the row

		if allocSize > 0 {

			startPage, err = psm.pager.AllocatePage(view.TypeDataPage)
			if err != nil {
				return 0, err
			}

			// Error checking was done in previous AllocatePage call

			record, _ = psm.storagefile.Get(startPage)

			pv = pageview.NewDataPage(record)
			pv.SetOffsetFirst(uint16(pageview.OffsetData + allocSize))

			psm.storagefile.ReleaseInUseID(startPage, true)
		}

	} else {

		// We found a free space on the current page

		// Increase the size if after the allocation only
		// ALLOCATION_ROUND_UP_THRESHOLD bytes would remain
		// on the record

		if (rspace - size) <= (AllocationRoundUpThreshold + util.SizeInfoSize) {

			newsize := rspace
			nnewsize := util.NormalizeSlotSize(newsize)

			// Only do so if the new value is a valid normalized value

			if newsize == nnewsize {
				size = newsize
			}
		}

		// Write row header

		util.SetAvailableSize(record, header, size)
		psm.storagefile.ReleaseInUseID(startPage, true)
	}

	return loc, nil
}
