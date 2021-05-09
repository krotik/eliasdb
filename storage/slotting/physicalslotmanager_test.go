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
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging"
	"devt.de/krotik/eliasdb/storage/paging/view"
	"devt.de/krotik/eliasdb/storage/slotting/pageview"
	"devt.de/krotik/eliasdb/storage/util"
)

func TestPhysicalSlotManager(t *testing.T) {
	sf, err := file.NewDefaultStorageFile(DBDIR+"/test5_data", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := paging.NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	fsf, err := file.NewDefaultStorageFile(DBDIR+"/test5_free", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	fpsf, err := paging.NewPagedStorageFile(fsf)
	if err != nil {
		t.Error(err)
		return
	}

	psm := NewPhysicalSlotManager(psf, fpsf, false)

	// Build up a data array

	arr := make([]byte, 9000)
	for i := 0; i < 9000; i++ {
		arr[i] = byte(i%5) + 1
	}
	arr2 := make([]byte, 9000)
	for i := 0; i < 9000; i++ {
		arr2[i] = byte(i%5) + 5
	}

	loc, err := psm.Insert(arr, 1, 8999)
	if err != nil {
		t.Error(err)
		return
	}

	// Location should be beginning of the first record

	//checkLocation(t, loc, 1, 20)

	// Read back the written data

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)

	if err := psm.Fetch(loc, buf); err != nil {
		t.Error("Unexpected read result:", err)
		return
	}

	buf.Flush()

	str1 := fmt.Sprint(b.Bytes())
	str2 := fmt.Sprint(arr[1:])

	if str1 != str2 {
		t.Error("Unexpected result reading back what was written")
		return
	}

	loc, err = psm.Update(loc, arr2, 0, 9000)
	if err != nil {
		t.Error(err)
		return
	}

	// Location should have changed now

	checkLocation(t, loc, 3, 871)

	// Make sure the new free slots are known

	psm.Flush()

	// Insert new data - the manager should reuse the previous location

	loc, err = psm.Insert(arr2, 1, 8999)
	if err != nil {
		t.Error(err)
		return
	}

	checkLocation(t, loc, 1, 20)

	if err := psm.Free(loc); err != nil {
		t.Error(err)
		return
	}

	if err := psm.Flush(); err != nil {
		t.Error(err)
		return
	}

	// Test error cases

	testInsertPanic(t, psm)

	record, err := fsf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.Insert(make([]byte, 1), 0, 1)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected insert result:", err)
		return
	}

	fsf.ReleaseInUse(record)

	err = psm.Free(util.PackLocation(0, 20))
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected free result:", err)
		return
	}

	record, err = sf.Get(5)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.allocate(10)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected allocate result:", err)
		return
	}

	sf.ReleaseInUse(record)

	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	// This slot shot be free on page 1

	_, err = psm.Insert(arr2, 1, 8999)

	// The insert should have failed. The allocated space
	// for it should have been send back to the free manager

	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected insert result:", err)
		return
	}

	sf.ReleaseInUse(record)

	// This should write the recovered free location
	// back to the free manager

	psm.Flush()

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.Insert(arr2, 1, 8999)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected update result:", err)
		return
	}

	sf.ReleaseInUse(record)

	checkLocation(t, loc, 1, 20)

	// Write the free data which has been declared during the
	// last failed call to disk

	psm.Flush()

	loc, err = psm.Insert(arr2, 1, 8999)
	if err != nil {
		t.Error("Unexpected insert result:", err)
		return
	}

	checkLocation(t, loc, 1, 20)

	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.Update(loc, arr2, 1, 8999)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected update result:", err)
		return
	}

	sf.ReleaseInUse(record)

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.Update(loc, arr2, 1, 8999)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected update result:", err)
		return
	}

	sf.ReleaseInUse(record)

	record, err = sf.Get(5)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.Update(loc, arr2, 0, 9000)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected update result:", err)
		return
	}

	sf.ReleaseInUse(record)

	record, err = sf.Get(5)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.Update(loc, arr2, 0, 9000)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
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

func testInsertPanic(t *testing.T, psm *PhysicalSlotManager) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Inserting 0 bytes did not cause a panic.")
		}
	}()

	psm.Insert(make([]byte, 0), 0, 0)
}

func TestPhysicalSlotManagerReadWrite(t *testing.T) {

	sf, err := file.NewDefaultStorageFile(DBDIR+"/test4_data", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := paging.NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	fsf, err := file.NewDefaultStorageFile(DBDIR+"/test4_free", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	fpsf, err := paging.NewPagedStorageFile(fsf)
	if err != nil {
		t.Error(err)
		return
	}

	psm := NewPhysicalSlotManager(psf, fpsf, false)

	// Allocate some space

	loc1, err := psm.allocateNew(10000, 0)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected offset is the beginning of page 1

	checkLocation(t, loc1, 1, 20)

	// Allocate some more space

	loc2, err := psm.allocateNew(10, 3)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected offset is on page 3

	checkLocation(t, loc2, 3, 1872)

	// Build up a data array

	arr := make([]byte, 9000)
	for i := 0; i < 9000; i++ {
		arr[i] = byte(i%5) + 1
	}

	// Now write the data array in the allocated space

	if err := psm.write(loc1, arr, 1, 8999); err != nil {
		t.Error("Unexpected write result:", err)
		return
	}

	// Now check the actual written data

	record, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Slot size header should be 10000 available and 8999 current

	if asize := util.AvailableSize(record, 20); asize != 10000 {
		t.Error("Unexpected available size:", asize)
		return
	}

	if csize := util.CurrentSize(record, 20); csize != 8999 {
		t.Error("Unexpected current size:", csize)
		return
	}

	// Check the beginning of the written data

	if wdata := record.ReadUInt16(24); wdata != 0x0203 {
		t.Error("Unexpected beginning of written data:", wdata)
		return
	}

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Check that the second page is a full data page

	pv := pageview.NewDataPage(record)
	if of := pv.OffsetFirst(); of != 0 {
		t.Error("Unexpected first offset:", of)
		return
	}

	if record.ReadSingleByte(20) != 0x04 || record.ReadSingleByte(4095) != 0x04 {
		t.Error("Unexpected record data:", record)
		return
	}

	record, err = sf.Get(3)
	if err != nil {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Check that the last page is partially written

	// Offset should be the location of the second allocated data block

	pv = pageview.NewDataPage(record)
	if of := pv.OffsetFirst(); of != 1872 {
		t.Error("Unexpected first offset:", of)
		return
	}

	// Data should end with 5 on the following location
	// 8999 data written + 4 byte header = 9003 bytes written
	// 9003 - 4076 page1 - 4076 page2 = 851 bytes for the last page
	// 20 bytes header + 851 written bytes = 871 bytes (offset 870)

	if lastByte := record.ReadSingleByte(870); lastByte != 5 {
		t.Error("Unexpected last byte:", lastByte)
		return
	}

	if lastByteAfter := record.ReadSingleByte(871); lastByteAfter != 0 {
		t.Error("Unexpected byte after last byte:", lastByteAfter)
		return
	}

	// Read back the written data

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)

	if err := psm.Fetch(loc1, buf); err != nil {
		t.Error("Unexpected read result:", err)
		return
	}

	buf.Flush()

	str1 := fmt.Sprint(b.Bytes())
	str2 := fmt.Sprint(arr[1:])

	if str1 != str2 {
		t.Error("Unexpected result reading back what was written")
		return
	}

	// Test some special cases

	record, err = sf.Get(util.LocationRecord(loc2))
	if err != nil {
		t.Error(err)
		return
	}

	err = psm.write(loc2, make([]byte, 0), 0, 0)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected write result:", err)
	}
	err = psm.Fetch(loc2, buf)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected read result:", err)
		return
	}

	sf.ReleaseInUse(record)

	if err := psm.write(loc2, make([]byte, 0), 0, 0); err != nil {
		t.Error("Unexpected write result:", err)
	}

	var b2 bytes.Buffer
	buf = bufio.NewWriter(&b2)

	if err := psm.Fetch(loc2, buf); err != nil {
		t.Error("Unexpected read result:", err)
		return
	}

	buf.Flush()
	if len(b2.Bytes()) != 0 {
		t.Error("Nothing should have been read back")
		return
	}

	if asize := util.AvailableSize(record, int(util.LocationOffset(loc2))); asize != 10 {
		t.Error("Unexpected available size:", asize)
		return
	}

	if csize := util.CurrentSize(record, int(util.LocationOffset(loc2))); csize != 0 {
		t.Error("Unexpected current size:", csize)
		return
	}

	record, err = sf.Get(3)
	if err != nil {
		t.Error(err)
		return
	}

	err = psm.write(loc2, make([]byte, 10000), 0, 9999)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected write result:", err)
	}
	err = psm.Fetch(loc2, buf)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected read result:", err)
		return
	}

	sf.ReleaseInUse(record)

	loc3, err := psm.allocateNew(10000, 3)
	if err != nil {
		t.Error(err)
		return
	}

	record, err = sf.Get(5)
	if err != nil {
		t.Error(err)
		return
	}

	err = psm.write(loc3, make([]byte, 10000), 0, 9999)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected write result:", err)
	}
	err = psm.Fetch(loc3, buf)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error("Unexpected read result:", err)
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

func TestPhysicalSlotManagerAllocateNew(t *testing.T) {

	sf, err := file.NewDefaultStorageFile(DBDIR+"/test3_data", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := paging.NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	fsf, err := file.NewDefaultStorageFile(DBDIR+"/test3_free", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	fpsf, err := paging.NewPagedStorageFile(fsf)
	if err != nil {
		t.Error(err)
		return
	}

	psm := NewPhysicalSlotManager(psf, fpsf, false)

	// Check the simple case

	size := util.NormalizeSlotSize(500)

	// Error case new allocated page is already in use

	record, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.allocateNew(size, 0)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Test first allocation

	loc, err := psm.allocateNew(size, 0)
	if err != nil {
		t.Error(err)
		return
	}

	checkLocation(t, loc, 1, pageview.OffsetData)

	// Error case existing page is already in use

	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.allocateNew(10, 1)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	loc, err = psm.allocateNew(10, 1)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected offset is 524

	// Page header (20) + prev. allocated data (500) + SizeInfoSize header (4)

	exploc := pageview.OffsetData + 500 + util.SizeInfoSize
	if exploc != 524 {
		t.Error("Expected location should be 532 but is:", exploc)
		return
	}

	checkLocation(t, loc, 1, uint16(exploc))

	loc, err = psm.allocateNew(7000, 1)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected offset is 538

	// Last offset (524) + prev. allocated data (10) + SizeInfoSize header (4)

	checkLocation(t, loc, 1, 538)

	// Last page is now page 2

	loc, err = psm.allocateNew(10, 2)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected offset is 3466 (+ 1 page)

	// Last offset (538) + prev. allocated data (7000) + SizeInfoSize header (4)
	// Default size for one record is 4096 - 20 bytes header = 4076
	// 7542 - 4076 = 3466

	checkLocation(t, loc, 2, 3466)

	loc, err = psm.allocateNew(10000, 2)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected offset is 3480

	// Last offset (3466) + prev. allocated data (10) + SizeInfoSize header (4)

	checkLocation(t, loc, 2, 3480)

	// Last page is now page 5 - This allocation should fill up page 5 exacly
	// - allocation should be rounded up by 6

	loc, err = psm.allocateNew(2830, 5)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected offset is 1256 (+ 3 pages)

	// Last offset (3480) + prev. allocated data (10000) + SizeInfoSize header (4)
	// Default size for one record is 4096 - 20 bytes header = 4076
	// 13484 - 4076 - 4076 - 4076 = 1256

	checkLocation(t, loc, 5, 1256)

	// Since page 5 was filled up we should be now allocated to page 6 at
	// the beginning - the next allocation should take up page 6 and 7

	loc, err = psm.allocateNew(8147, 5)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected offset is the beginning of page 6

	checkLocation(t, loc, 6, 20)

	// With the allocated space we should fill page 7
	// Rounded up by 1

	if lap := psm.pager.Last(view.TypeDataPage); lap != 7 {
		t.Error("Unexpected last allocated page", lap)
		return
	}

	// Since page 7 was filled up completely and its first offset is 0
	// the algorithm should allocate a new page.

	loc, err = psm.allocateNew(10, 7)
	if err != nil {
		t.Error(err)
	}

	// Expected offset is the beginning of page 8

	checkLocation(t, loc, 8, 20)

	// Construct a page where not enough space is free for an allocation

	page, err := psm.pager.AllocatePage(view.TypeDataPage)
	if err != nil {
		t.Error(err)
		return
	}

	record, err = psm.storagefile.Get(page)
	if err != nil {
		t.Error(err)
		return
	}

	pv := pageview.NewDataPage(record)
	pv.SetOffsetFirst(uint16(4093))

	psm.storagefile.ReleaseInUseID(page, true)

	loc, err = psm.allocateNew(10, 9)
	if err != nil {
		t.Error(err)
	}

	// Expected offset is the beginning of page 10

	checkLocation(t, loc, 10, 20)

	// Now a two error tests which will cause the page pointers get out of sync

	record, err = sf.Get(12)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.allocateNew(8147, 5)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Page 11 was now allocated but not written to

	loc, err = psm.allocateNew(10, 9)
	if err != nil {
		t.Error(err)
	}

	// Expected offset is the beginning of page 12

	checkLocation(t, loc, 12, 20)

	record, err = sf.Get(14)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psm.allocateNew(8147, 12)
	if sfe, ok := err.(*file.StorageFileError); !ok || sfe.Type != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Page 13 was now allocated but not written to

	loc, err = psm.allocateNew(10, 9)
	if err != nil {
		t.Error(err)
	}

	// Expected offset is the beginning of page 14

	checkLocation(t, loc, 14, 20)

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}

	if err := fpsf.Close(); err != nil {
		t.Error(err)
		return
	}
}

func checkLocation(t *testing.T, loc uint64, record uint64, offset uint16) {
	lrecord := util.LocationRecord(loc)
	loffset := util.LocationOffset(loc)
	if lrecord != record || loffset != offset {
		t.Error("Unexpected location. Expected:", record, offset, "Got:", lrecord, loffset)
	}
}
