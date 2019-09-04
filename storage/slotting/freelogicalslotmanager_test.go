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

	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging"
	"devt.de/krotik/eliasdb/storage/paging/view"
	"devt.de/krotik/eliasdb/storage/slotting/pageview"
	"devt.de/krotik/eliasdb/storage/util"
)

func TestFreeLogicalSlotManager(t *testing.T) {
	sf, err := file.NewDefaultStorageFile(DBDIR+"/test6", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := paging.NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	flsm := NewFreeLogicalSlotManager(psf)

	testAddPanic(t, flsm)

	// Add some locations

	flsm.Add(util.PackLocation(5, 22))
	flsm.Add(util.PackLocation(6, 23))

	out := flsm.String()

	if out != "FreeLogicalSlotManager: buckettest/test6\n"+
		"Ids  :[327702 393239]\n" {
		t.Error("Unexpected output of FreeLogicalSlotManager:", out)
	}

	if err = flsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	if len(flsm.slots) != 0 {
		t.Error("Nothing should be left in the slot cache after a flush")
		return
	}

	// Check pages are allocated

	cursor := paging.NewPageCursor(flsm.pager, view.TypeFreeLogicalSlotPage, 0)

	if page, err := cursor.Next(); page != 1 || err != nil {
		t.Error("Unexpected free logical slot page:", page, err)
		return
	}
	if page, err := cursor.Next(); page != 0 || err != nil {
		t.Error("Unexpected free logical slot page:", page, err)
		return
	}

	page := flsm.pager.First(view.TypeFreeLogicalSlotPage)
	if page != 1 {
		t.Error("Unexpected first free logical slot page")
		return
	}

	flspRec, err := sf.Get(1)
	if err != nil {
		t.Error(err)
	}
	flsp := pageview.NewFreeLogicalSlotPage(flspRec)

	if fsc := flsp.FreeSlotCount(); fsc != 2 {
		t.Error("Unexpected number of stored free slots", fsc)
	}

	// Check that both slotinfos have been written

	if flsp.SlotInfoLocation(0) != util.PackLocation(5, 22) {
		t.Error("Unexpected free slot info")
		return
	}

	if flsp.SlotInfoLocation(1) != util.PackLocation(6, 23) {
		t.Error("Unexpected free slot info")
		return
	}

	sf.ReleaseInUse(flspRec)

	// Check that we can find them

	loc, err := flsm.Get()
	if err != nil {
		t.Error(err)
		return
	}

	checkLocation(t, loc, 5, 22)

	if fsc := flsp.FreeSlotCount(); fsc != 1 {
		t.Error("Unexpected number of stored free slots", fsc)
	}

	// Test error handling in Flush

	flsm.Add(util.PackLocation(4, 21))

	rec, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	if err = flsm.Flush(); err != file.ErrAlreadyInUse {
		t.Error("Unexpected Get result:", err)
		return
	}

	// Can get something without error from the unflushed slot list

	loc, err = flsm.Get()
	if err != nil {
		t.Error(err)
		return
	}

	checkLocation(t, loc, 4, 21)

	err = sf.ReleaseInUseID(rec.ID(), false)
	if err != nil {
		t.Error(err)
		return
	}

	loc, err = flsm.Get()
	if err != nil {
		t.Error(err)
		return
	}

	checkLocation(t, loc, 6, 23)

	// Test multiple insert

	flsm.Add(util.PackLocation(9, 1))

	if err = flsm.Flush(); err != nil {
		t.Error(err)
		return
	}
	flsm.Add(util.PackLocation(9, 2))
	flsm.Add(util.PackLocation(9, 3))
	flsm.Add(util.PackLocation(9, 4))

	if err = flsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	// Test get error when record in use and slot list is empty

	rec, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = flsm.Get()
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	err = sf.ReleaseInUseID(rec.ID(), false)
	if err != nil {
		t.Error(err)
		return
	}

	loc, err = flsm.Get()
	if err != nil {
		t.Error(err)
		return
	}
	checkLocation(t, loc, 9, 1)

	loc, err = flsm.Get()
	if err != nil {
		t.Error(err)
		return
	}
	checkLocation(t, loc, 9, 2)

	loc, err = flsm.Get()
	if err != nil {
		t.Error(err)
		return
	}
	checkLocation(t, loc, 9, 3)

	loc, err = flsm.Get()
	if err != nil {
		t.Error(err)
		return
	}
	checkLocation(t, loc, 9, 4)

	if fsc := flsp.FreeSlotCount(); fsc != 0 {
		t.Error("Unexpected number of stored free slots", fsc)
	}

	// Test special case when a page is in the pager list but has no slots allocated

	for i := 1; i < 1001; i++ {
		flsm.Add(util.PackLocation(uint64(i), uint16(i%1000)))
	}

	if err = flsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	// Manually free all slots on the first page

	flspRec, err = sf.Get(1)
	if err != nil {
		t.Error(err)
	}
	flsp = pageview.NewFreeLogicalSlotPage(flspRec)

	var j uint16
	for j = 0; j < flsp.MaxSlots(); j++ {
		flsp.ReleaseSlotInfo(j)
	}

	sf.ReleaseInUse(flspRec)

	// Check we get slotlocation 0 from second page

	loc, err = flsm.Get()
	if err != nil {
		t.Error(err)
		return
	}

	checkLocation(t, loc, 510, 510)

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}
}

func testAddPanic(t *testing.T, flsm *FreeLogicalSlotManager) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Adding location 0 did not cause a panic.")
		}
	}()

	flsm.Add(0)
}

func TestFreeLogiclaSlotManagerScale(t *testing.T) {

	sf, err := file.NewDefaultStorageFile(DBDIR+"/test7", false)
	if err != nil {
		t.Error(err.Error())
		return
	}
	shadow, err := file.NewDefaultStorageFile(DBDIR+"/test7_", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := paging.NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	flsm := NewFreeLogicalSlotManager(psf)

	// Add a lot of locations

	for i := 1; i < 5001; i++ {
		flsm.Add(util.PackLocation(uint64(i), uint16(i%1000)))
	}

	// Check Flush and low level doFlush if a page can't be accessed

	if _, err := sf.Get(1); err != nil {
		t.Error(err)
		return
	}

	if err := flsm.Flush(); err != file.ErrAlreadyInUse {
		t.Error("Unexpected flush result:", err)
		return
	}

	if i, err := flsm.doFlush(1, 0); i != 0 || err != file.ErrAlreadyInUse {
		t.Error("Unexpected doFlush result:", i, err)
		return
	}

	if err := sf.ReleaseInUseID(1, false); err != nil {
		t.Error(err)
		return
	}

	// Check the doFlush error return in Flush when allocating new pages

	flsm.storagefile = shadow

	if _, err := shadow.Get(1); err != nil {
		t.Error(err)
		return
	}

	if err := flsm.Flush(); err != file.ErrAlreadyInUse {
		t.Error("Unexpected flush result:", err)
		return
	}

	if err := shadow.ReleaseInUseID(1, false); err != nil {
		t.Error(err)
		return
	}

	flsm.storagefile = sf

	// Now do the real flush

	if err := flsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	// Count the allocated pages

	c, err := paging.CountPages(flsm.pager, view.TypeFreeLogicalSlotPage)
	if c != 10 || err != nil {
		t.Error("Unexpected counting result:", c, err)
		return
	}

	// Remove some free slots from the list

	for i := 1; i < 1001; i++ {
		if res, err := flsm.Get(); res != util.PackLocation(uint64(i), uint16(i%1000)) || err != nil {
			t.Error("Unexpected Get result", util.LocationRecord(res), util.LocationOffset(res), i, err)
			return
		}
	}

	// Count the allocated pages (one page should be free now)

	c, err = paging.CountPages(flsm.pager, view.TypeFreeLogicalSlotPage)
	if c != 9 || err != nil {
		t.Error("Unexpected counting result:", c, err)
		return
	}

	// Now add new free slots with a different pattern

	for i := 1; i < 1001; i++ {
		flsm.Add(util.PackLocation(uint64(i+666), uint16(i%1000)))
	}

	if err := flsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	c, err = paging.CountPages(flsm.pager, view.TypeFreeLogicalSlotPage)
	if c != 10 || err != nil {
		t.Error("Unexpected counting result:", c, err)
		return
	}

	// All slots from page 1 were removed during the first request for 1000 slots. The page was
	// subsequently deallocated. Page 2 was partly cleared. Adding again 1000 slots filled up page 2 again
	// and allocated page 1 again this time however at the end of the list. In the following loop we empty
	// all pages.

	// First we empty page 2 containing partly the new pattern and partly the old one
	// We then empty all pages with the old pattern
	// Finally we empty the remaining page with the new pattern

	for i := 1; i < 1001; i++ {

		if res, err := flsm.Get(); res != util.PackLocation(uint64(i+666), uint16(i%1000)) || err != nil {
			t.Error("Unexpected Get result", util.LocationRecord(res), util.LocationOffset(res), i, err)
			return
		}

		if i == 491 {

			for j := 1001; j < 5001; j++ {

				if res, err := flsm.Get(); res != util.PackLocation(uint64(j), uint16(j%1000)) || err != nil {
					t.Error("*Unexpected Get result", util.LocationRecord(res), util.LocationOffset(res), j, err)
					return
				}
			}
		}
	}

	// Check that all empty slots have been retrieved and nothing is left on the free pages

	if res, err := flsm.Get(); res != 0 || err != nil {
		t.Error("Unexpected final Get call result", res, err)
		return
	}

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}
	if err := shadow.Close(); err != nil {
		t.Error(err)
		return
	}
}
