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
	"flag"
	"fmt"
	"os"
	"testing"

	"devt.de/common/fileutil"
	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging"
	"devt.de/eliasdb/storage/paging/view"
	"devt.de/eliasdb/storage/slotting/pageview"
	"devt.de/eliasdb/storage/util"
)

const DBDIR = "buckettest"

// Main function for all tests in this package

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup
	if res, _ := fileutil.PathExists(DBDIR); res {
		os.RemoveAll(DBDIR)
	}

	err := os.Mkdir(DBDIR, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests
	res := m.Run()

	// Teardown
	err = os.RemoveAll(DBDIR)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)
}

func TestFreePhysicalSlotManager(t *testing.T) {
	sf, err := file.NewDefaultStorageFile(DBDIR+"/test1", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := paging.NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	fpsm := NewFreePhysicalSlotManager(psf, false)

	// Add some locations

	fpsm.Add(util.PackLocation(5, 22), 30)
	fpsm.Add(util.PackLocation(6, 23), 35)

	out := fpsm.String()

	if out != "FreePhysicalSlotManager: buckettest/test1 (onlyAppend:false lastMaxSlotSize:0)\n"+
		"Ids  :[327702 393239]\n"+
		"Sizes:[30 35]" {
		t.Error("Unexpected output of FreePhysicalSlotManager:", out)
	}

	if err = fpsm.Flush(); err != nil {
		t.Error(nil)
		return
	}

	if len(fpsm.slots) != 0 || len(fpsm.sizes) != 0 {
		t.Error("Nothing should be left in the slot cache after a flush")
		return
	}

	// Check pages are allocated

	cursor := paging.NewPageCursor(fpsm.pager, view.TypeFreePhysicalSlotPage, 0)

	if page, err := cursor.Next(); page != 1 || err != nil {
		t.Error("Unexpected free physical slot page:", page, err)
		return
	}
	if page, err := cursor.Next(); page != 0 || err != nil {
		t.Error("Unexpected free physical slot page:", page, err)
		return
	}

	page := fpsm.pager.First(view.TypeFreePhysicalSlotPage)
	if page != 1 {
		t.Error("Unexpected first free physical slot page")
		return
	}

	fpspRec, err := sf.Get(1)
	if err != nil {
		t.Error(err)
	}
	fpsp := pageview.NewFreePhysicalSlotPage(fpspRec)

	if fsc := fpsp.FreeSlotCount(); fsc != 2 {
		t.Error("Unexpected number of stored free slots", fsc)
	}

	// Check that both slotinfos have been written

	if fpsp.SlotInfoLocation(0) != util.PackLocation(5, 22) {
		t.Error("Unexpected free slot info")
		return
	}

	if fpsp.SlotInfoLocation(1) != util.PackLocation(6, 23) {
		t.Error("Unexpected free slot info")
		return
	}

	sf.ReleaseInUse(fpspRec)

	// Check that we can find them

	loc, err := fpsm.Get(31)
	if err != nil {
		t.Error(err)
		return
	}

	if util.LocationRecord(loc) != 6 || util.LocationOffset(loc) != 23 {
		t.Error("Unexpected location was found", util.LocationRecord(loc), util.LocationOffset(loc))
		return
	}

	if fsc := fpsp.FreeSlotCount(); fsc != 1 {
		t.Error("Unexpected number of stored free slots", fsc)
	}

	// Test only append flag

	fpsm.onlyAppend = true

	loc, err = fpsm.Get(29)
	if err != nil || loc != 0 {
		t.Error("Unexpected onlyAppend result:", loc, err)
	}

	fpsm.onlyAppend = false

	loc, err = fpsm.Get(29)
	if err != nil {
		t.Error(err)
		return
	}

	if util.LocationRecord(loc) != 5 || util.LocationOffset(loc) != 22 {
		t.Error("Unexpected location was found", util.LocationRecord(loc), util.LocationOffset(loc))
		return
	}

	if fsc := fpsp.FreeSlotCount(); fsc != 0 {
		t.Error("Unexpected number of stored free slots", fsc)
	}

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestFreePhysicalSlotManagerScale(t *testing.T) {
	sf, err := file.NewDefaultStorageFile(DBDIR+"/test2", false)
	if err != nil {
		t.Error(err.Error())
		return
	}
	shadow, err := file.NewDefaultStorageFile(DBDIR+"/test2_", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := paging.NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	fpsm := NewFreePhysicalSlotManager(psf, false)

	// Add a lot of locations

	for i := 0; i < 5000; i++ {
		fpsm.Add(util.PackLocation(uint64(i), uint16(i%1000)), uint32(i%500))
	}

	// Check Flush and low level doFlush if a page can't be accessed

	if _, err := sf.Get(1); err != nil {
		t.Error(err)
		return
	}

	if err := fpsm.Flush(); err != file.ErrAlreadyInUse {
		t.Error("Unexpected flush result:", err)
		return
	}

	if i, err := fpsm.doFlush(1, 0); i != 0 || err != file.ErrAlreadyInUse {
		t.Error("Unexpected doFlush result:", i, err)
		return
	}

	if err := sf.ReleaseInUseID(1, false); err != nil {
		t.Error(err)
		return
	}

	// Check the doFlush error return in Flush when allocating new pages

	fpsm.storagefile = shadow

	if _, err := shadow.Get(1); err != nil {
		t.Error(err)
		return
	}

	if err := fpsm.Flush(); err != file.ErrAlreadyInUse {
		t.Error("Unexpected flush result:", err)
		return
	}

	if err := shadow.ReleaseInUseID(1, false); err != nil {
		t.Error(err)
		return
	}

	fpsm.storagefile = sf

	// Now do the real flush

	if err := fpsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	// Count the allocated pages

	c, err := paging.CountPages(fpsm.pager, view.TypeFreePhysicalSlotPage)
	if c != 15 || err != nil {
		t.Error("Unexpected counting result:", c, err)
		return
	}

	// Check lastMaxSlotSize works

	if loc, err := fpsm.Get(600); loc != 0 || err != nil {
		t.Error("Unexpected Get result:", loc, err)
		return
	}
	if fpsm.lastMaxSlotSize != 499 {
		t.Error("Unexpected lastMaxSlotSize:", fpsm.lastMaxSlotSize)
		return
	}

	// Any subsequent call should fail more quickly

	if loc, err := fpsm.Get(600); loc != 0 || err != nil {
		t.Error("Unexpected Get result:", loc, err)
		return
	}

	// Free slot for size 499 should be on page 2

	if _, err := sf.Get(2); err != nil {
		t.Error(err)
		return
	}

	if loc, err := fpsm.Get(499); err != file.ErrAlreadyInUse {
		t.Error("Unexpected Get result:", loc, err)
		return
	}

	if err := sf.ReleaseInUseID(2, false); err != nil {
		t.Error(err)
		return
	}

	loc, err := fpsm.Get(499)
	if err != nil || loc != 32702963 {
		t.Error("Unexpected Get result:", err, loc)
		return
	}

	if fpsm.lastMaxSlotSize != 0 {
		t.Error("Unexpected lastMaxSlotSize:", fpsm.lastMaxSlotSize)
		return
	}

	// Next free slot for size 499 should be on page 3

	if _, err := sf.Get(3); err != nil {
		t.Error(err)
		return
	}

	if loc, err := fpsm.Get(499); err != file.ErrAlreadyInUse {
		t.Error("Unexpected Get result:", loc, err)
		return
	}

	if err := sf.ReleaseInUseID(3, false); err != nil {
		t.Error(err)
		return
	}

	// There was an error check that the lastMaxSlotSize was reset

	if fpsm.lastMaxSlotSize != 0 {
		t.Error("Unexpected lastMaxSlotSize:", fpsm.lastMaxSlotSize)
		return
	}

	loc, err = fpsm.Get(499)
	if err != nil || loc != 65471463 {
		t.Error("Unexpected Get result:", err, loc)
		return
	}

	if fpsm.lastMaxSlotSize != 0 {
		t.Error("Unexpected lastMaxSlotSize:", fpsm.lastMaxSlotSize)
		return
	}

	fpsm.Add(65471666, 699)

	if _, err := sf.Get(2); err != nil {
		t.Error(err)
		return
	}

	if err := fpsm.Flush(); err != file.ErrAlreadyInUse {
		t.Error("Unexpected Flush result:", err)
		return
	}

	if err := sf.ReleaseInUseID(2, false); err != nil {
		t.Error(err)
		return
	}

	if err := fpsm.Flush(); err != nil {
		t.Error(err)
		return
	}

	loc, err = fpsm.Get(698)
	if err != nil || loc != 65471666 {
		t.Error("Unexpected Get result:", err, loc)
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
