/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package paging

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging/view"
)

const DBDIR = "pagingtest"

const InvalidFileName = "**" + string(0x0)

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

func TestPagedStorageFileInitialisation(t *testing.T) {

	sf, err := file.NewDefaultStorageFile(DBDIR+"/test1", true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record, err := sf.Get(0)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = NewPagedStorageFile(sf)
	if err != file.ErrAlreadyInUse {
		t.Error("Init of PageStorageFile should fail if header record is not available")
		return
	}

	sf.ReleaseInUse(record)

	psf, err := NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	if psf.StorageFile() != sf {
		t.Error("Unexpected StorageFile contained in PagedStorageFile")
		return
	}

	if psf.Header().record != record {
		t.Error("Unexpected Record contained in PagedStorageFileHeader")
		return
	}

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestPagedStorageFilePageManagement(t *testing.T) {
	sf, err := file.NewDefaultStorageFile(DBDIR+"/test2", true)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	if psf.FreePage(0) != ErrHeader {
		t.Error("Attempting to free the header record should cause a specific error")
		return
	}

	if _, err := psf.AllocatePage(view.TypeFreePage); err != ErrFreePage {
		t.Error("It should not be possible to allocate a free page")
		return
	}

	plist := make([]uint64, 0, 5)

	for i := 0; i < 5; i++ {
		p, err := psf.AllocatePage(view.TypeDataPage)
		if err != nil {
			t.Error(err)
		}
		plist = append(plist, p)
	}

	record, err := sf.Get(3)
	if err != nil {
		t.Error(err)
		return
	}
	if record.ReadUInt16(0) != 0x1991 {
		t.Error("Unexpected page header")
		return
	}
	sf.ReleaseInUse(record)

	if psf.First(view.TypeDataPage) != plist[0] {
		t.Error("Unexpected first page")
		return
	}
	if psf.Last(view.TypeDataPage) != plist[len(plist)-1] {
		t.Error("Unexpected last page")
		return
	}

	if psf.First(view.TypeFreePage) != 0 {
		t.Error("Unexpected first free page - no free pages should be available")
		return
	}

	record, err = sf.Get(3)
	if err != nil {
		t.Error(err)
		return
	}
	if err := psf.FreePage(3); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}
	sf.ReleaseInUse(record)

	if err := psf.FreePage(3); err != nil {
		t.Error(err)
		return
	}

	if err := psf.FreePage(3); err != ErrFreePage {
		t.Error("Attempting to free a page which is already free should cause an error")
		return
	}

	if psf.First(view.TypeFreePage) != 3 {
		t.Error("Unexpected first free page after freeing a page")
		return
	}

	checkPrevAndNext(t, psf, 3, 0, 0)

	if psf.FreePage(3) != ErrFreePage {
		t.Error("Attempting to free a free page should not be possible")
		return
	}

	if err := psf.FreePage(5); err != nil {
		t.Error(err)
		return
	}

	checkPrevAndNext(t, psf, 5, 0, 3)

	// Check that the second element has still the prev pointer pointing to 0

	checkPrevAndNext(t, psf, 3, 0, 0)

	// Check that the pointers for DATA pages are correct

	checkPrevAndNext(t, psf, 1, 0, 2)
	checkPrevAndNext(t, psf, 2, 1, 4)
	checkPrevAndNext(t, psf, 4, 2, 0)

	ptr, err := psf.AllocatePage(view.TypeTranslationPage)

	if err != nil {
		t.Error(err)
		return
	}
	if ptr != 5 {
		t.Error("New allocated page should be the last freed page")
		return
	}

	// Check data pointers
	checkPrevAndNext(t, psf, 1, 0, 2)
	checkPrevAndNext(t, psf, 2, 1, 4)
	checkPrevAndNext(t, psf, 4, 2, 0)

	// Check free pointers
	checkPrevAndNext(t, psf, 3, 0, 0)

	// Check translation pointers
	checkPrevAndNext(t, psf, 5, 0, 0)

	// Check the newly allocated page

	record, err = sf.Get(5)
	if err != nil {
		t.Error(err)
		return
	}

	// Record should have the translation page header

	if record.ReadUInt16(0) != 0x1992 {
		t.Error("Unexpected page header")
		return
	}

	pv := view.GetPageView(record)
	if pv.String() != "PageView: 5 (type:2 previous page:0 next page:0)" {
		t.Error("Unexpected pageview was returned:", pv)
		return
	}

	sf.ReleaseInUse(record)

	// Test allocation error - Using record 3 causes an error when getting the
	// first element of the free list. Using record 5 causes an error when
	// inserting the newly allocated record into the list

	record, err = sf.Get(3)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psf.AllocatePage(view.TypeTranslationPage)
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	record, err = sf.Get(5)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psf.AllocatePage(view.TypeTranslationPage)
	if err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	_, err = psf.AllocatePage(view.TypeTranslationPage)
	if err != nil {
		t.Error(err)
		return
	}

	record, err = sf.Get(7)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = psf.AllocatePage(view.TypeTranslationPage)
	if err != file.ErrAlreadyInUse {
		t.Error(err)
	}

	sf.ReleaseInUse(record)

	// Check data pointers
	checkPrevAndNext(t, psf, 1, 0, 2)
	checkPrevAndNext(t, psf, 2, 1, 4)
	checkPrevAndNext(t, psf, 4, 2, 0)

	// Check error case next record is in use

	record, err = sf.Get(4)
	if err != nil {
		t.Error(err)
		return
	}

	// Check we can't get Prev info when record is in use

	if _, err := psf.Prev(4); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	if err := psf.FreePage(2); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// Check error case previous record is in use

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	if err := psf.FreePage(4); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	if err := psf.FreePage(1); err != nil {
		t.Error(err)
		return
	}

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	if err := psf.Close(); err != file.ErrInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}

}

func TestPagedStorageFileTransactionPageManagement(t *testing.T) {
	sf, err := file.NewDefaultStorageFile(DBDIR+"/test3", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	if err := psf.Rollback(); err != nil {
		t.Error(err)
		return
	}

	plist := make([]uint64, 0, 5)

	for i := 0; i < 5; i++ {
		p, err := psf.AllocatePage(view.TypeDataPage)
		if err != nil {
			t.Error(err)
		}
		plist = append(plist, p)
	}

	if err := psf.Flush(); err != nil {
		t.Error(err)
		return
	}

	// Check that the pointers for DATA pages are correct

	checkPrevAndNext(t, psf, 1, 0, 2)
	checkPrevAndNext(t, psf, 2, 1, 3)
	checkPrevAndNext(t, psf, 3, 2, 4)
	checkPrevAndNext(t, psf, 4, 3, 5)
	checkPrevAndNext(t, psf, 5, 4, 0)

	// Now break it in a way that the datastructure is broken

	record, err := sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	if err := psf.FreePage(3); err != file.ErrAlreadyInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	// At this point page 3 is marked as free but the data pointers
	// of page 2 and 4 have not been updated

	checkPrevAndNext(t, psf, 1, 0, 2)
	checkPrevAndNext(t, psf, 2, 1, 3)
	checkPrevAndNext(t, psf, 3, 0, 0)
	checkPrevAndNext(t, psf, 4, 3, 5)
	checkPrevAndNext(t, psf, 5, 4, 0)

	if err := psf.Rollback(); err != nil {
		t.Error(err)
		return
	}

	checkPrevAndNext(t, psf, 1, 0, 2)
	checkPrevAndNext(t, psf, 2, 1, 3)
	checkPrevAndNext(t, psf, 3, 2, 4)
	checkPrevAndNext(t, psf, 4, 3, 5)
	checkPrevAndNext(t, psf, 5, 4, 0)

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}

	// Test error cases

	sf, err = file.NewDefaultStorageFile(DBDIR+"/test3-1", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err = NewPagedStorageFile(sf)
	if err != nil {
		t.Error(err)
		return
	}

	record, err = sf.Get(4)

	if err := psf.Flush(); err != file.ErrInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	psf.header.record = nil

	if err := psf.Flush(); err != file.ErrInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUseID(0, false)

	record, err = sf.Get(4)

	if err := psf.Rollback(); err != file.ErrInUse {
		t.Error(err)
		return
	}

	sf.ReleaseInUse(record)

	psf.header.record = nil

	sf.ReleaseInUseID(0, false)

	if err := psf.Close(); err != nil {
		t.Error(err)
		return
	}
}

func checkPrevAndNext(t *testing.T, psf *PagedStorageFile, rid uint64,
	prev uint64, next uint64) {

	p, err := psf.Prev(rid)
	if err != nil {
		t.Error(err)
		return
	}
	if p != prev {
		t.Error("Unexpected previous pointer:", p, "expected:", prev)
		return
	}

	n, err := psf.Next(rid)
	if err != nil {
		t.Error(err)
		return
	}
	if n != next {
		t.Error("Unexpected next pointer:", n, "expected:", next)
		return
	}

}
