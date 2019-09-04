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
	"testing"

	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging/view"
)

func TestPageCursor(t *testing.T) {
	sf, err := file.NewDefaultStorageFile(DBDIR+"/test4", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	psf, err := NewPagedStorageFile(sf)
	if err != nil {
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

	pc := NewPageCursor(psf, view.TypeDataPage, 0)

	checkPrev(t, pc, 0)

	if cur := pc.Current(); cur != 0 {
		t.Error("Unexpected current page", cur)
	}

	checkNext(t, pc, 1)

	if cur := pc.Current(); cur != 1 {
		t.Error("Unexpected current page", cur)
	}

	checkNext(t, pc, 2)
	checkPrev(t, pc, 1)
	checkPrev(t, pc, 0)
	checkPrev(t, pc, 0)

	// Once the first page was iterated we will not go back to 0

	if cur := pc.Current(); cur != 1 {
		t.Error("Unexpected current page", cur)
	}

	checkNext(t, pc, 2)
	checkNext(t, pc, 3)
	checkNext(t, pc, 4)
	checkNext(t, pc, 5)

	if cur := pc.Current(); cur != 5 {
		t.Error("Unexpected current page", cur)
	}
	checkNext(t, pc, 0)

	if cur := pc.Current(); cur != 5 {
		t.Error("Unexpected current page", cur)
	}

	checkPrev(t, pc, 4)

	// Test error cases of next / prev by putting records in use

	sf.Get(4)

	_, err = pc.Prev()
	if err != file.ErrAlreadyInUse {
		t.Error("Operation should fail as the required record is in use")
		return
	}

	_, err = pc.Next()
	if err != file.ErrAlreadyInUse {
		t.Error("Operation should fail as the required record is in use")
		return
	}

	sf.ReleaseInUseID(4, false)

	psf.Close()
}

func checkNext(t *testing.T, pc *PageCursor, expected uint64) {
	next, err := pc.Next()
	if err != nil {
		t.Error(err)
		return
	}
	if next != expected {
		t.Error("Unexpected next page", next, "expected", expected)
	}
}

func checkPrev(t *testing.T, pc *PageCursor, expected uint64) {
	prev, err := pc.Prev()
	if err != nil {
		t.Error(err)
		return
	}
	if prev != expected {
		t.Error("Unexpected previous page", prev, "expected", expected)
	}
}
