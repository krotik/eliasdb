/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package view

import (
	"testing"

	"devt.de/krotik/eliasdb/storage/file"
)

func TestPageView(t *testing.T) {
	r := file.NewRecord(123, make([]byte, 20))

	pv := NewPageView(r, TypeDataPage)

	// Check that page type has been set

	if r.ReadInt16(0) != 0x1991 {
		t.Error("Unexpected header value")
		return
	}

	if r.PageView() != GetPageView(r) {
		t.Error("Unexpected page view on record")
		return
	}

	// Test corrupted page

	r.WriteSingleByte(0, 0x18)
	r.SetPageView(nil)

	testCheckMagicPanic(t, r)

	r.WriteSingleByte(0, 0x19)

	// Record should now contain the correct magic

	pv.checkMagic()

	if pv.Type() != TypeDataPage {
		t.Error("Wrong type for page view")
		return
	}

	if o := pv.String(); o != "PageView: 123 (type:1 previous page:0 next page:0)" {
		t.Error("Unexpected String output:", o)
	}

	// Check next/prev pointers - no particular error checking at this level

	if pv.NextPage() != 0 {
		t.Error("Unexpected next page")
		return
	}

	pv.SetNextPage(1)

	if pv.NextPage() != 1 {
		t.Error("Unexpected next page")
		return
	}

	if pv.PrevPage() != 0 {
		t.Error("Unexpected prev page")
		return
	}

	pv.SetPrevPage(1)

	if pv.PrevPage() != 1 {
		t.Error("Unexpected Prev page")
		return
	}
}

func testCheckMagicPanic(t *testing.T, r *file.Record) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Getting the page view from a corrupted record did not cause a panic.")
		}
	}()

	GetPageView(r)
}
