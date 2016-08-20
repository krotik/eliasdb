/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package pageview

import (
	"testing"

	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging/view"
)

func TestDataPage(t *testing.T) {
	r := file.NewRecord(123, make([]byte, 44))

	testCheckDataPageMagicPanic(t, r)

	// Make sure the record has a correct magic

	view.NewPageView(r, view.TypeDataPage)

	dp := NewDataPage(r)

	if ds := dp.DataSpace(); ds != 24 {
		t.Error("Unexpected data space", ds)
	}

	testCheckDataPageOffsetFirstPanic(t, dp)

	if of := dp.OffsetFirst(); of != 0 {
		t.Error("Unexpected first offset", of)
		return
	}

	dp.SetOffsetFirst(20)

	if of := dp.OffsetFirst(); of != 20 {
		t.Error("Unexpected first offset", of)
		return
	}
}

func testCheckDataPageMagicPanic(t *testing.T, r *file.Record) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Checking magic should fail.")
		}
	}()

	checkDataPageMagic(r)
}

func testCheckDataPageOffsetFirstPanic(t *testing.T, dp *DataPage) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Setting offset which is too small should fail.")
		}
	}()

	dp.SetOffsetFirst(OffsetData - 1)
}
