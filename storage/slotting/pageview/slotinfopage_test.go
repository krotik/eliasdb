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

	"devt.de/krotik/eliasdb/storage/file"
	"devt.de/krotik/eliasdb/storage/paging/view"
)

func TestSlotInfoPage(t *testing.T) {
	r := file.NewRecord(123, make([]byte, 20))

	// Make sure the record has a correct magic

	view.NewPageView(r, view.TypeDataPage)

	si := NewSlotInfoPage(r)

	si.SetSlotInfo(2, 99, 45)

	if si.SlotInfoOffset(2) != 45 {
		t.Error("Unexpected offset read back")
	}

	if si.SlotInfoRecord(2) != 99 {
		t.Error("Unexpected record read back")
	}
}
