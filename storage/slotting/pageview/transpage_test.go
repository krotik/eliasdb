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

func TestTransPage(t *testing.T) {
	r := file.NewRecord(123, make([]byte, 44))

	testCheckTransPageMagicPanic(t, r)

	// Make sure the record has a correct magic

	view.NewPageView(r, view.TypeTranslationPage)

	NewTransPage(r)
}

func testCheckTransPageMagicPanic(t *testing.T, r *file.Record) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Checking magic should fail.")
		}
	}()

	checkTransPageMagic(r)
}
