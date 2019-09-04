/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package util

import "testing"

func TestLocation(t *testing.T) {
	var i uint16
	var j uint64

	for i = 0; i < MaxOffsetValue; i++ {

		location := PackLocation(1, uint16(i))
		recID := LocationRecord(location)

		if recID != 1 {
			t.Error("Unexpected record for location", location, " i:", i, " recId:", recID)
			return
		}

		off := LocationOffset(location)

		if off != i {
			t.Error("Unexpected record for location", location, " i:", i, " off:", off)
			return
		}
	}

	testPackLocationPanic(t)

	for j = 0; j < MaxRecordValue; j++ {

		location := PackLocation(j, 1)
		recID := LocationRecord(location)

		if recID != j {
			t.Error("Unexpected record for location", location, " i:", i, " recId:", recID)
			return
		}

		off := LocationOffset(location)

		if off != 1 {
			t.Error("Unexpected record for location", location, " i:", i, " off:", off)
			return
		}
	}

	if PackLocation(0xFFFFFF, 0xFFFF) != 0xFFFFFFFF {
		t.Error("Unexpected max location")
	}
}

func testPackLocationPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Packing location with invalid record id.")
		}
	}()

	PackLocation(MaxRecordValue+1, 0)
}
