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

	for i = 0; i < MAX_OFFSET_VALUE; i++ {

		location := PackLocation(1, uint16(i))
		recId := LocationRecord(location)

		if recId != 1 {
			t.Error("Unexpected record for location", location, " i:", i, " recId:", recId)
			return
		}

		off := LocationOffset(location)

		if off != i {
			t.Error("Unexpected record for location", location, " i:", i, " off:", off)
			return
		}
	}

	testPackLocationPanic(t)

	for j = 0; j < MAX_RECORD_VALUE; j++ {

		location := PackLocation(j, 1)
		recId := LocationRecord(location)

		if recId != j {
			t.Error("Unexpected record for location", location, " i:", i, " recId:", recId)
			return
		}

		off := LocationOffset(location)

		if off != 1 {
			t.Error("Unexpected record for location", location, " i:", i, " off:", off)
			return
		}
	}

	if PackLocation(0xFFFFFF,0xFFFF) != 0xFFFFFFFF {
		t.Error("Unexpected max location")
	}
}

func testPackLocationPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Packing location with invalid record id.")
		}
	}()

	PackLocation(MAX_RECORD_VALUE+1, 0)
}
