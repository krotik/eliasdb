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

import (
	"testing"

	"devt.de/krotik/eliasdb/storage/file"
)

func TestSlotSize(t *testing.T) {
	var i, mxSlotSize uint32

	mxSlotSize = decodeSize(UnsignedShortMax)

	r := file.NewRecord(123, make([]byte, 20))

	if AvailableSize(r, 2) != 0 {
		t.Error("Unexpected size")
		return
	}

	if CurrentSize(r, 2) != 0 {
		t.Error("Unexpected initial size")
		return
	}

	for i = 0; i < mxSlotSize; i = i + 1000 {
		round := NormalizeSlotSize(i)

		if uint32(round) < i {
			t.Error("Normalized size ", round, " is smaller than actual size:", i)
			return
		}

		if i < 17000 { // Up to 17 kilobyte
			// the rouding result should be exact.
			if round-i != 0 {
				t.Error("Unexpected rounding result.")
				return
			}
		} else if i < 278495 { // Up to 271.966797 kilobyte
			// we are no more then 15 bytes off
			if round-i >= 16 {
				t.Error("Unexpected rounding result.")
				return
			}
			testNormalizationPanic(t, r, i)
		} else if i < 4472287 { // Up to 4.26510429 megabyte
			// we are no more then 255 bytes off
			if round-i >= 256 {
				t.Error("Unexpected rounding result.")
				return
			}
			testNormalizationPanic(t, r, i)
		} else { // In all other cases
			// we are never more then 8191 bytes off
			if round-i >= 8192 {
				t.Error("Unexpected rounding result.")
				return
			}
			testNormalizationPanic(t, r, i)
		}

		SetAvailableSize(r, 2, round)

		if r.ReadSingleByte(1) != 0 {
			t.Error("Unexpected record data on byte 1:", r)
			return
		}
		if r.ReadSingleByte(6) != 0 {
			t.Error("Unexpected record data on byte 6:", r)
			return
		}
	}

	if r.ReadUInt16(2) != 0xFFFF {
		t.Error("Unexpected written size value")
		return
	}

	testNormalizationPanic(t, r, mxSlotSize+1)

	SetCurrentSize(r, 2, mxSlotSize-MaxAvailableSizeDifference)

	if CurrentSize(r, 2) != mxSlotSize-MaxAvailableSizeDifference {
		t.Error("Unexpected current size on extreme values")
	}

	r = file.NewRecord(123, make([]byte, 20))

	// Test a growing slot

	SetAvailableSize(r, 2, 100)

	SetCurrentSize(r, 2, 1)
	SetCurrentSize(r, 2, 10)
	SetCurrentSize(r, 2, 100)

	// Test panic if the slot grows too much

	testCurrentSizePanic(t, r, 101)
}

func testNormalizationPanic(t *testing.T, record *file.Record, i uint32) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Using not normalized sized values should cause a panic.")
		}
	}()
	SetAvailableSize(record, 2, i)
}

func testCurrentSizePanic(t *testing.T, record *file.Record, i uint32) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Using not normalized sized values should cause a panic.")
		}
	}()
	SetCurrentSize(record, 2, i)
}
