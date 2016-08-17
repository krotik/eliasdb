/* 
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

/*
Util class to pack sizes for physical slots and logical buckets. The size info is a 4 byte 
value which allocates 2 bytes for current size and 2 bytes for available size.

CCCC CCCC CCCC CCCC AAAA AAAA AAAA AAAA

The allocated size value is a packed integer using a 2 bit multiplier
in the beginning - using these packed values a slot can grow up to
138681822 bytes (138 MB). The space allocation becomes more and more
wasteful with increasing slot size. The current size is stored as a
difference to the allocated size. The maximum difference between
alloacted and current space is 65534 bytes.
*/
package util

import (
	"fmt"

	"devt.de/eliasdb/storage/file"
)

const OFFSET_CURRENT_SIZE = 0
const OFFSET_AVAILABLE_SIZE = file.SIZE_UNSIGNED_SHORT

const UNSIGNED_SHORT_MAX = 0xFFFF

/*
Max size of the difference between available size and current size
*/
const MAX_AVAILABLE_SIZE_DIFFERENCE = UNSIGNED_SHORT_MAX - 1

/*
Size of the size info
*/
const SIZE_INFO_SIZE = OFFSET_AVAILABLE_SIZE + file.SIZE_UNSIGNED_SHORT

/*
CurrentSize returns the current size of a slot.
*/
func CurrentSize(record *file.Record, offset int) uint32 {
	currentSize := record.ReadUInt16(offset + OFFSET_CURRENT_SIZE)
	if currentSize == UNSIGNED_SHORT_MAX {
		return 0
	}
	return AvailableSize(record, offset) - uint32(currentSize)
}

/*
SetCurrentSize sets the current size of a slot.
*/
func SetCurrentSize(record *file.Record, offset int, value uint32) {
	if value == 0 {
		record.WriteUInt16(offset+OFFSET_CURRENT_SIZE, UNSIGNED_SHORT_MAX)
		return
	}

	size := AvailableSize(record, offset)

	if (size > MAX_AVAILABLE_SIZE_DIFFERENCE &&
		value < size-MAX_AVAILABLE_SIZE_DIFFERENCE) ||
		value > size {

		panic(fmt.Sprint("Cannot store current size as difference "+
			"to available size. Value:", value, " Available size:", size))
	}

	record.WriteUInt16(offset+OFFSET_CURRENT_SIZE, uint16(size-value))
}

/*
AvailableSize returns the available size of a slot.
*/
func AvailableSize(record *file.Record, offset int) uint32 {
	value := record.ReadUInt16(offset + OFFSET_AVAILABLE_SIZE)
	return decodeSize(value)
}

/*
SetAvailableSize sets the available size of a slot.
*/
func SetAvailableSize(record *file.Record, offset int, value uint32) {
	currentSize := CurrentSize(record, offset)

	size := encodeSize(value)

	// Safeguard against not using normalized size values

	if decodeSize(size) != value {
		panic("Size value was not normalized")
	}

	record.WriteUInt16(offset+OFFSET_AVAILABLE_SIZE, size)

	// Current size needs to be updated since it depends on the available size

	SetCurrentSize(record, offset, currentSize)
}

/*
Normalize a given slot size.
*/
func NormalizeSlotSize(value uint32) uint32 {
	return decodeSize(encodeSize(value))
}

const sizeMask = 1<<15 | 1<<14

const multi0 = 1
const multi1 = 1 << 4
const multi2 = 1 << 8
const multi3 = 1 << 13

const base0 = 0
const base1 = base0 + multi0*((1<<14)-2)
const base2 = base1 + multi1*((1<<14)-2)
const base3 = base2 + multi2*((1<<14)-2)

/*
decodeSize decodes a given size value.
*/
func decodeSize(packedSize uint16) uint32 {
	size := packedSize & sizeMask

	multiplier := size >> 14
	counter := uint32(packedSize - size)

	switch multiplier {
	case 0:
		return counter * multi0
	case 1:
		return base1 + counter*multi1
	case 2:
		return base2 + counter*multi2
	default:
		return base3 + counter*multi3
	}
}

/*
encodeSize encodes a given size value.
*/
func encodeSize(size uint32) uint16 {
	var multiplier, counter, v uint32

	switch {

	case size <= base1:
		multiplier = 0
		counter = size / multi0

	case size < base2:

		multiplier = 1 << 14
		v = size - base1
		counter = v / multi1
		if v%multi1 != 0 {
			counter++
		}

	case size < base3:

		multiplier = 2 << 14
		v = size - base2

		counter = v / multi2
		if v%multi2 != 0 {
			counter++
		}

	default:

		multiplier = 3 << 14
		v = size - base3
		counter = v / multi3
		if v%multi3 != 0 {
			counter++
		}
	}

	if counter >= (1 << 14) {
		panic(fmt.Sprint("Cannot pack slot size:", size))
	}

	return uint16(multiplier + counter)
}
