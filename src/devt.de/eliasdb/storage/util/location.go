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
Package util contains utility functions for slot headers.

This file contains utility functions to pack and unpack location information
in an uint64. A location is a pointer which identifies a specific record and
within the record a specific offset.

The 8 byte uint64 value is split into a 6 byte (48 bits) record address and
2 byte offset.

RRRR RRRR RRRR RRRR RRRR RRRR RRRR RRRR RRRR RRRR RRRR RRRR OOOO OOOO OOOO OOOO

We can address at maximum (having a record size of 32767 bytes):

(2^48 / 2 - 1) * 32767 = 4.61154528 * 10^18 which is around 4 exabyte

Considering a default page size of 4096 bytes we can address:

(2^48 / 2 - 1) * 4096 = 5.76460752 * 10^17 which is around 512 petabyte
*/
package util

import "devt.de/eliasdb/storage/file"

/*
LocationSize is the size of a location in bytes
*/
const LocationSize = file.SizeLong

/*
MaxRecordValue is the maximum record value (2^48 / 2 - 1)

6 byte = 48 bits
*/
const MaxRecordValue = 0xFFFFFF

/*
MaxOffsetValue is the maximum offset value for a location (32767).
*/
const MaxOffsetValue = 0xFFFF

/*
LocationRecord retirms the record id from a location.
*/
func LocationRecord(location uint64) uint64 {
	return uint64(location >> 16)
}

/*
LocationOffset returns the offset from a location.
*/
func LocationOffset(location uint64) uint16 {
	return uint16(location & 0xffff)
}

/*
PackLocation packs location information into an uint64.
*/
func PackLocation(recordID uint64, offset uint16) uint64 {
	if offset == 0xFFFF && recordID == 0xFFFFFF {
		return 0xFFFFFFFF
	}

	if recordID > MaxRecordValue {
		panic("Cannot create location with record id greater than 0xFFFFFF")
	}

	return (recordID << 16) + uint64(offset)
}
