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
Util class to pack and unpack location information in a uint64. A location is a
pointer which identifies a specific record and within the record a specific
offset.

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
Size of a location value in bytes
*/
const LOCATION_SIZE = file.SIZE_LONG

/*
Maximum record value (2^48 / 2 - 1)

6 byte = 48 bits
*/
const MAX_RECORD_VALUE = 0xFFFFFF

/*
Maximum offset value (32767)
*/
const MAX_OFFSET_VALUE = 0xFFFF

/*
Get the record id from a location.
*/
func LocationRecord(location uint64) uint64 {
	return uint64(location >> 16)
}

/*
LocationOffset gets the offset from a location.
*/
func LocationOffset(location uint64) uint16 {
	return uint16(location & 0xffff)
}

/*
Pack location information into an uint64.
*/
func PackLocation(recordId uint64, offset uint16) uint64 {
	if offset == 0xFFFF && recordId == 0xFFFFFF {
		return 0xFFFFFFFF
	}

	if recordId > MAX_RECORD_VALUE {
		panic("Cannot create location with record id greater than 0xFFFFFF")
	}

	return (recordId << 16) + uint64(offset)
}
