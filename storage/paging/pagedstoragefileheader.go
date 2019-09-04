/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package paging

import "devt.de/krotik/eliasdb/storage/file"

/*
PageHeader is the magic number to identify page headers
*/
const PageHeader = 0x1980

/*
TotalLists is the number of lists which can be stored in this header
*/
const TotalLists = 5

/*
OffsetLists is the offset for list entries in this header
*/
const OffsetLists = 2

/*
OffsetRoots is the number of lists which can be stored in this header
*/
const OffsetRoots = OffsetLists + (2 * TotalLists * file.SizeLong)

/*
PagedStorageFileHeader data structure
*/
type PagedStorageFileHeader struct {
	record     *file.Record // Record which is being used for the header information
	totalRoots int          // Number of root values which can be stored
}

/*
NewPagedStorageFileHeader creates a new NewPagedStorageFileHeader.
*/
func NewPagedStorageFileHeader(record *file.Record, isnew bool) *PagedStorageFileHeader {
	totalRoots := (len(record.Data()) - OffsetRoots) / file.SizeLong
	if totalRoots < 1 {
		panic("Cannot store any roots - record is too small")
	}

	ret := &PagedStorageFileHeader{record, totalRoots}

	if isnew {
		record.WriteUInt16(0, PageHeader)
	} else {
		ret.CheckMagic()
	}

	return ret
}

/*
CheckMagic checks the header magic value of this header.
*/
func (psfh *PagedStorageFileHeader) CheckMagic() {
	if psfh.record.ReadUInt16(0) != PageHeader {
		panic("Unexpected header found in PagedStorageFileHeader")
	}
}

/*
Roots returns the number of possible root values which can be set.
*/
func (psfh *PagedStorageFileHeader) Roots() int {
	return psfh.totalRoots
}

/*
Root returns a root value.
*/
func (psfh *PagedStorageFileHeader) Root(root int) uint64 {
	return psfh.record.ReadUInt64(offsetRoot(root))
}

/*
SetRoot sets a root value.
*/
func (psfh *PagedStorageFileHeader) SetRoot(root int, val uint64) {
	psfh.record.WriteUInt64(offsetRoot(root), val)
}

/*
offsetRoot calculates the offset of a root in the header record.
*/
func offsetRoot(root int) int {
	return OffsetRoots + root*file.SizeLong
}

/*
FirstListElement returns the first element of a list.
*/
func (psfh *PagedStorageFileHeader) FirstListElement(list int16) uint64 {
	return psfh.record.ReadUInt64(offsetFirstListElement(list))
}

/*
SetFirstListElement sets the first element of a list.
*/
func (psfh *PagedStorageFileHeader) SetFirstListElement(list int16, val uint64) {
	psfh.record.WriteUInt64(offsetFirstListElement(list), val)
}

/*
LastListElement returns the last element of a list.
*/
func (psfh *PagedStorageFileHeader) LastListElement(list int16) uint64 {
	return psfh.record.ReadUInt64(offsetLastListElement(list))
}

/*
SetLastListElement sets the last element of a list.
*/
func (psfh *PagedStorageFileHeader) SetLastListElement(list int16, val uint64) {
	psfh.record.WriteUInt64(offsetLastListElement(list), val)
}

/*
offsetFirstListElement returns offset of the first element of a list.
*/
func offsetFirstListElement(list int16) int {
	return OffsetLists + 2*file.SizeLong*int(list)
}

/*
offsetLastListElement returns offset of the last element of a list.
*/
func offsetLastListElement(list int16) int {
	return offsetFirstListElement(list) + file.SizeLong
}
