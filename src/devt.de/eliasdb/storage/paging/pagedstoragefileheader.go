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
PagedStorageFileHeader is a wrapper object for the header record of a StorageFile.
The header record stores information about linked lists and root values.
*/
package paging

import "devt.de/eliasdb/storage/file"

/*
Header magic number to identify page headers
*/
const PAGE_HEADER = 0x1980

/*
Number of lists which can be stored in this header
*/
const TOTAL_LISTS = 5

/*
Offset for list entries in this header
*/
const OFFSET_LISTS = 2

/*
Number of lists which can be stored in this header
*/
const OFFSET_ROOTS = OFFSET_LISTS + (2 * TOTAL_LISTS * file.SIZE_LONG)

/*
PagedStorageFileHeader data structure
*/
type PagedStorageFileHeader struct {
	record     *file.Record // Record which is being used for the header information
	totalRoots int          // Number of root values which can be stored
}

/*
Create a new NewPagedStorageFileHeader.
*/
func NewPagedStorageFileHeader(record *file.Record, isnew bool) *PagedStorageFileHeader {
	totalRoots := (len(record.Data()) - OFFSET_ROOTS) / file.SIZE_LONG
	if totalRoots < 1 {
		panic("Cannot store any roots - record is too small")
	}

	ret := &PagedStorageFileHeader{record, totalRoots}

	if isnew {
		record.WriteUInt16(0, PAGE_HEADER)
	} else {
		ret.CheckMagic()
	}

	return ret
}

/*
CheckMagic checks the header magic value of this header.
*/
func (psfh *PagedStorageFileHeader) CheckMagic() {
	if psfh.record.ReadUInt16(0) != PAGE_HEADER {
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
	return OFFSET_ROOTS + root*file.SIZE_LONG
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
	return OFFSET_LISTS + 2*file.SIZE_LONG*int(list)
}

/*
offsetLastListElement returns offset of the last element of a list.
*/
func offsetLastListElement(list int16) int {
	return offsetFirstListElement(list) + file.SIZE_LONG
}
