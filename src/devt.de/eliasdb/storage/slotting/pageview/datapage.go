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
DataPage is a page which holds actual data.
*/
package pageview

import (
	"fmt"

	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging/view"
)

/*
Pointer to first element on the page
*/
const OFFSET_FIRST = view.OFFSET_DATA

// OFFSET_DATA declared in freephysicalslotpage

/*
DataPage data structure
*/
type DataPage struct {
	*SlotInfoPage
}

/*
NewDataPage creates a new page which holds actual data.
*/
func NewDataPage(record *file.Record) *DataPage {
	checkDataPageMagic(record)
	dp := &DataPage{NewSlotInfoPage(record)}
	return dp
}

/*
checkDataPageMagic checks if the magic number at the beginning of
the wrapped record is valid.
*/
func checkDataPageMagic(record *file.Record) bool {
	magic := record.ReadInt16(0)

	if magic == view.VIEW_PAGE_HEADER+view.TYPE_DATA_PAGE {
		return true
	}
	panic("Unexpected header found in DataPage")
}

/*
DataSpace returns the available data space on this page.
*/
func (dp *DataPage) DataSpace() uint16 {
	return uint16(len(dp.Record.Data()) - OFFSET_DATA)
}

/*
OffsetFirst returns the pointer to the first element on the page.
*/
func (dp *DataPage) OffsetFirst() uint16 {
	return dp.Record.ReadUInt16(OFFSET_FIRST)
}

/*
SetOffsetFirst sets the pointer to the first element on the page.
*/
func (dp *DataPage) SetOffsetFirst(first uint16) {
	if first > 0 && first < OFFSET_DATA {
		panic(fmt.Sprint("Cannot set offset of first element on DataPage below ", OFFSET_DATA))
	}
	dp.Record.WriteUInt16(OFFSET_FIRST, first)
}
