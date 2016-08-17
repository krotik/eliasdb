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
TransPage is a page which holds data to translate between physical and logical
slots.
*/
package pageview

import (
	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging/view"
)

/*
Data offset for translation page
*/
const OFFSET_TRANS_DATA = view.OFFSET_DATA

/*
TransPage data structure
*/
type TransPage struct {
	*SlotInfoPage
}

/*
NewTransPage creates a new page which holds data to translate between physical
and logical slots.
*/
func NewTransPage(record *file.Record) *DataPage {
	checkTransPageMagic(record)
	return &DataPage{NewSlotInfoPage(record)}
}

/*
checkTransPageMagic checks if the magic number at the beginning of
the wrapped record is valid.
*/
func checkTransPageMagic(record *file.Record) bool {
	magic := record.ReadInt16(0)

	if magic == view.VIEW_PAGE_HEADER+view.TYPE_TRANSLATION_PAGE {
		return true
	}
	panic("Unexpected header found in TransPage")
}
