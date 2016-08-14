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
PageView is the super-struct for all page views. A page view is special object
attached to a particular StorageFile record. A view provides specialised 
functions for the record it is attached to.

Use GetPageView if the record has already view information stored on it or
NewPageView to initialise or reassign a given record.
*/
package view

import (
	"fmt"

	"devt.de/eliasdb/storage/file"
)

/*
Header magic number to identify view pages
*/
const VIEW_PAGE_HEADER = 0x1990

/*
Offset for next page id
*/
const OFFSET_NEXT_PAGE = file.SIZE_SHORT

/*
Offset for previous page id
*/
const OFFSET_PREV_PAGE = OFFSET_NEXT_PAGE + file.SIZE_LONG

/*
Offset for page specific data
*/
const OFFSET_DATA = OFFSET_PREV_PAGE + file.SIZE_LONG

/*
PageView data structure
*/
type PageView struct {
	Record *file.Record // Record which is wrapped by the PageView
}

/*
Return the page view of a given record.
*/
func GetPageView(record *file.Record) *PageView {
	rpv := record.PageView()

	pv, ok := rpv.(*PageView)
	if ok {
		return pv
	}

	pv = &PageView{record}
	pv.checkMagic()
	record.SetPageView(pv)

	return pv
}

/*
Create a new page view for a given record.
*/
func NewPageView(record *file.Record, pagetype int16) *PageView {
	pv := &PageView{record}
	record.SetPageView(pv)
	pv.SetType(pagetype)
	return pv
}

/*
Type gets the type of this page view which is stored on the record.
*/
func (pv *PageView) Type() int16 {
	return pv.Record.ReadInt16(0) - VIEW_PAGE_HEADER
}

/*
SetType sets the type of this page view which is stored on the record.
*/
func (pv *PageView) SetType(pagetype int16) {
	pv.Record.WriteInt16(0, VIEW_PAGE_HEADER+pagetype)
}

/*
checkMagic checks if the magic number at the beginning of the wrapped record
is valid.
*/
func (pv *PageView) checkMagic() bool {
	magic := pv.Record.ReadInt16(0)

	if magic >= VIEW_PAGE_HEADER &&
		magic <= VIEW_PAGE_HEADER+TYPE_FREE_PHYSICAL_SLOT_PAGE {
		return true
	}
	panic("Unexpected header found in PageView")
}

/*
NextPage returns the id of the next page.
*/
func (pv *PageView) NextPage() uint64 {
	pv.checkMagic()
	return pv.Record.ReadUInt64(OFFSET_NEXT_PAGE)
}

/*
SetNextPage sets the id of the next page.
*/
func (pv *PageView) SetNextPage(val uint64) {
	pv.checkMagic()
	pv.Record.WriteUInt64(OFFSET_NEXT_PAGE, val)
}

/*
PrevPage returns the id of the previous page.
*/
func (pv *PageView) PrevPage() uint64 {
	pv.checkMagic()
	return pv.Record.ReadUInt64(OFFSET_PREV_PAGE)
}

/*
SetPrevPage sets the id of the previous page.
*/
func (pv *PageView) SetPrevPage(val uint64) {
	pv.checkMagic()
	pv.Record.WriteUInt64(OFFSET_PREV_PAGE, val)
}

func (pv *PageView) String() string {
	return fmt.Sprintf("PageView: %v (type:%v previous page:%v next page:%v)",
		pv.Record.Id(), pv.Type(), pv.PrevPage(), pv.NextPage())
}
