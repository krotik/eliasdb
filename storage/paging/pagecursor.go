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
Package paging contains functions and constants necessary for paging of records.

	NOTE: Operations in this code are expected to either fail completely or succeed.
	Errors in the middle of an operation may leave the datastructures in an
	inconsistent state.

PageCursor

PageCursor is a pointer into a PagedStorageFile and can be used to traverse
a linked list of pages (see also PagedStorageFileHeader which stores the
entry points).

PagedStorageFile

PagedStorageFile is a wrapper object for a StorageFile which views the file
records as a linked list of pages.

PagedStorageFileHeader

PagedStorageFileHeader is a wrapper object for the header record of a StorageFile.
The header record stores information about linked lists and root values.
*/
package paging

/*
PageCursor data structure
*/
type PageCursor struct {
	psf     *PagedStorageFile // Pager to be used
	ptype   int16             // Page type which will be traversed
	current uint64            // Current page
}

/*
NewPageCursor creates a new cursor object which can be used to traverse a set of pages.
*/
func NewPageCursor(psf *PagedStorageFile, ptype int16, current uint64) *PageCursor {
	return &PageCursor{psf, ptype, current}
}

/*
Current gets the page this cursor currently points at.
*/
func (pc *PageCursor) Current() uint64 {
	return pc.current
}

/*
Next moves the PageCursor to the next page and returns it.
*/
func (pc *PageCursor) Next() (uint64, error) {
	var page uint64
	var err error

	if pc.current == 0 {
		page = pc.psf.First(pc.ptype)
	} else {
		page, err = pc.psf.Next(pc.current)

		if err != nil {
			return 0, err
		}
	}

	if page != 0 {
		pc.current = page
	}

	return page, nil
}

/*
Prev moves the PageCursor to the previous page and returns it.
*/
func (pc *PageCursor) Prev() (uint64, error) {
	if pc.current == 0 {
		return 0, nil
	}

	page, err := pc.psf.Prev(pc.current)

	if err != nil {
		return 0, err
	}

	if page != 0 {
		pc.current = page
	}

	return page, nil
}
