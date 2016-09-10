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

import (
	"errors"

	"devt.de/eliasdb/storage/file"
	"devt.de/eliasdb/storage/paging/view"
)

/*
Common paged storage file related errors
*/
var (
	ErrFreePage = errors.New("Cannot allocate/free a free page")
	ErrHeader   = errors.New("Cannot modify header record")
)

/*
PagedStorageFile data structure
*/
type PagedStorageFile struct {
	storagefile *file.StorageFile       // StorageFile which is wrapped
	header      *PagedStorageFileHeader // Header object
}

/*
NewPagedStorageFile wraps a given StorageFile and returns a PagedStorageFile.
*/
func NewPagedStorageFile(storagefile *file.StorageFile) (*PagedStorageFile, error) {
	var header *PagedStorageFileHeader

	record, err := storagefile.Get(0)
	if err != nil {
		return nil, err
	}

	// Check if this is a new header record or not

	isnew := record.ReadInt16(0) == 0

	header = NewPagedStorageFileHeader(record, isnew)

	return &PagedStorageFile{storagefile, header}, nil
}

/*
StorageFile returns the wrapped StorageFile.
*/
func (psf *PagedStorageFile) StorageFile() *file.StorageFile {
	return psf.storagefile
}

/*
Header returns the header object of this PagedStorageFile.
*/
func (psf *PagedStorageFile) Header() *PagedStorageFileHeader {
	return psf.header
}

/*
AllocatePage allocates a new page of a specific type.
*/
func (psf *PagedStorageFile) AllocatePage(pagetype int16) (uint64, error) {

	var record *file.Record
	var err error

	if pagetype == view.TypeFreePage {
		return 0, ErrFreePage
	}

	// Check first the free list

	ptr := psf.header.FirstListElement(view.TypeFreePage)
	isnew := ptr == 0

	if !isnew {

		// If there is something on the free list set the pointer
		// for the first item to the second item. The first item
		// becomes our newly allocated element.

		nextptr, err := psf.Next(ptr)
		if err != nil {
			return 0, err
		}

		// Get the record - error checking already done in the
		// previous psf.Next call

		record, _ = psf.storagefile.Get(ptr)

		psf.header.SetFirstListElement(view.TypeFreePage, nextptr)

	} else {

		// Need to create a new rcord

		ptr = psf.header.LastListElement(view.TypeFreePage)
		if ptr == 0 {
			// If the file is new the first pointer is 1
			ptr = 1
		}

		// Get the record - if it fails we need to return before
		// increasing the last list element pointer

		record, err = psf.storagefile.Get(ptr)
		if err != nil {
			return 0, err
		}

		// The last list element pointer is used to point to the next free record
		// it is not actuallz the last element of the list.

		psf.header.SetLastListElement(view.TypeFreePage, ptr+1)
	}

	// Set the view data on the record

	var pageview *view.PageView

	// Add a temp. page view so we can modify the record

	if isnew {
		pageview = view.NewPageView(record, pagetype)
	} else {
		pageview = view.GetPageView(record)
	}

	oldtail := psf.header.LastListElement(pagetype)

	record.ClearData()

	pageview.SetType(pagetype)
	pageview.SetPrevPage(oldtail)
	pageview.SetNextPage(0)

	// Check if this page was the first of its type

	if oldtail == 0 {
		psf.header.SetFirstListElement(pagetype, ptr)
	}

	// New allocated record is now the new last element

	psf.header.SetLastListElement(pagetype, ptr)

	// We can release the record now

	psf.storagefile.ReleaseInUse(record)

	// Need to fix up the pointer of the former previous element

	if oldtail != 0 {
		record, err = psf.storagefile.Get(oldtail)
		if err != nil {
			return 0, err
		}
		pageview = view.GetPageView(record)
		pageview.SetNextPage(ptr)
		psf.storagefile.ReleaseInUse(record)
	}

	// Remove temp. page view

	record.SetPageView(nil)

	return ptr, nil
}

/*
FreePage frees a given page and adds it to the free list.
*/
func (psf *PagedStorageFile) FreePage(id uint64) error {

	if id == 0 {
		return ErrHeader
	}

	record, err := psf.storagefile.Get(id)
	if err != nil {
		return err
	}

	pageview := view.GetPageView(record)
	pagetype := pageview.Type()

	if pagetype == view.TypeFreePage {
		psf.storagefile.ReleaseInUse(record)
		return ErrFreePage
	}

	prev := pageview.PrevPage()
	next := pageview.NextPage()

	// Put the page to the front of the free list
	pageview.SetType(view.TypeFreePage)
	pageview.SetNextPage(psf.header.FirstListElement(view.TypeFreePage))
	pageview.SetPrevPage(0)
	psf.header.SetFirstListElement(view.TypeFreePage, id)

	// NOTE The prev pointers will always point to 0 for records in the
	// free list. There is no need to update them.

	psf.storagefile.ReleaseInUse(record)

	// Remove page from its old list - an error in the below leaves
	// the lists in an inconsistent state.

	if prev != 0 {
		record, err = psf.storagefile.Get(prev)
		if err != nil {
			return err
		}
		pageview := view.GetPageView(record)
		pageview.SetNextPage(next)
		psf.storagefile.ReleaseInUse(record)
	} else {
		psf.header.SetFirstListElement(pagetype, next)
	}

	if next != 0 {
		record, err = psf.storagefile.Get(next)
		if err != nil {
			return err
		}
		pageview := view.GetPageView(record)
		pageview.SetPrevPage(prev)
		psf.storagefile.ReleaseInUse(record)
	} else {
		psf.header.SetLastListElement(pagetype, prev)
	}

	return nil
}

/*
First returns the first page of a list of a given type.
*/
func (psf *PagedStorageFile) First(pagetype int16) uint64 {
	return psf.header.FirstListElement(pagetype)
}

/*
Last returns the first page of a list of a given type.
*/
func (psf *PagedStorageFile) Last(pagetype int16) uint64 {
	return psf.header.LastListElement(pagetype)
}

/*
Next returns the next page of a given page in a list.
*/
func (psf *PagedStorageFile) Next(id uint64) (uint64, error) {
	record, err := psf.storagefile.Get(id)
	if err != nil {
		return 0, err
	}
	defer psf.storagefile.ReleaseInUse(record)

	pageview := view.GetPageView(record)

	return pageview.NextPage(), nil
}

/*
Prev returns the previous page of a given page in a list.
*/
func (psf *PagedStorageFile) Prev(id uint64) (uint64, error) {
	record, err := psf.storagefile.Get(id)
	if err != nil {
		return 0, err
	}
	defer psf.storagefile.ReleaseInUse(record)

	pageview := view.GetPageView(record)

	return pageview.PrevPage(), nil
}

/*
Flush writes all pending data to disk.
*/
func (psf *PagedStorageFile) Flush() error {
	psf.storagefile.ReleaseInUse(psf.header.record)

	if err := psf.storagefile.Flush(); err != nil {

		// If an error happens try to recover by putting
		// the header record back in use

		psf.storagefile.Get(0)

		return err
	}

	// No particular error checking for Get operation as
	// it should succeed if the previous Flush was successful.

	record, _ := psf.storagefile.Get(0)
	psf.header = NewPagedStorageFileHeader(record, false)

	return nil
}

/*
Rollback discards all changes which were done after the last flush.
The PageStorageFile object should be discarded if something
goes wrong during a rollback operation.
*/
func (psf *PagedStorageFile) Rollback() error {
	psf.storagefile.Discard(psf.header.record)

	if err := psf.storagefile.Rollback(); err != nil {

		// If there is a problem try to get the header record back
		// otherwise close operations may fail later

		psf.header.record, _ = psf.storagefile.Get(0)

		return err
	}

	// No particular error checking for Get operation as
	// it should succeed if the previous Rollback was successful.

	record, _ := psf.storagefile.Get(0)
	psf.header = NewPagedStorageFileHeader(record, record.ReadInt16(0) == 0)

	return nil
}

/*
Close commits all data and closes all physical files.
*/
func (psf *PagedStorageFile) Close() error {

	if psf.header != nil {
		psf.storagefile.ReleaseInUse(psf.header.record)
		psf.header = nil
	}

	if err := psf.storagefile.Close(); err != nil {
		return err
	}

	psf.storagefile = nil

	return nil
}
