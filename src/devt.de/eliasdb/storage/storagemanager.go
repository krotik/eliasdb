/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package storage

/*
RootIDVersion is the root id holding the version.
*/
const RootIDVersion = 1

/*
Manager describes an abstract storage manager.
*/
type Manager interface {

	/*
	   Name returns the name of the StorageManager instance.
	*/
	Name() string

	/*
		Root returns a root value.
	*/
	Root(root int) uint64

	/*
		SetRoot writes a root value.
	*/
	SetRoot(root int, val uint64)

	/*
	   Insert inserts an object and return its storage location.
	*/
	Insert(o interface{}) (uint64, error)

	/*
	   Update updates a storage location.
	*/
	Update(loc uint64, o interface{}) error

	/*
		Free frees a storage location.
	*/
	Free(loc uint64) error

	/*
		Fetch fetches an object from a given storage location and writes it to
		a given data container.
	*/
	Fetch(loc uint64, o interface{}) error

	/*
		FetchCached fetches an object from a cache and returns its reference.
		Returns a storage.ErrNotInCache error if the entry is not in the cache.
	*/
	FetchCached(loc uint64) (interface{}, error)

	/*
	   Flush writes all pending changes to disk.
	*/
	Flush() error

	/*
		Rollback cancels all pending changes which have not yet been written to disk.
	*/
	Rollback() error

	/*
		Close the StorageManager and write all pending changes to disk.
	*/
	Close() error
}
