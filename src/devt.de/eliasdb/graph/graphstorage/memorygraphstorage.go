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
Package graphstorage contains classes which model storage objects for graph data.

Graph storage which stores its data in memory only.
*/
package graphstorage

import "devt.de/eliasdb/storage"

/*
MgsRetClose is the return value on successful close
*/
var MgsRetClose error

/*
MgsRetFlushMain is the return value on successful flush
*/
var MgsRetFlushMain error

/*
MgsRetRollbackMain is the return value on successful rollback
*/
var MgsRetRollbackMain error

/*
MemoryGraphStorage data structure
*/
type MemoryGraphStorage struct {
	name            string                     // Name of the graph storage
	mainDB          map[string]string          // Database storing names
	storagemanagers map[string]storage.Manager // Map of StorageManagers
}

/*
NewMemoryGraphStorage creates a new MemoryGraphStorage instance.
*/
func NewMemoryGraphStorage(name string) GraphStorage {
	return &MemoryGraphStorage{name, make(map[string]string),
		make(map[string]storage.Manager)}
}

/*
Name returns the name of the MemoryGraphStorage instance.
*/
func (mgs *MemoryGraphStorage) Name() string {
	return mgs.name
}

/*
MainDB returns the main database.
*/
func (mgs *MemoryGraphStorage) MainDB() map[string]string {
	return mgs.mainDB
}

/*
RollbackMain rollback th/e main database.
*/
func (mgs *MemoryGraphStorage) RollbackMain() error {
	return MgsRetRollbackMain
}

/*
FlushMain writes the main database to the storage.
*/
func (mgs *MemoryGraphStorage) FlushMain() error {
	return MgsRetFlushMain
}

/*
StorageManager gets a storage manager with a certain name. A non-existing
StorageManager is not created automatically if the create flag is set to false.
*/
func (mgs *MemoryGraphStorage) StorageManager(smname string, create bool) storage.Manager {

	sm, ok := mgs.storagemanagers[smname]

	if !ok && create {
		sm = storage.NewMemoryStorageManager(mgs.name + "/" + smname)
		mgs.storagemanagers[smname] = sm
	}

	return sm
}

/*
Close closes the storage.
*/
func (mgs *MemoryGraphStorage) Close() error {
	return MgsRetClose
}
