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
A graph storage provides the storage mechanism for the graph manager.
*/
package graphstorage

import "devt.de/eliasdb/storage"

type GraphStorage interface {

	/*
	   Name returns the name of the GraphStorage instance.
	*/
	Name() string

	/*
		MainDB returns the main database. The main database is a quick
		lookup map for meta data which is always kept in memory.
	*/
	MainDB() map[string]string

	/*
	   RollbackMain rollback the main database.
	*/
	RollbackMain() error

	/*
	   FlushMain writes the main database to the storage.
	*/
	FlushMain() error

	/*
	   StorageManager gets a storage manager with a certain name. A non-existing
	   StorageManager is not created automatically if the create flag is set to false.
	*/
	StorageManager(smname string, create bool) storage.StorageManager

	/*
		Close closes the storage.
	*/
	Close() error
}
