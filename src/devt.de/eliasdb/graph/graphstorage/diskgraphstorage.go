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
Graph storage which stores its data on disk.
*/
package graphstorage

import (
	"fmt"
	"os"
	"strings"

	"devt.de/common/datautil"
	"devt.de/common/fileutil"
	"devt.de/eliasdb/graph/util"
	"devt.de/eliasdb/storage"
)

/*
Filename for name DB
*/
var FILENAME_NAME_DB = "names.pm"

/*
DiskStorageManager data structure
*/
type DiskGraphStorage struct {
	name            string                            // Name of the graph storage
	mainDB          *datautil.PersistentMap           // Database storing names
	storagemanagers map[string]storage.StorageManager // Map of StorageManagers
}

/*
NewDiskGraphStorage creates a new DiskGraphStorage instance.
*/
func NewDiskGraphStorage(name string) (GraphStorage, error) {

	dgs := &DiskGraphStorage{name, nil, make(map[string]storage.StorageManager)}

	// Load the graph storage if the storage directory already exists if not try to create it

	if res, _ := fileutil.PathExists(name); !res {
		if err := os.Mkdir(name, 0770); err != nil {
			return nil, &util.GraphError{util.ErrOpening, err.Error()}
		}

		// Create the graph storage files

		mainDB, err := datautil.NewPersistentMap(name + "/" + FILENAME_NAME_DB)
		if err != nil {
			return nil, &util.GraphError{util.ErrOpening, err.Error()}
		}

		dgs.mainDB = mainDB

	} else {

		// Load graph storage files

		mainDB, err := datautil.LoadPersistentMap(name + "/" + FILENAME_NAME_DB)
		if err != nil {
			return nil, &util.GraphError{util.ErrOpening, err.Error()}
		}

		dgs.mainDB = mainDB
	}

	return dgs, nil
}

/*
Name returns the name of the DiskGraphStorage instance.
*/
func (dgs *DiskGraphStorage) Name() string {
	return dgs.name
}

/*
MainDB returns the main database.
*/
func (dgs *DiskGraphStorage) MainDB() map[string]string {
	return dgs.mainDB.Data
}

/*
RollbackMain rollback the main database.
*/
func (dgs *DiskGraphStorage) RollbackMain() error {
	mainDB, err := datautil.LoadPersistentMap(dgs.name + "/" + FILENAME_NAME_DB)
	if err != nil {
		return &util.GraphError{util.ErrOpening, err.Error()}
	}
	
	dgs.mainDB = mainDB
	
	return nil
}

/*
FlushMain writes the main database to the storage.
*/
func (dgs *DiskGraphStorage) FlushMain() error {
	if err := dgs.mainDB.Flush(); err != nil {
		return &util.GraphError{util.ErrFlushing, err.Error()}
	}
	return nil
}

/*
StorageManager gets a storage manager with a certain name. A non-existing
StorageManager is not created automatically if the create flag is set to false.
*/
func (dgs *DiskGraphStorage) StorageManager(smname string, create bool) storage.StorageManager {

	sm, ok := dgs.storagemanagers[smname]

	filename := dgs.name + "/" + smname

	// Create storage manager object either if we may create or if the
	// database already exists

	if !ok && (create || storage.StorageFileExist(filename)) {
		dsm := storage.NewDiskStorageManager(dgs.name+"/"+smname, false, false, false)
		sm = storage.NewCachedDiskStorageManager(dsm, 100000)
		dgs.storagemanagers[smname] = sm
	}

	return sm
}

/*
Close closes the storage.
*/
func (dgs *DiskGraphStorage) Close() error {

	errors := make([]string, 0)

	err := dgs.mainDB.Flush()
	if err != nil {
		errors = append(errors, err.Error())
	}

	for _, sm := range dgs.storagemanagers {
		err := sm.Close()
		if err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		details := fmt.Sprint(dgs.name, " :", strings.Join(errors, "; "))

		return &util.GraphError{util.ErrClosing, details}
	}

	return nil
}
