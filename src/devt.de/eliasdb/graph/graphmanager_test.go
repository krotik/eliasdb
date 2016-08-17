/* 
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

package graph

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"devt.de/common/fileutil"
	"devt.de/eliasdb/graph/graphstorage"
)

/*
Flag to enable / disable tests which use actual disk storage.
(Only used for test development - should never be false)
*/
const RUN_DISK_STORAGE_TESTS = true

const GRAPHMANAGER_TEST_DBDIR1 = "gmtest1"
const GRAPHMANAGER_TEST_DBDIR2 = "gmtest2"
const GRAPHMANAGER_TEST_DBDIR3 = "gmtest3"
const GRAPHMANAGER_TEST_DBDIR4 = "gmtest4"

var DBDIRS = []string{GRAPHMANAGER_TEST_DBDIR1, GRAPHMANAGER_TEST_DBDIR2,
	GRAPHMANAGER_TEST_DBDIR3, GRAPHMANAGER_TEST_DBDIR4}

const INVALID_FILE_NAME = "**" + string(0x0)

// Main function for all tests in this package

func TestMain(m *testing.M) {
	flag.Parse()

	for _, dbdir := range DBDIRS {
		if res, _ := fileutil.PathExists(dbdir); res {
			if err := os.RemoveAll(dbdir); err != nil {
				fmt.Print("Could not remove test directory:", err.Error())
			}
		}
	}

	// Run the tests

	res := m.Run()

	// Teardown

	for _, dbdir := range DBDIRS {
		if res, _ := fileutil.PathExists(dbdir); res {
			if err := os.RemoveAll(dbdir); err != nil {
				fmt.Print("Could not remove test directory:", err.Error())
			}
		}
	}

	os.Exit(res)
}

/*
NewGraphManager returns a new GraphManager instance without loading rules.
*/
func newGraphManagerNoRules(gs graphstorage.GraphStorage) *GraphManager {
	return createGraphManager(gs)
}
