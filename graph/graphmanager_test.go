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

	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

/*
Flag to enable / disable tests which use actual disk storage.
(Only used for test development - should never be false)
*/
const RunDiskStorageTests = true

const GraphManagerTestDBDir1 = "gmtest1"
const GraphManagerTestDBDir2 = "gmtest2"
const GraphManagerTestDBDir3 = "gmtest3"
const GraphManagerTestDBDir4 = "gmtest4"
const GraphManagerTestDBDir5 = "gmtest5"
const GraphManagerTestDBDir6 = "gmtest6"

var DBDIRS = []string{GraphManagerTestDBDir1, GraphManagerTestDBDir2,
	GraphManagerTestDBDir3, GraphManagerTestDBDir4, GraphManagerTestDBDir5,
	GraphManagerTestDBDir6}

const InvlaidFileName = "**" + "\x00"

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
func newGraphManagerNoRules(gs graphstorage.Storage) *Manager {
	return createGraphManager(gs)
}
