/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain. 
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package datautil

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"devt.de/common/fileutil"
)

const DBDIR = "test"

const INVALID_FILE_NAME = "**" + string(0x0)

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup
	if res, _ := fileutil.PathExists(DBDIR); res {
		os.RemoveAll(DBDIR)
	}

	err := os.Mkdir(DBDIR, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests
	res := m.Run()

	// Teardown
	err = os.RemoveAll(DBDIR)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)

}

func TestPersistentMap(t *testing.T) {

	// Test main scenario

	pm, err := NewPersistentMap(DBDIR + "/testmap.map")
	if err != nil {
		t.Error(nil)
		return
	}

	pm.Data["test1"] = "test1data"
	pm.Data["test2"] = "test2data"

	pm.Flush()

	pm2, err := LoadPersistentMap(DBDIR + "/testmap.map")

	if len(pm2.Data) != 2 {
		t.Error("Unexpected size of map")
		return
	}

	if pm.Data["test1"] != "test1data" || pm.Data["test2"] != "test2data" {
		t.Error("Unexpected data in map:", pm.Data)
		return
	}

	// Test error cases

	pm, err = NewPersistentMap(INVALID_FILE_NAME)
	if err == nil {
		t.Error("Unexpected result of new map")
		return
	}

	pm, err = LoadPersistentMap(INVALID_FILE_NAME)
	if err == nil {
		t.Error("Unexpected result of new map")
		return
	}

	pm = &PersistentMap{INVALID_FILE_NAME, make(map[string]string)}
	if err := pm.Flush(); err == nil {
		t.Error("Unexpected result of new map")
		return
	}
}
