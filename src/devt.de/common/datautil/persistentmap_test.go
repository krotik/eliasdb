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

const testdbdir = "test"

const invalidFileName = "**" + string(0x0)

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup
	if res, _ := fileutil.PathExists(testdbdir); res {
		os.RemoveAll(testdbdir)
	}

	err := os.Mkdir(testdbdir, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests
	res := m.Run()

	// Teardown
	err = os.RemoveAll(testdbdir)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)

}

func TestPersistentMap(t *testing.T) {

	// Test main scenario

	pm, err := NewPersistentMap(testdbdir + "/testmap.map")
	if err != nil {
		t.Error(nil)
		return
	}

	pm.Data["test1"] = "test1data"
	pm.Data["test2"] = "test2data"

	pm.Flush()

	pm2, err := LoadPersistentMap(testdbdir + "/testmap.map")

	if len(pm2.Data) != 2 {
		t.Error("Unexpected size of map")
		return
	}

	if pm.Data["test1"] != "test1data" || pm.Data["test2"] != "test2data" {
		t.Error("Unexpected data in map:", pm.Data)
		return
	}

	// Test error cases

	pm, err = NewPersistentMap(invalidFileName)
	if err == nil {
		t.Error("Unexpected result of new map")
		return
	}

	pm, err = LoadPersistentMap(invalidFileName)
	if err == nil {
		t.Error("Unexpected result of new map")
		return
	}

	pm = &PersistentMap{invalidFileName, make(map[string]interface{})}
	if err := pm.Flush(); err == nil {
		t.Error("Unexpected result of new map")
		return
	}
}

func TestPersistentStringMap(t *testing.T) {

	// Test main scenario

	pm, err := NewPersistentStringMap(testdbdir + "/teststringmap.map")
	if err != nil {
		t.Error(nil)
		return
	}

	pm.Data["test1"] = "test1data"
	pm.Data["test2"] = "test2data"

	pm.Flush()

	pm2, err := LoadPersistentStringMap(testdbdir + "/teststringmap.map")

	if len(pm2.Data) != 2 {
		t.Error("Unexpected size of map")
		return
	}

	if pm.Data["test1"] != "test1data" || pm.Data["test2"] != "test2data" {
		t.Error("Unexpected data in map:", pm.Data)
		return
	}

	// Test error cases

	pm, err = NewPersistentStringMap(invalidFileName)
	if err == nil {
		t.Error("Unexpected result of new map")
		return
	}

	pm, err = LoadPersistentStringMap(invalidFileName)
	if err == nil {
		t.Error("Unexpected result of new map")
		return
	}

	pm = &PersistentStringMap{invalidFileName, make(map[string]string)}
	if err := pm.Flush(); err == nil {
		t.Error("Unexpected result of new map")
		return
	}
}
