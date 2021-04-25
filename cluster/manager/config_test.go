/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package manager

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"devt.de/krotik/common/datautil"
)

const invalidFileName = "**" + "\x00"

func TestDefaultStateInfo(t *testing.T) {

	_, err := NewDefaultStateInfo(invalidFileName)
	if !strings.HasPrefix(err.Error(),
		"ClusterError: Cluster configuration error (Cannot create state info file") {
		t.Error("It should not be possible to create a state info with an invalid filename:", err)
		return
	}

	// Store some data

	fc, err := NewDefaultStateInfo("test_conf.cfg")
	defer func() {
		if err := os.RemoveAll("test_conf.cfg"); err != nil {
			t.Error(err)
		}
	}()

	// Store list of strings

	testList := make([]string, 5)
	testList[3] = "456"
	testList[4] = "111"

	fc.Put("data", testList)

	// Write to disk

	fc.Flush()

	if len(fc.Map()) != len(fc.(*DefaultStateInfo).Data) {
		t.Error("Unexpected result")
		return
	}

	// Load data again

	fc2, err := NewDefaultStateInfo("test_conf.cfg")

	// Check we can get the data back

	m, ok := fc2.Get("data")

	if !ok || fmt.Sprint(m) != fmt.Sprint(fc.(*DefaultStateInfo).Data["data"]) {
		t.Error("Should get back what is stored")
		return
	}

	pm, _ := datautil.NewPersistentMap(invalidFileName)
	fc2.(*DefaultStateInfo).PersistentMap = pm

	if err := fc2.Flush(); !strings.HasPrefix(err.Error(),
		"ClusterError: Cluster configuration error (Cannot persist state info") {
		t.Error("Unexpected error:", err)
		return
	}

	ioutil.WriteFile("test_conf.cfg", []byte{0x00, 0x00}, 0660)

	_, err = NewDefaultStateInfo("test_conf.cfg")
	if !strings.HasPrefix(err.Error(),
		"ClusterError: Cluster configuration error (Cannot load state info file test_conf.cfg") {
		t.Error(err)
	}
}

func TestMemStateInfo(t *testing.T) {
	msi := NewMemStateInfo()

	// Store list of strings

	testList := make([]string, 5)
	testList[3] = "456"
	testList[4] = "111"

	msi.Put("data", testList)

	// NOP

	msi.Flush()

	// Check we can get the data back

	m, ok := msi.Get("data")

	if !ok || fmt.Sprint(m) != fmt.Sprint(msi.(*MemStateInfo).data["data"]) {
		t.Error("Should get back what is stored")
		return
	}

	if len(msi.Map()) != len(msi.(*MemStateInfo).data) {
		t.Error("Unexpected result")
		return
	}
}

func TestErrors(t *testing.T) {

	// Test cluster error

	err := &Error{errors.New("test"), ""}

	if err.Error() != "ClusterError: test" {
		t.Error("Unexpected result:", err.Error())
		return
	}

	err = &Error{errors.New("test"), "testdetail"}

	if err.Error() != "ClusterError: test (testdetail)" {
		t.Error("Unexpected result:", err.Error())
		return
	}

}
