/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package graphstorage

import "testing"

func TestMemoryGraphStorage(t *testing.T) {
	mstore := NewMemoryGraphStorage("mytest")

	// Test nop functions

	mstore.FlushAll()
	mstore.RollbackMain()
	mstore.FlushMain()
	mstore.Close()

	if mstore.Name() != "mytest" {
		t.Error("Unexpected name:", mstore.Name())
	}

	mstore.MainDB()["test1"] = "testvalue1"
	if mstore.MainDB()["test1"] != "testvalue1" {
		t.Error("Unexpected name db value")
		return
	}

	if res := mstore.StorageManager("123", false); res != nil {
		t.Error("Unexpected result", res)
		return
	}

	res := mstore.StorageManager("123", true)
	if res == nil {
		t.Error("Unexpected result", res)
		return
	}

	loc, _ := res.Insert("test")

	sm2 := mstore.StorageManager("123", false)
	if res2, _ := sm2.FetchCached(loc); res2.(string) != "test" {
		t.Error("Unexpected result", res2)
		return
	}
}
