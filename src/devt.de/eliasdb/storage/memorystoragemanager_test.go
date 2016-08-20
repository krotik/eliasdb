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

import (
	"testing"

	"devt.de/eliasdb/storage/file"
)

func TestMemoryStorageManager(t *testing.T) {
	var ret string

	msm := NewMemoryStorageManager("test")

	// Simple tests

	if msm.Name() != "test" {
		t.Error("Unexpected name")
		return
	}

	if err := msm.Fetch(5, &ret); err != ErrSlotNotFound {
		t.Error("Unexpected fetch result:", err)
		return
	}

	msm.SetRoot(5, 1)
	if msm.Root(5) != 1 {
		t.Error("Unexpected root", msm.Root(5))
		return
	}

	// Standard tests

	loc, _ := msm.Insert("MyString")

	msm.Fetch(loc, &ret)
	if ret != "MyString" {
		t.Error("Unexpected fetch result:", ret)
		return
	}

	if res, _ := msm.FetchCached(loc); res != "MyString" {
		t.Error("Unexpected fetchcached result:", res)
		return
	}

	msm.Update(loc, "MyOtherString")

	if res, _ := msm.FetchCached(loc); res != "MyOtherString" {
		t.Error("Unexpected fetchcached result:", res)
		return
	}

	if s := msm.String(); s != "MemoryStorageManager test\n"+
		"1 - MyOtherString\n" {
		t.Error("Unexpected string representation:", s)
	}

	msm.Free(loc)

	if res, _ := msm.FetchCached(loc); res != nil {
		t.Error("Unexpected fetchcached result:", res)
		return
	}

	// Error cases

	msm.AccessMap[loc] = AccessNotInCache

	if _, err := msm.FetchCached(loc); err != ErrNotInCache {
		t.Error("Unexpected fetchcached result:", err)
		return
	}

	msm.AccessMap[loc] = AccessCacheAndFetchSeriousError

	if _, err := msm.FetchCached(loc); err != file.ErrAlreadyInUse {
		t.Error("Unexpected fetchcached result:", err)
		return
	}

	if err := msm.Fetch(loc, &ret); err != file.ErrAlreadyInUse {
		t.Error("Unexpected fetch result:", err)
		return
	}

	msm.AccessMap[loc] = AccessFetchError

	if err := msm.Fetch(loc, &ret); err != ErrSlotNotFound {
		t.Error("Unexpected fetch result:", err)
		return
	}

	msm.AccessMap[loc] = AccessUpdateError

	if err := msm.Update(loc, ""); err != ErrSlotNotFound {
		t.Error("Unexpected update result:", err)
		return
	}

	msm.AccessMap[loc] = AccessFreeError

	if err := msm.Free(loc); err != ErrSlotNotFound {
		t.Error("Unexpected free result:", err)
		return
	}

	msm.AccessMap[msm.LocCount] = AccessInsertError

	if _, err := msm.Insert(""); err != file.ErrAlreadyInUse {
		t.Error("Unexpected insert result:", err)
		return
	}

	// Dummy calls

	msm.Flush()
	msm.Rollback()
	msm.Close()
}
