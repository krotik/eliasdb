/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package cluster

import (
	"errors"
	"math"
	"testing"

	"devt.de/eliasdb/hash"
	"devt.de/eliasdb/storage"
)

func TestAddressTableClusterLoc(t *testing.T) {

	// Set a low distribution range

	defaultDistributionRange = 3
	defer func() { defaultDistributionRange = math.MaxUint64 }()

	// Test an inoperable cluster

	cluster1, ms1 := createCluster(1, 1)
	cluster1[0].distributionTable = nil
	cluster1[0].distributionTableError = errors.New("testerror")

	_, err := ms1[0].at.NewClusterLoc("test1")
	if err.Error() != "Storage is currently disabled on member: TestClusterMember-0 (testerror)" {
		t.Error("Unexpected result:", err)
		return
	}

	// Test normal number sequence

	cluster1, ms1 = createCluster(1, 1)

	loc, err := ms1[0].at.NewClusterLoc("test1")
	if loc != 0 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}
	ms1[0].at.SetTransClusterLoc("test1", loc, 123, 1)

	// Starting an unrelated counter should have no effect

	loc, err = ms1[0].at.NewClusterLoc("test2")
	if loc != 0 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	delete(ms1[0].at.newlocCounters, "test1")

	// Advance the counter

	loc, err = ms1[0].at.NewClusterLoc("test1")
	if loc != 1 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}
	ms1[0].at.SetTransClusterLoc("test1", loc, 123, 1)

	loc, err = ms1[0].at.NewClusterLoc("test1")
	if loc != 2 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	// Not filling the location 2

	loc, err = ms1[0].at.NewClusterLoc("test1")
	if loc != 3 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}
	ms1[0].at.SetTransClusterLoc("test1", loc, 123, 1)

	// The next call should find the free location 2

	loc, err = ms1[0].at.NewClusterLoc("test1")
	if loc != 2 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}
	ms1[0].at.SetTransClusterLoc("test1", loc, 123, 1)

	// Now we are full the next call should error

	loc, err = ms1[0].at.NewClusterLoc("test1")
	if err.Error() != "Could not find any free storage location on this member" {
		t.Error("Unexpected result:", loc, err)
		return
	}

	// Change distribution table - test member which is in the middle of the cluster

	defaultDistributionRange = 120
	dd, _ := NewDistributionTable([]string{"aa", cluster1[0].MemberManager.Name(), "bb"}, 1)
	cluster1[0].distributionTable = dd
	cluster1[0].distributionTableError = nil

	start, end := cluster1[0].distributionTable.MemberRange(cluster1[0].MemberManager.Name())
	if start != 40 || end != 79 {
		t.Error("Unexpected range:", start, end)
		return
	}

	loc, err = ms1[0].at.NewClusterLoc("test1")
	if loc != 40 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}
	ms1[0].at.SetTransClusterLoc("test1", loc, 123, 1)

	// Simulate a lookup failure

	for i := 40; i < 70; i++ {
		ms1[0].at.SetTransClusterLoc("test1", uint64(i), 123, 1)
	}

	// Get the translation location for cluster location 6

	_, loc, _ = ms1[0].at.translation.GetValueAndLocation(transKey("test1", 69))

	msm := ms1[0].at.sm.(*storage.MemoryStorageManager)
	msm.AccessMap[loc] = storage.AccessCacheAndFetchSeriousError

	loc, err = ms1[0].at.NewClusterLoc("test1")
	if err.Error() != "Record is already in-use (? - )" {
		t.Error("Unexpected result:", loc, err)
		return
	}

	delete(msm.AccessMap, loc)

	// Recreate the member address table

	ms1[0].at, err = newMemberAddressTable(cluster1[0], ms1[0].at.sm)
	if err != nil {
		t.Error("Could not recreate MemberAddressTable:", err)
		return
	}

	// Now check the translation lookup

	if tr, ok, err := ms1[0].at.TransClusterLoc("test1", 50); tr.loc != 123 || tr.ver != 1 || !ok || err != nil {
		t.Error("Unexpected translation:", tr, ok, err)
		return
	}

	if tr, ok, err := ms1[0].at.TransClusterLoc("test1", 150); tr != nil || ok || err != nil {
		t.Error("Unexpected translation:", tr, ok, err)
		return
	}

	if tr, ok, err := ms1[0].at.SetTransClusterLoc("test1", 50, 555, 2); tr.loc != 123 || tr.ver != 1 || !ok || err != nil {
		t.Error("Unexpected translation:", tr, ok, err)
		return
	}

	if tr, ok, err := ms1[0].at.TransClusterLoc("test1", 50); tr.loc != 555 || tr.ver != 2 || !ok || err != nil {
		t.Error("Unexpected translation:", tr, ok, err)
		return
	}

	if tr, ok, err := ms1[0].at.RemoveTransClusterLoc("test1", 50); tr.loc != 555 || tr.ver != 2 || !ok || err != nil {
		t.Error("Unexpected translation:", tr, ok, err)
		return
	}

	if tr, ok, err := ms1[0].at.TransClusterLoc("test1", 50); tr != nil || ok || err != nil {
		t.Error("Unexpected translation:", tr, ok, err)
		return
	}

	if tr, ok, err := ms1[0].at.RemoveTransClusterLoc("test1", 50); tr != nil || ok || err != nil {
		t.Error("Unexpected translation:", tr, ok, err)
		return
	}
}

func TestAddressTableTransfer(t *testing.T) {

	_, ms1 := createCluster(1, 1)

	// Test storing transfer requests

	ms1[0].at.AddTransferRequest([]string{"a,b"}, nil)
	ms1[0].at.AddTransferRequest([]string{"c,d"}, nil)
	ms1[0].at.AddTransferRequest([]string{"e,f"}, nil)

	counter := 0

	it := hash.NewHTreeIterator(ms1[0].at.transfer)
	for ; it.HasNext(); it.Next() {
		counter++
	}

	// Check that we have 3 entries

	if counter != 3 {
		t.Error("Unexpected counter value:", counter)
		return
	}
}
