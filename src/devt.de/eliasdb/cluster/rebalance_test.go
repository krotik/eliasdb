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
	"bytes"
	"encoding/gob"
	"math"
	"testing"
	"time"

	"devt.de/eliasdb/cluster/manager"
)

func TestRebalancing(t *testing.T) {

	// Set a low distribution range

	defaultDistributionRange = 10
	defer func() { defaultDistributionRange = math.MaxUint64 }()

	// Setup a cluster

	manager.FreqHousekeeping = 5
	defer func() { manager.FreqHousekeeping = 1000 }()

	// Log transfer worker runs

	logTransferWorker = true
	defer func() { logTransferWorker = false }()

	// Log rebalance worker runs

	logRebalanceWorker = true
	defer func() { logRebalanceWorker = false }()

	// Create a cluster with 3 members and a replication factor of 2

	cluster3, ms := createCluster(4, 2)

	// Debug output

	//manager.LogDebug = manager.LogInfo
	//log.SetOutput(os.Stderr)
	//defer func() { log.SetOutput(ioutil.Discard) }()

	// At first join up only 3 members

	for i, dd := range cluster3[:3] {
		dd.Start()
		defer dd.Close()

		if i > 0 {
			err := dd.MemberManager.JoinCluster(cluster3[0].MemberManager.Name(),
				cluster3[0].MemberManager.NetAddr())
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	sm := cluster3[1].StorageManager("test", true)

	// Insert two strings into the store

	if loc, err := sm.Insert("test1"); loc != 0 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	sm.Flush()

	time.Sleep(10 * time.Millisecond)

	if loc, err := sm.Insert("test2"); loc != 3 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	sm.Flush()

	time.Sleep(10 * time.Millisecond)

	if loc, err := sm.Insert("test3"); loc != 6 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	sm.Flush()

	// Ensure the transfer worker is running on all members

	for _, m := range ms {
		m.transferWorker()
		for m.transferRunning {
			time.Sleep(time.Millisecond)
		}
	}

	// Check that we have a certain storage layout in the cluster

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 6 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 6 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Join the 4th member

	cluster3[3].Start()
	defer cluster3[3].Close()

	err := cluster3[3].MemberManager.JoinCluster(cluster3[0].MemberManager.Name(),
		cluster3[0].MemberManager.NetAddr())
	if err != nil {
		t.Error(err)
		return
	}

	// Switch off rebalance for now

	runRebalanceWorker = false
	for ms[3].rebalanceRunning {
		time.Sleep(time.Millisecond)
	}

	if loc, err := sm.Insert("test4"); loc != 7 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	// Ensure the transfer worker is running on all members

	for _, m := range ms {
		m.transferWorker()
		for m.transferRunning {
			time.Sleep(time.Millisecond)
		}
	}

	// Check that we have a certain storage layout in the cluster

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 6 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
cloc: 7 (v:1) - lloc: 3 - "\b\f\x00\x05test4"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 6 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 7 (v:1) - lloc: 1 - "\b\f\x00\x05test4"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Check distribution table

	if dt, _ := cluster3[2].DistributionTable(); dt.String() != `
Location ranges:
TestClusterMember-0: 0 -> 1
TestClusterMember-1: 2 -> 3
TestClusterMember-2: 4 -> 5
TestClusterMember-3: 6 -> 10
Replicas (factor=2) :
TestClusterMember-0: [TestClusterMember-1]
TestClusterMember-1: [TestClusterMember-2]
TestClusterMember-2: [TestClusterMember-3]
TestClusterMember-3: [TestClusterMember-0]
`[1:] {
		t.Error("Unexpected distribution table: ", dt.String())
		return
	}

	// Switch on rebalancing

	runRebalanceWorker = true

	// Ensure the rebalance worker is running on all members

	for _, m := range ms {
		m.rebalanceWorker(true)
		for m.rebalanceRunning {
			time.Sleep(time.Millisecond)
		}
	}

	// Check that the rebalancing was successful

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 6 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
cloc: 7 (v:1) - lloc: 3 - "\b\f\x00\x05test4"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 7 (v:1) - lloc: 1 - "\b\f\x00\x05test4"
cloc: 6 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Update a piece of data on a replica. This simulates the situation when a
	// primary storage member was down and an update was only received on a replica.

	var bb bytes.Buffer
	if err := gob.NewEncoder(&bb).Encode("test3_updated"); err != nil {
		t.Error(err)
		return
	}

	localsm := ms[0].gs.StorageManager(LocalStoragePrefix+"test", false)
	localsm.Update(2, bb.Bytes())
	ms[0].at.SetTransClusterLoc("test", 6, 2, 2)

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 6 (v:2) - lloc: 2 - "\x10\f\x00\rtest3_updated"
cloc: 7 (v:1) - lloc: 3 - "\b\f\x00\x05test4"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 7 (v:1) - lloc: 1 - "\b\f\x00\x05test4"
cloc: 6 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	manager.MemberErrors = make(map[string]error)
	defer func() { manager.MemberErrors = nil }()

	// Simulate an error on member 3

	manager.MemberErrors[cluster3[3].MemberManager.Name()] = &testNetError{}

	// Ensure the transfer worker is running on all members

	for _, m := range ms {
		m.rebalanceWorker(true)
	}

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 6 (v:2) - lloc: 2 - "\x10\f\x00\rtest3_updated"
cloc: 7 (v:1) - lloc: 3 - "\b\f\x00\x05test4"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 7 (v:1) - lloc: 1 - "\b\f\x00\x05test4"
cloc: 6 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Remove the error

	delete(manager.MemberErrors, cluster3[3].MemberManager.Name())

	// Ensure the transfer worker is running on all members

	for _, m := range ms {
		m.rebalanceWorker(true)
	}

	// Check that update has happened

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 6 (v:2) - lloc: 2 - "\x10\f\x00\rtest3_updated"
cloc: 7 (v:1) - lloc: 3 - "\b\f\x00\x05test4"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 7 (v:1) - lloc: 1 - "\b\f\x00\x05test4"
cloc: 6 (v:2) - lloc: 2 - "\x10\f\x00\rtest3_updated"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}
}
