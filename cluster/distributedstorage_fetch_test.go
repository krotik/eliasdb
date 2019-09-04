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
	"math"
	"testing"
	"time"

	"devt.de/krotik/eliasdb/cluster/manager"
)

func TestSimpleDataReplicationFetch(t *testing.T) {

	// Set a low distribution range

	defaultDistributionRange = 5000
	defer func() { defaultDistributionRange = math.MaxUint64 }()

	// Setup a cluster

	manager.FreqHousekeeping = 5
	defer func() { manager.FreqHousekeeping = 1000 }()

	// Log transfer worker runs

	logTransferWorker = true
	defer func() { logTransferWorker = false }()

	// Create a cluster with 3 members and a replication factor of 2

	cluster3, ms := createCluster(3, 2)

	// Debug output

	// manager.LogDebug = manager.LogInfo
	// log.SetOutput(os.Stderr)
	// defer func() { log.SetOutput(ioutil.Discard) }()

	for i, dd := range cluster3 {
		dd.Start()
		defer dd.Close()

		if i > 0 {
			err := dd.MemberManager.JoinCluster(cluster3[0].MemberManager.Name(), cluster3[0].MemberManager.NetAddr())
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

	if loc, err := sm.Insert("test2"); loc != 1666 || err != nil {
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
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 1666 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1666 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] && res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 0 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1666 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 0 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1666 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Now do a normal fetch

	var ret string

	if err := sm.Fetch(0, &ret); err != nil || ret != "test1" {
		t.Error("Unexpected result:", err, ret)
		return
	}

	if err := sm.Fetch(1666, &ret); err != nil || ret != "test2" {
		t.Error("Unexpected result:", err, ret)
		return
	}

	// Simulate an error on member 0

	manager.MemberErrors = make(map[string]error)
	defer func() { manager.MemberErrors = nil }()

	manager.MemberErrors[cluster3[0].MemberManager.Name()] = &testNetError{}
	cluster3[0].MemberManager.StopHousekeeping = true
	defer func() { cluster3[0].MemberManager.StopHousekeeping = false }()

	if err := sm.Fetch(0, &ret); err != nil || ret != "test1" {
		t.Error("Unexpected result:", err, ret)
		return
	}

	if err := sm.Fetch(1666, &ret); err != nil || ret != "test2" {
		t.Error("Unexpected result:", err, ret)
		return
	}

	sm = cluster3[2].StorageManager("test", false)

	if err := sm.Fetch(0, &ret); err != nil || ret != "test1" {
		t.Error("Unexpected result:", err, ret)
		return
	}

	if err := sm.Fetch(1666, &ret); err != nil || ret != "test2" {
		t.Error("Unexpected result:", err, ret)
		return
	}

	if err := sm.Fetch(1667, &ret); err.Error() !=
		"ClusterError: Member error (Cluster slot not found (TestClusterMember-1 - Location: 1667))" {
		t.Error("Unexpected result:", err)
		return
	}
}
