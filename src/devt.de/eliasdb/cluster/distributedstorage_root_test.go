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

	"devt.de/eliasdb/cluster/manager"
)

func TestSimpleDataReplicationRoot(t *testing.T) {

	// Set a low distribution range

	defaultDistributionRange = 10
	defer func() { defaultDistributionRange = math.MaxUint64 }()

	// Setup a cluster

	manager.FreqHousekeeping = 5
	defer func() { manager.FreqHousekeeping = 1000 }()

	// Disable the transfer worker for this test

	runTransferWorker = false
	defer func() { runTransferWorker = true }()

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

	// Main DB should be empty at this point

	sm := cluster3[1].StorageManager("test", true)
	sm.SetRoot(5, 10)

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=10 6=0 7=0 8=0 9=0 
transfer: [TestClusterMember-1] - SetRoot {"Root":5,"StoreName":"test"} "10"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Run the transfer worker

	runTransferWorker = true
	ms[0].transferWorker()
	runTransferWorker = false

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=10 6=0 7=0 8=0 9=0 
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=10 6=0 7=0 8=0 9=0 
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Simulate a failure on member 0

	manager.MemberErrors = make(map[string]error)
	defer func() { manager.MemberErrors = nil }()

	manager.MemberErrors[cluster3[0].MemberManager.Name()] = &testNetError{}

	sm = cluster3[2].StorageManager("test", true)
	sm.SetRoot(8, 14)

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=10 6=0 7=0 8=0 9=0 
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=10 6=0 7=0 8=14 9=0 
transfer: [TestClusterMember-0] - SetRoot {"Root":8,"StoreName":"test"} "14"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	sm = cluster3[2].StorageManager("test", true)
	if rv := sm.Root(8); rv != 14 {
		t.Error("Unexpected root value returned:", rv)
		return
	}

	// Simulate a member 0 is working again

	manager.MemberErrors = make(map[string]error)
	defer func() { manager.MemberErrors = nil }()

	delete(manager.MemberErrors, cluster3[0].MemberManager.Name())

	// Run the transfer worker

	runTransferWorker = true
	ms[0].transferWorker()
	ms[1].transferWorker()
	ms[2].transferWorker()
	runTransferWorker = false

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=10 6=0 7=0 8=14 9=0 
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=10 6=0 7=0 8=14 9=0 
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}
}
