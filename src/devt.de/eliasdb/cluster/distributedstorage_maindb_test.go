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

func TestSimpleDataReplicationMainDB(t *testing.T) {

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

	if res := clusterLayout(ms, ""); res != `
TestClusterMember-0 MemberStorageManager MainDB
TestClusterMember-1 MemberStorageManager MainDB
TestClusterMember-2 MemberStorageManager MainDB
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Insert a string

	main := cluster3[1].MainDB()

	main["test1"] = "123"

	cluster3[1].FlushMain()

	if res := clusterLayout(ms, ""); res != `
TestClusterMember-0 MemberStorageManager MainDB
test1 - "123"
transfer: [TestClusterMember-1] - SetMain null "{\"test1\":\"123\"}"
TestClusterMember-1 MemberStorageManager MainDB
TestClusterMember-2 MemberStorageManager MainDB
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Run the transfer worker

	runTransferWorker = true
	ms[0].transferWorker()
	runTransferWorker = false

	if res := clusterLayout(ms, ""); res != `
TestClusterMember-0 MemberStorageManager MainDB
test1 - "123"
TestClusterMember-1 MemberStorageManager MainDB
test1 - "123"
TestClusterMember-2 MemberStorageManager MainDB
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Simulate a failure on member 0

	manager.MemberErrors = make(map[string]error)
	defer func() { manager.MemberErrors = nil }()

	manager.MemberErrors[cluster3[0].MemberManager.Name()] = &testNetError{}

	// Insert another string

	main = cluster3[2].MainDB()

	if len(main) != 1 {
		t.Error("MainDB should have only one entry at this point:", main)
		return
	}

	main["test2"] = "456"

	cluster3[2].FlushMain()

	if res := clusterLayout(ms, ""); res != `
TestClusterMember-0 MemberStorageManager MainDB
test1 - "123"
TestClusterMember-1 MemberStorageManager MainDB
test1 - "123"
test2 - "456"
transfer: [TestClusterMember-0] - SetMain null "{\"test1\":\"123\",\"test2\":\"456\"}"
TestClusterMember-2 MemberStorageManager MainDB
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
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

	if res := clusterLayout(ms, ""); res != `
TestClusterMember-0 MemberStorageManager MainDB
test1 - "123"
test2 - "456"
TestClusterMember-1 MemberStorageManager MainDB
test1 - "123"
test2 - "456"
TestClusterMember-2 MemberStorageManager MainDB
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	main = cluster3[2].MainDB()

	if len(main) != 2 {
		t.Error("MainDB should have two entries at this point:", main)
		return
	}

	// Remove from main DB

	main = cluster3[1].MainDB()

	delete(main, "test1")

	cluster3[1].FlushMain()

	runTransferWorker = true
	ms[0].transferWorker()
	ms[1].transferWorker()
	ms[2].transferWorker()
	runTransferWorker = false

	if res := clusterLayout(ms, ""); res != `
TestClusterMember-0 MemberStorageManager MainDB
test2 - "456"
TestClusterMember-1 MemberStorageManager MainDB
test2 - "456"
TestClusterMember-2 MemberStorageManager MainDB
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}
}
