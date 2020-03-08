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

func TestSimpleDataReplicationInsert(t *testing.T) {

	// Set a low distribution range

	defaultDistributionRange = 10
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

	if loc, err := sm.Insert("test1"); loc != 1 || err != nil {
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
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] && res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// At this point we should have the records on the main machines and their replicas

	if err := retrieveStringFromClusterLoc(ms[0], "test", 1, "test1"); err != nil {
		t.Error(err)
		return
	}

	// Simulate a failure on members 0 and 2

	manager.MemberErrors = make(map[string]error)
	defer func() { manager.MemberErrors = nil }()

	if err := checkStateInfo(cluster3[1].MemberManager, `
{
  "failed": null,
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-2",
    "localhost:9022"
  ],
  "replication": 2
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	if !cluster3[1].IsOperational() {
		t.Error("Cluster should be operational at this point")
		return
	}

	if res := cluster3[1].ReplicationFactor(); res != 2 {
		t.Error("Unexpected result:", res)
		return
	}
	// Simulate a failure on members 0 and 2

	manager.MemberErrors[cluster3[0].MemberManager.Name()] = &testNetError{}
	cluster3[0].MemberManager.StopHousekeeping = true
	defer func() { cluster3[0].MemberManager.StopHousekeeping = false }()

	manager.MemberErrors[cluster3[2].MemberManager.Name()] = &testNetError{}
	cluster3[2].MemberManager.StopHousekeeping = true

	// Since members 0 and 2 are not reachable the system should choose member 1

	if loc, err := sm.Insert("test3"); loc != 4 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	// Make sure Housekeeping is running

	cluster3[1].MemberManager.HousekeepingWorker()

	time.Sleep(10 * time.Microsecond)

	// Check that the cluster has recorded the failure

	if err := checkStateInfo(cluster3[1].MemberManager, `
{
  "failed": [
    "TestClusterMember-0",
    "test.net.Error",
    "TestClusterMember-2",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-1",
    "localhost:9021",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-2",
    "localhost:9022"
  ],
  "replication": 2
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	// Ensure the transfer worker is running on member 1

	ms[1].transferWorker()

	// Check that we have a certain storage layout in the cluster

	// The test3 record is stored on member 1. Member 1 should also have
	// a pending transfer request for member 2 which is for the
	// replication of the test3 record

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
cloc: 4 (v:1) - lloc: 3 - "\b\f\x00\x05test3"
transfer: [TestClusterMember-2] - Insert {"Loc":4,"StoreName":"test"} "\b\f\x00\x05test3"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Although the insert succeeded the cluster should now be reported as broken

	if ms[1].ds.distributionTable != nil {
		t.Error("Distribution table should not exist in a broken cluster")
		return
	}

	if loc, err := sm.Insert("test4"); err.Error() != "Storage disabled: Too many members failed (total: 3, failed: 2, replication: 2)" {
		t.Error("Unexpected result:", loc, err)
		return
	}

	if cluster3[1].IsOperational() {
		t.Error("Cluster should not be operational at this point")
		return
	}

	if res := cluster3[1].ReplicationFactor(); res != 0 {
		t.Error("Unexpected result:", res)
		return
	}

	// Now make member 2 work again

	delete(manager.MemberErrors, cluster3[2].MemberManager.Name())
	cluster3[2].MemberManager.StopHousekeeping = false

	// Make sure Housekeeping was running on all available members

	cluster3[1].MemberManager.HousekeepingWorker()
	cluster3[2].MemberManager.HousekeepingWorker()

	// Check that the cluster has recovered

	if err := checkStateInfo(cluster3[2].MemberManager, `
{
  "failed": [
    "TestClusterMember-0",
    "test.net.Error"
  ],
  "members": [
    "TestClusterMember-2",
    "localhost:9022",
    "TestClusterMember-0",
    "localhost:9020",
    "TestClusterMember-1",
    "localhost:9021"
  ],
  "replication": 2
}
`[1:]); err != nil {
		t.Error(err)
		return
	}

	if dss := ms[1].ds.distributionTable.String(); dss != `
Location ranges:
TestClusterMember-0: 0 -> 2
TestClusterMember-1: 3 -> 5
TestClusterMember-2: 6 -> 10
Replicas (factor=2) :
TestClusterMember-0: [TestClusterMember-1]
TestClusterMember-1: [TestClusterMember-2]
TestClusterMember-2: [TestClusterMember-0]
`[1:] {
		t.Error("Unexpected distribution table:", dss)
		return
	}

	// Check that replication has happened

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 3 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
cloc: 4 (v:1) - lloc: 3 - "\b\f\x00\x05test3"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 3 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 4 (v:1) - lloc: 2 - "\b\f\x00\x05test3"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}
}

func TestSimpleDataReplicationInsertWithErrors(t *testing.T) {

	// Set a low distribution range

	defaultDistributionRange = 10
	defer func() { defaultDistributionRange = math.MaxUint64 }()

	// Setup a cluster

	manager.FreqHousekeeping = 5
	defer func() { manager.FreqHousekeeping = 1000 }()

	// Log transfer worker runs

	logTransferWorker = true
	defer func() { logTransferWorker = false }()

	// Create a cluster with 4 members and a replication factor of 3

	cluster4, ms := createCluster(4, 3)

	// Debug output

	// manager.LogDebug = manager.LogInfo
	// log.SetOutput(os.Stderr)
	// defer func() { log.SetOutput(ioutil.Discard) }()

	for i, dd := range cluster4 {
		dd.Start()
		defer dd.Close()

		if i > 0 {
			err := dd.MemberManager.JoinCluster(cluster4[0].MemberManager.Name(), cluster4[0].MemberManager.NetAddr())
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	// Simulate member 3 failing

	manager.MemberErrors = make(map[string]error)
	defer func() { manager.MemberErrors = nil }()

	manager.MemberErrors[cluster4[3].MemberManager.Name()] = &testNetError{}
	cluster4[3].MemberManager.StopHousekeeping = true

	sm := cluster4[1].StorageManager("test", true)

	// Insert two strings into the store

	if loc, err := sm.Insert("test1"); loc != 1 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	sm.Flush()

	time.Sleep(10 * time.Millisecond)

	if loc, err := sm.Insert("test2"); loc != 2 || err != nil {
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
	// The transfer request has partially succeeded

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
transfer: [TestClusterMember-3] - Insert {"Loc":2,"StoreName":"test"} "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
`[1:] && res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
transfer: [TestClusterMember-3] - Insert {"Loc":2,"StoreName":"test"} "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
`[1:] && res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
transfer: [TestClusterMember-3] - Insert {"Loc":2,"StoreName":"test"} "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
`[1:] && res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
transfer: [TestClusterMember-3] - Insert {"Loc":2,"StoreName":"test"} "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}

	// Simulate member 3 working again

	delete(manager.MemberErrors, cluster4[3].MemberManager.Name())
	cluster4[3].MemberManager.StopHousekeeping = false

	// Ensure the transfer worker is running on all members

	for _, m := range ms {
		m.transferWorker()
		for m.transferRunning {
			time.Sleep(time.Millisecond)
		}
	}

	// Check that we have a certain storage layout in the cluster
	// The transfer request has now fully succeeded

	if res := clusterLayout(ms, "test"); res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] && res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] && res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
cloc: 2 (v:1) - lloc: 2 - "\b\f\x00\x05test2"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] && res != `
TestClusterMember-0 MemberStorageManager mgs1/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 1 (v:1) - lloc: 1 - "\b\f\x00\x05test1"
TestClusterMember-1 MemberStorageManager mgs2/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
TestClusterMember-2 MemberStorageManager mgs3/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
cloc: 1 (v:1) - lloc: 2 - "\b\f\x00\x05test1"
TestClusterMember-3 MemberStorageManager mgs4/ls_test
Roots: 0=0 1=0 2=0 3=0 4=0 5=0 6=0 7=0 8=0 9=0 
cloc: 2 (v:1) - lloc: 1 - "\b\f\x00\x05test2"
`[1:] {
		t.Error("Unexpected cluster storage layout: ", res)
		return
	}
}
