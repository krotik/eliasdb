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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"testing"

	"devt.de/eliasdb/cluster/manager"
	"devt.de/eliasdb/graph/graphstorage"
)

func TestMainDBDistribution(t *testing.T) {

	// Setup a cluster

	// Housekeeping frequency is low so the test runs fast and we have it
	// interfering - try to produce dead locks, etc ...

	manager.FreqHousekeeping = 5
	defer func() { manager.FreqHousekeeping = 1000 }()

	cluster3, _ := createCluster(3, 2)

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

	// Insert stuff

	cluster3[0].MainDB()["test1"] = "123"
	cluster3[0].FlushMain()
	cluster3[1].MainDB()["test2"] = "234"
	cluster3[1].FlushMain()
	cluster3[2].MainDB()["test3"] = "345"
	cluster3[2].FlushMain()

	mdb := cluster3[1].MainDB()
	if mdb["test1"] != "123" || mdb["test2"] != "234" || mdb["test3"] != "345" || len(mdb) != 3 {
		t.Error("Unexpected main db:", mdb)
		return
	}
}

func TestSimpleDataDistribution(t *testing.T) {

	// Set a low distribution range

	defaultDistributionRange = 10
	defer func() { defaultDistributionRange = math.MaxUint64 }()

	// Make sure the transfer worker is not running

	runTransferWorker = false
	defer func() { runTransferWorker = true }()

	// Setup a cluster

	manager.FreqHousekeeping = 5
	defer func() { manager.FreqHousekeeping = 1000 }()

	cluster3, _ := createCluster(3, 2)

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

	// Make sure the replication factor is distributed in the cluster. Once set
	// in the configuration it becomes part of the cluster state.

	if rf := cluster3[2].MemberManager.StateInfo().Map()[manager.StateInfoREPFAC].(int); rf != 2 ||
		cluster3[1].distributionTable.repFac != 2 || cluster3[2].distributionTable.repFac != 2 {
		t.Error("Unexpected replication factor in the cluster:", rf)
		return
	}

	// Initially the storage manager should not exist

	if smtest := cluster3[1].StorageManager("test1", false); smtest != nil {
		t.Error("Not existing storage manager should be nil")
		return
	}

	// Simple insert requests - data is stored on all members

	sm := cluster3[0].StorageManager("test1", true)

	// Even after the first creation call should the storage manage not exist

	if smtest := cluster3[1].StorageManager("test1", false); smtest != nil {
		t.Error("Not existing storage manager should be nil")
		return
	}

	if loc, err := sm.Insert("test1"); loc != 0 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	sm.Flush()

	// After the first insert the storage manage should exist

	if smtest := cluster3[1].StorageManager("test1", false); smtest == nil {
		t.Error("Existing storage manager should be not nil")
		return
	}

	if loc, err := sm.Insert("test2"); loc != 3 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	sm.Flush()

	if loc, err := sm.Insert("test3"); loc != 6 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	sm.Flush()

	if loc, err := sm.Insert("test4"); loc != 1 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	if loc, err := sm.Insert("test5"); loc != 2 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	// Lookup the data again

	var res string

	if err := sm.Fetch(0, &res); res != "test1" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
	if err := sm.Fetch(1, &res); res != "test4" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
	if err := sm.Fetch(3, &res); res != "test2" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
	if err := sm.Fetch(2, &res); res != "test5" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
	if err := sm.Fetch(6, &res); res != "test3" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Update some data

	if err := sm.Update(0, "test11"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}
	if err := sm.Update(6, "test44"); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	// Lookup the data again

	if err := sm.Fetch(0, &res); res != "test11" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
	if err := sm.Fetch(6, &res); res != "test44" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Delete some data

	if err := sm.Free(0); err != nil {
		t.Error("Unexpected result:", err)
		return
	}
	if err := sm.Free(6); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	// Lookup the data again

	res = ""
	if err := sm.Fetch(0, &res); res != "" || err.Error() != "Cluster slot not found (TestClusterMember-0 - Location: 0)" {
		t.Error("Unexpected result:", res, err)
		return
	}
	res = ""
	if err := sm.Fetch(2, &res); res != "test5" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
	res = ""
	if err := sm.Fetch(6, &res); res != "" || err.Error() != "ClusterError: Member error (Cluster slot not found (TestClusterMember-2 - Location: 6))" {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Set and retrieve root values

	sm = cluster3[0].StorageManager("test1", true)
	sm.SetRoot(4, 67)

	sm = cluster3[1].StorageManager("test1", true)
	sm.SetRoot(5, 88)

	sm = cluster3[2].StorageManager("test1", true)
	sm.SetRoot(4, 22)

	sm = cluster3[0].StorageManager("test1", true)
	if root := sm.Root(4); root != 22 {
		t.Error("Unexpected result:", root)
		return
	}

	sm = cluster3[2].StorageManager("test1", true)
	if root := sm.Root(5); root != 88 {
		t.Error("Unexpected result:", root)
		return
	}

	manager.LogDebug = manager.LogInfo
	log.SetOutput(os.Stderr)
	defer func() { log.SetOutput(ioutil.Discard) }()
}

/*
createCluster creates a cluster with n members (all storage is in memory)
*/
func createCluster(n int, rep float64) ([]*DistributedStorage, []*memberStorage) {

	// By default no log output

	log.SetOutput(ioutil.Discard)

	var mgs []*graphstorage.MemoryGraphStorage
	var dss []*DistributedStorage
	var mss []*memberStorage

	for i := 0; i < n; i++ {
		mgs = append(mgs, graphstorage.NewMemoryGraphStorage(fmt.Sprintf("mgs%v", i+1)).(*graphstorage.MemoryGraphStorage))
	}

	for i := 0; i < n; i++ {
		ds, ms, _ := newDistributedAndMemberStorage(mgs[i], map[string]interface{}{
			manager.ConfigRPC:               fmt.Sprintf("localhost:%v", 9020+i),
			manager.ConfigMemberName:        fmt.Sprintf("TestClusterMember-%v", i),
			manager.ConfigClusterSecret:     "test123",
			manager.ConfigReplicationFactor: (rep + float64(i)),
		}, manager.NewMemStateInfo())
		dss = append(dss, ds)
		mss = append(mss, ms)
	}

	return dss, mss
}

/*
clusterLayout returns the current storage layout in a cluster. Parameters is an
array of memberStorages and a storage name.
*/
func clusterLayout(ms []*memberStorage, smname string) string {
	buf := new(bytes.Buffer)

	for _, m := range ms {
		buf.WriteString(m.dump(smname))
	}

	return buf.String()
}

/*
retrieveStringFromClusterLoc tries to retrieve a given cluster location from a given member storage.
*/
func retrieveStringFromClusterLoc(ms *memberStorage, smname string, cloc uint64, exp string) error {
	var out interface{}
	var res string

	err := ms.handleFetchRequest(ms.ds.distributionTable, &DataRequest{RTFetch, map[DataRequestArg]interface{}{
		RPStoreName: smname,
		RPLoc:       cloc,
	}, nil, false}, &out, true)

	if err == nil {

		// Decode stored value (this would be otherwise done on the receiving end)

		err = gob.NewDecoder(bytes.NewReader(out.([]byte))).Decode(&res)

		if err == nil && res != exp {
			err = fmt.Errorf("Unexpected cloc result: %v (expected: %v)", res, exp)
		}
	}

	return err
}

func checkStateInfo(mm *manager.MemberManager, expectedStateInfo string) error {
	var w bytes.Buffer

	ret := json.NewEncoder(&w)

	si := mm.StateInfo().Map()

	// We don't care about timestamps in this test since goroutines run
	// concurrently - we can't say which one will do the update first

	delete(si, "ts")
	delete(si, "tsold")

	ret.Encode(si)

	out := bytes.Buffer{}

	err := json.Indent(&out, w.Bytes(), "", "  ")
	if err != nil {
		return err
	}

	if out.String() != expectedStateInfo {
		return fmt.Errorf("Unexpected state info: %v\nexpected: %v",
			out.String(), expectedStateInfo)
	}

	return nil
}

// Test network failure

type testNetError struct {
}

func (*testNetError) Error() string {
	return "test.net.Error"
}

func (*testNetError) Timeout() bool {
	return false
}

func (*testNetError) Temporary() bool {
	return true
}
