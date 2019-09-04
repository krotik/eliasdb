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
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"testing"

	"devt.de/krotik/eliasdb/cluster/manager"
	"devt.de/krotik/eliasdb/graph/graphstorage"
	"devt.de/krotik/eliasdb/storage"
)

func TestDistributionStorageInitialisationError(t *testing.T) {

	gs := graphstorage.NewMemoryGraphStorage("bla")

	si := manager.NewMemStateInfo()

	// Set an unreasonable replication factor

	si.Put(manager.StateInfoREPFAC, 500)

	conf := map[string]interface{}{
		manager.ConfigClusterSecret: "",
	}

	// Make flush error at first

	storage.MsmRetFlush = errors.New("testerror")

	ds, err := NewDistributedStorage(gs, conf, si)
	if err.Error() != "testerror" || ds != nil {
		t.Error("Unexpected result:", ds, err)
		return
	}

	storage.MsmRetFlush = nil

	// Create a new DistributionStorage

	ds, err = NewDistributedStorage(gs, conf, si)
	if err != nil || ds == nil {
		t.Error("Unexpected result:", ds, err)
		return
	}

	// Test simple operations

	if res := ds.LocalName(); res != "bla" {
		t.Error("Unexpected local name:", res)
		return
	}

	if ds.FlushAll() != nil {
		t.Error(err)
		return
	}

	if ds.IsOperational() {
		t.Error("Cluster should not be operational at this point")
		return
	}

	if len(ds.MainDB()) != 0 {
		t.Error("MainDB should return an empty map at this point")
		return
	}

	ds.RollbackMain()

	// This returns the distTableErr

	if err := ds.FlushMain(); err.Error() != "Not enough members (1) for given replication factor: 500" {
		t.Error("Unexpected result:", err)
		return
	}

	if res := ds.StorageManager("test", true); res != nil {
		t.Error("StorageManager should return nil with missing distribution table")
		return
	}

	if len(ds.MainDB()) != 0 {
		t.Error("MainDB should return an empty map at this point")
		return
	}

	// This returns the mainDBErr

	if err := ds.FlushMain(); err.Error() != "Not enough members (1) for given replication factor: 500" {
		t.Error("Unexpected result:", err)
		return
	}

	if res := ds.StorageManager("test", true); res != nil {
		t.Error("StorageManager should return nil when main db is not available")
		return
	}
}

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

	cluster3, ds := createCluster(3, 2)

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

	if res := sm.Name(); res != "DistributedStorageManager: test1" {
		t.Error("Unexpected name:", res)
		return
	}

	// Test handling of distribution table errors with existing storage manager objects

	cluster3[0].distributionTableError = errors.New("TestError")

	sm.SetRoot(1, 5)

	if sm.Root(1) != 0 {
		t.Error("All root values should be returned as 0 with distribution table errors")
		return
	}

	if _, err := sm.Insert("test"); err.Error() != "TestError" {
		t.Error("Unexpected response:", err)
		return
	}

	if err := sm.Update(5, "test"); err.Error() != "TestError" {
		t.Error("Unexpected response:", err)
		return
	}

	if err := sm.Free(5); err.Error() != "TestError" {
		t.Error("Unexpected response:", err)
		return
	}

	if _, err := sm.FetchCached(5); err != storage.ErrNotInCache {
		t.Error("Unexpected response:", err)
		return
	}

	if err := sm.Fetch(5, nil); err.Error() != "TestError" {
		t.Error("Unexpected response:", err)
		return
	}

	if err := sm.Close(); err.Error() != "TestError" {
		t.Error("Unexpected response:", err)
		return
	}

	if err := sm.Flush(); err.Error() != "TestError" {
		t.Error("Unexpected response:", err)
		return
	}

	// No effect on NOP operations

	if err := sm.Rollback(); err != nil {
		t.Error("Unexpected response:", err)
		return
	}

	cluster3[0].distributionTableError = nil

	if err := sm.Close(); err != nil {
		t.Error("Unexpected response:", err)
		return
	}

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

	// Test sending outdated request

	request := &DataRequest{RTUpdate, map[DataRequestArg]interface{}{
		RPStoreName: "test1",
		RPLoc:       uint64(6),
		RPVer:       uint64(1),
	}, []byte("1111"), true}

	_, err := ds[1].ds.sendDataRequest(cluster3[2].MemberManager.Name(), request)
	if err != nil {
		t.Error("Unexpected response:", err)
		return
	}

	// Check that the outdated transfer request was ignored

	if err := sm.Fetch(6, &res); res != "test44" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Try updating something which does not exist

	request = &DataRequest{RTUpdate, map[DataRequestArg]interface{}{
		RPStoreName: "test1",
		RPLoc:       uint64(99),
		RPVer:       uint64(1),
	}, []byte("1111"), true}

	_, err = ds[1].ds.sendDataRequest(cluster3[2].MemberManager.Name(), request)
	if err.Error() != "ClusterError: Member error (Cluster slot not found (TestClusterMember-2 - Location: 99))" {
		t.Error("Unexpected response:", err)
		return
	}

	lsm := ds[0].dataStorage("test1", false)

	// Destroy the gob encoding in cluster slot 0 (local slot 1)

	lsm.Update(1, "test11")

	if err := sm.Fetch(0, &res); err.Error() !=
		"gob: decoding into local type *[]uint8, received remote type string" {
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

	// Test certain errors

	var RPMyRequest RequestType = "RPMyRequest"

	request = &DataRequest{RPMyRequest, map[DataRequestArg]interface{}{}, nil, false}

	_, err = ds[1].ds.sendDataRequest(cluster3[0].MemberManager.Name(), request)
	if err.Error() != "ClusterError: Member error (Unknown request type)" {
		t.Error("Unexpected response:", err)
		return
	}

	cluster3[0].distributionTableError = errors.New("testerror")

	_, err = ds[1].ds.sendDataRequest(cluster3[0].MemberManager.Name(), request)
	if err.Error() != "ClusterError: Member error (Storage is currently disabled on member: TestClusterMember-0 (testerror))" {
		t.Error("Unexpected response:", err)
		return
	}
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
