/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package graph

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"devt.de/krotik/eliasdb/cluster"
	"devt.de/krotik/eliasdb/cluster/manager"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
	"devt.de/krotik/eliasdb/hash"
)

func TestClusterWithPhysicalStorage(t *testing.T) {

	log.SetOutput(ioutil.Discard)

	dgs1, err := graphstorage.NewDiskGraphStorage(GraphManagerTestDBDir5, false)
	if err != nil {
		t.Error(err)
		return
	}

	ds1, _ := cluster.NewDistributedStorage(dgs1, map[string]interface{}{
		manager.ConfigRPC:           fmt.Sprintf("localhost:%v", 9021),
		manager.ConfigMemberName:    fmt.Sprintf("TestClusterMember-1"),
		manager.ConfigClusterSecret: "test123",
	}, manager.NewMemStateInfo())

	ds1.Start()
	defer ds1.Close()

	dgs2, err := graphstorage.NewDiskGraphStorage(GraphManagerTestDBDir6, false)
	if err != nil {
		t.Error(err)
		return
	}

	ds2, _ := cluster.NewDistributedStorage(dgs2, map[string]interface{}{
		manager.ConfigRPC:           fmt.Sprintf("localhost:%v", 9022),
		manager.ConfigMemberName:    fmt.Sprintf("TestClusterMember-2"),
		manager.ConfigClusterSecret: "test123",
	}, manager.NewMemStateInfo())

	ds2.Start()
	defer ds2.Close()

	err = ds2.MemberManager.JoinCluster(ds1.MemberManager.Name(),
		ds1.MemberManager.NetAddr())
	if err != nil {
		t.Error(err)
		return
	}

	sm := ds1.StorageManager("foo", true)
	sm2 := ds2.StorageManager("foo", true)

	loc, err := sm.Insert("test123")
	if loc != 1 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	loc, err = sm2.Insert("test456")
	if loc != 2 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	res := ""

	if err := sm2.Fetch(1, &res); err != nil {
		t.Error(err)
		return
	}

	if res != "test123" {
		t.Error("Unexpected result:", res)
		return
	}

	if err := sm2.Fetch(2, &res); err != nil {
		t.Error(err)
		return
	}

	if res != "test456" {
		t.Error("Unexpected result:", res)
		return
	}

	// *** HTree storage

	// Use a HTree to insert to and fetch from a storage manager

	sm = ds1.StorageManager("foo2", true)
	sm2 = ds2.StorageManager("foo2", true)

	htree, err := hash.NewHTree(sm)
	if err != nil {
		t.Error(err)
		return
	}

	if valres, err := htree.Put([]byte("123"), "Test1"); err != nil || valres != nil {
		t.Error("Unexpected result:", valres, err)
		return
	}

	if valres, err := htree.Put([]byte("123"), "Test2"); err != nil || valres != "Test1" {
		t.Error("Unexpected result:", valres, err)
		return
	}

	// Try to retrieve the item again

	cluster.WaitForTransfer()

	if val, err := htree.Get([]byte("123")); err != nil || val != "Test2" {
		t.Error("Unexpected result:", val, err)
		return
	}

	htree2, _ := hash.LoadHTree(sm2, 1)
	if val, err := htree2.Get([]byte("123")); err != nil || val != "Test2" {
		t.Error("Unexpected result:", val, err)
		return
	}

	// *** GraphManager storage

	gm1 := NewGraphManager(ds1)

	if err := gm1.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "123",
		"kind": "testnode",
		"foo":  "bar",
	})); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	cluster.WaitForTransfer()

	if node, err := gm1.FetchNode("main", "123", "testnode"); err != nil ||
		node.String() != `GraphNode:
     key : 123
    kind : testnode
     foo : bar
` {
		t.Error("Unexpected result:", node, err)
		return
	}

	gm2 := NewGraphManager(ds2)

	if node, err := gm2.FetchNode("main", "123", "testnode"); err != nil ||
		node.String() != `GraphNode:
     key : 123
    kind : testnode
     foo : bar
` {
		t.Error("Unexpected result:", node, err)
		return
	}
}

func TestClusterStorage(t *testing.T) {

	cluster2 := createCluster(2)

	joinCluster(cluster2, t)

	// *** Direct storage

	// Insert something into a storage manager and wait for the transfer

	sm := cluster2[0].StorageManager("foo", true)
	sm2 := cluster2[1].StorageManager("foo", true)

	loc, err := sm.Insert("test123")
	if loc != 1 || err != nil {
		t.Error("Unexpected result:", loc, err)
		return
	}

	cluster.WaitForTransfer()

	// Try to retrieve the item again

	// fmt.Println(cluster.DumpMemoryClusterLayout("foo"))

	var res string
	if err := sm.Fetch(1, &res); err != nil {
		t.Error(err)
		return
	}

	if res != "test123" {
		t.Error("Unexpected result:", res)
		return
	}

	res = ""

	if err := sm2.Fetch(1, &res); err != nil {
		t.Error(err)
		return
	}

	if res != "test123" {
		t.Error("Unexpected result:", res)
		return
	}

	// *** HTree storage

	// Use a HTree to insert to and fetch from a storage manager

	sm = cluster2[0].StorageManager("foo2", true)
	sm2 = cluster2[1].StorageManager("foo2", true)

	htree, err := hash.NewHTree(sm)
	if err != nil {
		t.Error(err)
		return
	}

	if valres, err := htree.Put([]byte("123"), "Test1"); err != nil || valres != nil {
		t.Error("Unexpected result:", valres, err)
		return
	}

	if valres, err := htree.Put([]byte("123"), "Test2"); err != nil || valres != "Test1" {
		t.Error("Unexpected result:", valres, err)
		return
	}

	// Try to retrieve the item again

	cluster.WaitForTransfer()

	if val, err := htree.Get([]byte("123")); err != nil || val != "Test2" {
		t.Error("Unexpected result:", val, err)
		return
	}

	htree2, _ := hash.LoadHTree(sm2, 1)
	if val, err := htree2.Get([]byte("123")); err != nil || val != "Test2" {
		t.Error("Unexpected result:", val, err)
		return
	}

	// *** GraphManager storage

	gm1 := NewGraphManager(cluster2[0])

	if err := gm1.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "123",
		"kind": "testnode",
		"foo":  "bar",
	})); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	cluster.WaitForTransfer()

	if node, err := gm1.FetchNode("main", "123", "testnode"); err != nil ||
		node.String() != `GraphNode:
     key : 123
    kind : testnode
     foo : bar
` {
		t.Error("Unexpected result:", node, err)
		return
	}

	gm2 := NewGraphManager(cluster2[1])

	if node, err := gm2.FetchNode("main", "123", "testnode"); err != nil ||
		node.String() != `GraphNode:
     key : 123
    kind : testnode
     foo : bar
` {
		t.Error("Unexpected result:", node, err)
		return
	}
}

/*
Create a cluster with n members (all storage is in memory)
*/
func createCluster(n int) []*cluster.DistributedStorage {
	// By default no log output

	log.SetOutput(ioutil.Discard)

	var mgs []*graphstorage.MemoryGraphStorage
	var cs []*cluster.DistributedStorage

	cluster.ClearMSMap()

	for i := 0; i < n; i++ {
		mgs = append(mgs, graphstorage.NewMemoryGraphStorage(fmt.Sprintf("mgs%v", i+1)).(*graphstorage.MemoryGraphStorage))
	}

	for i := 0; i < n; i++ {
		ds, _ := cluster.NewDistributedStorage(mgs[i], map[string]interface{}{
			manager.ConfigRPC:           fmt.Sprintf("localhost:%v", 9020+i),
			manager.ConfigMemberName:    fmt.Sprintf("TestClusterMember-%v", i),
			manager.ConfigClusterSecret: "test123",
		}, manager.NewMemStateInfo())
		cs = append(cs, ds)
	}

	return cs
}

/*
joinCluster joins up a given cluster.
*/
func joinCluster(cluster []*cluster.DistributedStorage, t *testing.T) {

	for i, dd := range cluster {
		dd.Start()
		defer dd.Close()

		if i > 0 {
			err := dd.MemberManager.JoinCluster(cluster[0].MemberManager.Name(),
				cluster[0].MemberManager.NetAddr())
			if err != nil {
				t.Error(err)
				return
			}
		}
	}
}
