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
	"errors"
	"fmt"
	"strings"
	"testing"

	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/storage"
)

func TestNormalTrans(t *testing.T) {
	if !RunDiskStorageTests {
		return
	}

	constructEdge := func(node1 data.Node, kind string, node2 data.Node) data.Edge {

		edge := data.NewGraphEdge()

		edge.SetAttr("key", "abc"+node1.Key()+node2.Key())
		edge.SetAttr("kind", kind)

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "node1")
		edge.SetAttr(data.EdgeEnd1Cascading, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "node2")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr(data.NodeName, "Edge "+kind)

		return edge
	}

	dgs, err := graphstorage.NewDiskGraphStorage(GraphManagerTestDBDir4)
	if err != nil {
		t.Error(err)
		return
	}

	gm := newGraphManagerNoRules(dgs)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("Name", "Node1")

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynewnode")
	node2.SetAttr("Name", "Node2")

	trans := NewGraphTrans(gm)

	// Store some nodes

	if err := trans.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}
	if err := trans.StoreNode("main", node2); err != nil {
		t.Error(err)
		return
	}
	if err := trans.StoreEdge("main", constructEdge(node1, "myedge", node2)); err != nil {
		t.Error(err)
		return
	}

	if c := gm.NodeCount("mynode"); c != 0 {
		t.Error("Unexpected node count:", c)
		return
	}

	if err := trans.Commit(); err != nil {
		t.Error(err)
		return
	}

	if c := gm.NodeCount("mynode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.NodeCount("mynewnode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.EdgeCount("myedge"); c != 1 {
		t.Error("Unexpected edge count:", c)
		return
	}

	trans2 := NewGraphTrans(gm)
	trans3 := NewGraphTrans(gm)

	node3 := data.NewGraphNode()
	node3.SetAttr("key", "789")
	node3.SetAttr("kind", "mynode")
	node3.SetAttr("Name", "Node3")

	node4 := data.NewGraphNode()
	node4.SetAttr("key", "abc")
	node4.SetAttr("kind", "mynode")
	node4.SetAttr("Name", "Node4")

	if err := trans2.StoreNode("main", node3); err != nil {
		t.Error(err)
		return
	}
	if err := trans2.StoreEdge("main", constructEdge(node3, "myedge", node4)); err != nil {
		t.Error(err)
		return
	}
	if err := trans3.StoreNode("main", node4); err != nil {
		t.Error(err)
		return
	}

	// This should fail since node 4 is not there

	if err := trans2.Commit(); err.Error() != "GraphError: Invalid data (Can't find edge endpoint: abc (mynode))" {
		t.Error(err)
		return
	}

	// Transaction should have rolled back

	if c := gm.NodeCount("mynode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.NodeCount("mynewnode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.EdgeCount("myedge"); c != 1 {
		t.Error("Unexpected edge count:", c)
		return
	}

	// Now commit transaction 3 to make transaction 2 work

	if err := trans3.Commit(); err != nil {
		t.Error(err)
		return
	}

	if c := gm.NodeCount("mynode"); c != 2 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.NodeCount("mynewnode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.EdgeCount("myedge"); c != 1 {
		t.Error("Unexpected edge count:", c)
		return
	}

	if err := trans2.StoreNode("main", node3); err != nil {
		t.Error(err)
		return
	}
	if err := trans2.StoreEdge("main", constructEdge(node3, "myedge", node4)); err != nil {
		t.Error(err)
		return
	}

	if err := trans2.Commit(); err != nil {
		t.Error(err)
		return
	}

	if c := gm.NodeCount("mynode"); c != 3 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.NodeCount("mynewnode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.EdgeCount("myedge"); c != 2 {
		t.Error("Unexpected edge count:", c)
		return
	}

	// Check that we commit the transactions again - the inserts become
	// updates but the numbers won't change

	transUpdate := NewGraphTrans(gm)

	transUpdate.StoreNode("main", node1)
	transUpdate.StoreNode("main", node2)
	transUpdate.StoreNode("main", node3)
	transUpdate.StoreNode("main", node4)

	transUpdate.StoreEdge("main", constructEdge(node1, "myedge", node2))
	transUpdate.StoreEdge("main", constructEdge(node3, "myedge", node4))

	if err := transUpdate.Commit(); err != nil {
		t.Error(err)
		return
	}

	// Test commit of empty transaction

	if err := NewGraphTrans(gm).Commit(); err != nil {
		t.Error(err)
		return
	}

	if c := gm.NodeCount("mynode"); c != 3 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.NodeCount("mynewnode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.EdgeCount("myedge"); c != 2 {
		t.Error("Unexpected edge count:", c)
		return
	}

	// Test search index updates

	q, _ := gm.NodeIndexQuery("main", node4.Kind())
	res, _ := q.LookupWord("Name", "Node4")
	if fmt.Sprint(res) != "map[abc:[1]]" {
		t.Error("Unexpected index lookup result:", res)
		return
	}

	// Test removal of stuff

	trans4 := NewGraphTrans(gm)

	trans4.RemoveEdge("main", "abc789abc", "myedge")
	trans4.RemoveNode("main", node4.Key(), node4.Kind())

	if err := trans4.Commit(); err != nil {
		t.Error(err)
		return
	}

	q, _ = gm.NodeIndexQuery("main", node4.Kind())
	res, _ = q.LookupWord("Name", "Node4")
	if fmt.Sprint(res) != "map[]" {
		t.Error("Unexpected index lookup result:", res)
		return
	}

	if c := gm.NodeCount("mynode"); c != 2 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.NodeCount("mynewnode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	}
	if c := gm.EdgeCount("myedge"); c != 1 {
		t.Error("Unexpected edge count:", c)
		return
	}

	dgs.Close()
}

func TestTransBuilding(t *testing.T) {
	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mykind")
	node1.SetAttr("Name", "Node1")

	updnode1 := data.NewGraphNode()
	updnode1.SetAttr("key", "123")
	updnode1.SetAttr("kind", "mykind")
	updnode1.SetAttr("Update", "ok")

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynewkind")
	node2.SetAttr("Name", "Node2")

	constructEdge := func(kind string) data.Edge {

		edge := data.NewGraphEdge()

		edge.SetAttr("key", "abc")
		edge.SetAttr("kind", kind)

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "node1")
		edge.SetAttr(data.EdgeEnd1Cascading, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "node2")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr(data.NodeName, "Edge "+kind)

		return edge
	}

	// Creeate storage and insert test nodes

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := newGraphManagerNoRules(mgs)

	trans := NewGraphTrans(gm)

	if err := trans.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	countMaps(t, trans, 1, 0, 0, 0)

	if err := trans.StoreNode("main", node2); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	countMaps(t, trans, 2, 0, 0, 0)

	edge1 := constructEdge("myedge")
	if err := trans.StoreEdge("main", edge1); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 2, 0, 1, 0)

	// Check that updating will not remove anything

	if err := trans.UpdateNode("main", updnode1); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 2, 0, 1, 0)

	// Check that the node was updated

	test := trans.storeNodes[trans.createKey("main", node1.Key(), node1.Kind())]
	if !data.NodeCompare(test, data.NodeMerge(node1, updnode1), nil) {
		t.Error("Unexpected update result:", test)
		return
	}

	// Remove a node

	if err := trans.RemoveNode("main", node1.Key(), node1.Kind()); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), false, true, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 1, 1, 1, 0)

	// Check that the update does an insert

	if err := trans.UpdateNode("main", updnode1); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 2, 0, 1, 0)

	// Check that the node was inserted

	test = trans.storeNodes[trans.createKey("main", node1.Key(), node1.Kind())]
	if !data.NodeCompare(test, updnode1, nil) {
		t.Error("Unexpected update result:", test)
		return
	}

	// Remove a node

	if err := trans.RemoveNode("main", node1.Key(), node1.Kind()); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), false, true, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 1, 1, 1, 0)

	// Store node again

	if err := trans.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 2, 0, 1, 0)

	// Remove edge

	if err := trans.RemoveEdge("main", edge1.Key(), edge1.Kind()); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, false, true)
	countMaps(t, trans, 2, 0, 0, 1)

	// Store the edge again

	if err := trans.StoreEdge("main", edge1); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 2, 0, 1, 0)

	// Test trivial errors using broken nodes and partitions

	brokenNode := data.NewGraphNode()
	if trans.StoreNode("b b", brokenNode) == nil || trans.StoreNode("main", brokenNode) == nil {
		t.Error("Error result was expected")
		return
	}
	if trans.UpdateNode("b b", brokenNode) == nil || trans.UpdateNode("main", brokenNode) == nil {
		t.Error("Error result was expected")
		return
	}
	if trans.RemoveNode("b b", "123", "bla") == nil {
		t.Error("Error result was expected")
		return
	}

	brokenEdge := data.NewGraphEdge()
	if trans.StoreEdge("b b", brokenEdge) == nil || trans.StoreEdge("main", brokenEdge) == nil {
		t.Error("Error result was expected")
		return
	}
	if trans.RemoveEdge("b b", "123", "bla") == nil {
		t.Error("Error result was expected")
		return
	}

	// Test update case if a node exists already in the datastore

	node3instore := data.NewGraphNode()
	node3instore.SetAttr("key", "789")
	node3instore.SetAttr("kind", "mynewkind")
	node3instore.SetAttr("Existing", "TestNode3")

	gm.StoreNode("main", node3instore)

	node3 := data.NewGraphNode()
	node3.SetAttr("key", "789")
	node3.SetAttr("kind", "mynewkind")
	node3.SetAttr("Name", "Node3")

	sm := gm.gs.StorageManager("main"+node3.Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	// Check that the update fails

	if err := trans.UpdateNode("main", node3); err.Error() !=
		"GraphError: Failed to access graph storage component (Slot not found (mystorage/mainmynewkind.nodes - Location:1))" {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 2, 0, 1, 0)

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	// Check that the update is converted to an insert with the updated node

	if err := trans.UpdateNode("main", node3); err != nil {
		t.Error(err)
		return
	}

	checkMaps(t, trans, "main", node1.Key(), node1.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node2.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", node2.Key(), node3.Kind(), true, false, false, false)
	checkMaps(t, trans, "main", edge1.Key(), edge1.Kind(), false, false, true, false)
	countMaps(t, trans, 3, 0, 1, 0)

	test = trans.storeNodes[trans.createKey("main", node3.Key(), node3.Kind())]
	if !data.NodeCompare(test, data.NodeMerge(node3instore, node3), nil) {
		t.Error("Unexpected update result:", test)
		return
	}
}

func checkMaps(t *testing.T, trans *Trans, part string, ikey string, ikind string,
	nodeStore bool, nodeRemove bool, edgeStore bool, edgeRemove bool) {

	key := trans.createKey(part, ikey, ikind)

	if _, ok := trans.storeNodes[key]; ok != nodeStore {
		t.Error("Expected element is not in storesNodes:", key)
	}
	if _, ok := trans.removeNodes[key]; ok != nodeRemove {
		t.Error("Expected element is not in removeNodes:", key)
	}
	if _, ok := trans.storeEdges[key]; ok != edgeStore {
		t.Error("Expected element is not in storesEdges:", key)
	}
	if _, ok := trans.removeEdges[key]; ok != edgeRemove {
		t.Error("Expected element is not in removeEdges:", key)
	}
}

func countMaps(t *testing.T, trans *Trans, nodeStore int, nodeRemove int,
	edgeStore int, edgeRemove int) {

	if c := len(trans.storeNodes); c != nodeStore {
		t.Error("Unexpected storeNodes count:", c, " expected:", nodeStore)
	}
	if c := len(trans.removeNodes); c != nodeRemove {
		t.Error("Unexpected storeNodes count:", c, " expected:", nodeRemove)
	}
	if c := len(trans.storeEdges); c != edgeStore {
		t.Error("Unexpected storeNodes count:", c, " expected:", edgeStore)
	}
	if c := len(trans.removeEdges); c != edgeRemove {
		t.Error("Unexpected storeNodes count:", c, " expected:", edgeRemove)
	}
}

func TestTransErrors(t *testing.T) {
	testTransPanic(t)

	constructEdge := func(node1 data.Node, kind string, node2 data.Node) data.Edge {

		edge := data.NewGraphEdge()

		edge.SetAttr("key", "abc"+node1.Key()+node2.Key())
		edge.SetAttr("kind", kind)

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "node1")
		edge.SetAttr(data.EdgeEnd1Cascading, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "node2")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr(data.NodeName, "Edge "+kind)

		return edge
	}

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")

	gm := newGraphManagerNoRules(mgs)

	trans := NewGraphTrans(gm)

	resetTrans := func(namesuffix string) {
		trans = NewGraphTrans(gm)

		node1 := data.NewGraphNode()
		node1.SetAttr("key", "123")
		node1.SetAttr("kind", "mynode")
		node1.SetAttr("Name", "Node1"+namesuffix)

		node2 := data.NewGraphNode()
		node2.SetAttr("key", "456")
		node2.SetAttr("kind", "mynewnode")
		node2.SetAttr("Name", "Node2"+namesuffix)

		if err := trans.StoreNode("main", node1); err != nil {
			t.Error(err)
			return
		}
		if err := trans.StoreNode("main", node2); err != nil {
			t.Error(err)
			return
		}
		if err := trans.StoreEdge("main", constructEdge(node1, "myedge", node2)); err != nil {
			t.Error(err)
			return
		}
	}

	resetTransAndStorage := func() {

		mgs = graphstorage.NewMemoryGraphStorage("mystorage")

		gm = newGraphManagerNoRules(mgs)

		resetTrans("")
	}

	resetTransAndStorage()

	// Test an inaccessible edge index

	storage.MsmCallNumRollback = 0

	sm := mgs.StorageManager("main"+"myedge"+StorageSuffixEdgesIndex, true).(*storage.MemoryStorageManager)
	sm.AccessMap[3] = storage.AccessInsertError

	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Index error") {
		t.Error("Unexpected error return:", err)
		return
	}

	if storage.MsmCallNumRollback != 6 {
		t.Error("Unexpected number of rollback calls:", storage.MsmCallNumRollback)
	}

	delete(sm.AccessMap, 3)

	// Test node commit failures

	resetTransAndStorage()
	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodes, true).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessInsertError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTrans("")

	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodes, true).(*storage.MemoryStorageManager)
	sm.AccessMap[4] = storage.AccessInsertError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Could not write graph information") {
		fmt.Println(sm)
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 4)

	resetTransAndStorage()
	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodesIndex, true).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessInsertError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTransAndStorage()
	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodesIndex, true).(*storage.MemoryStorageManager)
	sm.AccessMap[2] = storage.AccessInsertError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Index error") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 2)

	resetTransAndStorage()
	if err := trans.Commit(); err != nil {
		t.Error(err)
	}
	resetTrans("123")
	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodesIndex, false).(*storage.MemoryStorageManager)
	sm.AccessMap[2] = storage.AccessCacheAndFetchError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Index error") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 2)

	trans2 := NewGraphTrans(gm)
	trans2.RemoveNode("main", "123", "mynode")

	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodesIndex, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTransAndStorage()
	if err := trans.Commit(); err != nil {
		t.Error(err)
	}

	trans2 = NewGraphTrans(gm)
	trans2.RemoveNode("main", "123", "mynode")

	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodesIndex, false).(*storage.MemoryStorageManager)
	sm.AccessMap[2] = storage.AccessCacheAndFetchError

	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Index error") {
		t.Error("Unexpected error return:", err)
		return
	}

	delete(sm.AccessMap, 2)

	resetTransAndStorage()
	if err := trans.Commit(); err != nil {
		t.Error(err)
	}

	trans2 = NewGraphTrans(gm)
	trans2.RemoveNode("main", "123", "mynode")

	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	trans2.RemoveNode("main", "123", "mynode")

	sm.AccessMap[3] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Could not write graph information") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 3)

	// Test edge errors

	resetTransAndStorage()

	sm = mgs.StorageManager("main"+"myedge"+StorageSuffixEdgesIndex, true).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessInsertError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTrans("")

	sm = mgs.StorageManager("main"+"myedge"+StorageSuffixEdges, true).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessInsertError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTrans("")

	sm.AccessMap[2] = storage.AccessInsertError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Could not write graph information") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 2)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("Name", "Node1")

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynewnode")
	node2.SetAttr("Name", "Node2")

	node3 := data.NewGraphNode()
	node3.SetAttr("key", "xxx")
	node3.SetAttr("kind", "mynode2")
	node3.SetAttr("Name", "Node3")

	if err := trans.StoreEdge("main", constructEdge(node3, "myedge", node3)); err != nil {
		t.Error(err)
		return
	}
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Invalid data") {
		t.Error("Unexpected error return:", err)
		return
	}

	node3 = data.NewGraphNode()
	node3.SetAttr("key", "xxx")
	node3.SetAttr("kind", "mynode")
	node3.SetAttr("Name", "Node3")

	if err := trans.StoreEdge("main", constructEdge(node3, "myedge", node3)); err != nil {
		t.Error(err)
		return
	}
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Invalid data") {
		t.Error("Unexpected error return:", err)
		return
	}

	resetTransAndStorage()
	trans.Commit()
	resetTrans("")

	node3 = data.NewGraphNode()
	node3.SetAttr("key", "xxx")
	node3.SetAttr("kind", "mynode2")
	node3.SetAttr("Name", "Node3")

	if err := trans.StoreEdge("main", constructEdge(node1, "myedge", node3)); err != nil {
		t.Error(err)
		return
	}
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Invalid data") {
		t.Error("Unexpected error return:", err)
		return
	}

	node3 = data.NewGraphNode()
	node3.SetAttr("key", "xxx")
	node3.SetAttr("kind", "mynode")
	node3.SetAttr("Name", "Node3")

	if err := trans.StoreEdge("main", constructEdge(node1, "myedge", node3)); err != nil {
		t.Error(err)
		return
	}
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Invalid data") {
		t.Error("Unexpected error return:", err)
		return
	}

	resetTransAndStorage()
	trans.Commit()

	trans = NewGraphTrans(gm)
	if err := trans.StoreEdge("main", constructEdge(node1, "myedge", node2)); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+"mynode"+StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessCacheAndFetchError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTransAndStorage()
	trans.Commit()

	trans = NewGraphTrans(gm)
	if err := trans.StoreEdge("main", constructEdge(node1, "myedge", node2)); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+"mynewnode"+StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessCacheAndFetchError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTransAndStorage()
	trans.Commit()

	trans = NewGraphTrans(gm)
	if err := trans.StoreEdge("main", constructEdge(node1, "myedge", node2)); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+"myedge"+StorageSuffixEdgesIndex, false).(*storage.MemoryStorageManager)
	sm.AccessMap[4] = storage.AccessCacheAndFetchError
	if err := trans.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Index error") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 4)

	// Test edge deletion errors

	deleteEdge := constructEdge(node1, "myedge", node2)

	resetTransAndStorage()
	trans.Commit()

	trans2 = NewGraphTrans(gm)
	if err := trans2.RemoveEdge("main", deleteEdge.Key(), deleteEdge.Kind()); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+"myedge"+StorageSuffixEdgesIndex, false).(*storage.MemoryStorageManager)
	sm.AccessMap[2] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Index error") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 2)

	resetTransAndStorage()
	trans.Commit()

	trans2 = NewGraphTrans(gm)
	if err := trans2.RemoveEdge("main", deleteEdge.Key(), deleteEdge.Kind()); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+"myedge"+StorageSuffixEdgesIndex, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	trans2 = NewGraphTrans(gm)
	if err := trans2.RemoveEdge("main", deleteEdge.Key(), deleteEdge.Kind()); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+"myedge"+StorageSuffixEdges, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	trans2 = NewGraphTrans(gm)
	if err := trans2.RemoveEdge("main", deleteEdge.Key(), deleteEdge.Kind()); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+"myedge"+StorageSuffixEdges, false).(*storage.MemoryStorageManager)
	sm.AccessMap[2] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Could not write graph information") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 2)

	resetTransAndStorage()
	trans.Commit()

	trans2 = NewGraphTrans(gm)
	if err := trans2.RemoveEdge("main", deleteEdge.Key(), deleteEdge.Kind()); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+deleteEdge.End1Kind()+StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTransAndStorage()
	trans.Commit()

	trans2 = NewGraphTrans(gm)
	if err := trans2.RemoveEdge("main", deleteEdge.Key(), deleteEdge.Kind()); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+deleteEdge.End2Kind()+StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 1)

	resetTransAndStorage()
	trans.Commit()

	trans2 = NewGraphTrans(gm)
	if err := trans2.RemoveEdge("main", deleteEdge.Key(), deleteEdge.Kind()); err != nil {
		t.Error(err)
		return
	}

	sm = mgs.StorageManager("main"+deleteEdge.End2Kind()+StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	sm.AccessMap[5] = storage.AccessCacheAndFetchError
	if err := trans2.Commit(); !strings.Contains(fmt.Sprint(err), "GraphError: Could not read graph information") {
		t.Error("Unexpected error return:", err)
		return
	}
	delete(sm.AccessMap, 5)
}

func testTransPanic(t *testing.T) {
	defer func() {
		graphstorage.MgsRetFlushMain = nil

		if r := recover(); r == nil {
			t.Error("Transaction with a serious write error (during flushing) did not cause a panic.")
		}
	}()

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")

	gm := newGraphManagerNoRules(mgs)

	gm.getNodeStorageHTree("main", "mynode", true)

	trans := NewGraphTrans(gm)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")

	trans.StoreNode("main", node1)

	graphstorage.MgsRetFlushMain = errors.New("test")

	trans.Commit()
}
