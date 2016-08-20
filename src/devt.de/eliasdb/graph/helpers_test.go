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
	"testing"

	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/storage"
)

func TestSanityChecks(t *testing.T) {
	gs := graphstorage.NewMemoryGraphStorage("test")
	gm := newGraphManagerNoRules(gs)

	if gm.checkPartitionName("abba") != nil {
		t.Error("Partition name should be valid")
		return
	}

	if err := gm.checkPartitionName("abba 5").Error(); err != "GraphError: "+
		"Invalid data (Partition name abba 5 is not alphanumeric - can only "+
		"contain [a-zA-Z0-9_])" {

		t.Error("Partition name should be valid", err)
		return
	}

	node := data.NewGraphNode()

	if err := gm.checkNode(node); err.Error() != "GraphError: Invalid data "+
		"(Node is missing a key value)" {

		t.Error("Unexpected result:", err)
		return
	}

	node.SetAttr("key", "123")

	if err := gm.checkNode(node); err.Error() != "GraphError: Invalid data (Node "+
		"is missing a kind value)" {

		t.Error("Unexpected result:", err)
		return
	}

	node.SetAttr("kind", "123 3")

	if err := gm.checkNode(node); err.Error() != "GraphError: Invalid data (Node "+
		"kind 123 3 is not alphanumeric - can only contain [a-zA-Z0-9_])" {

		t.Error("Unexpected result:", err)
		return
	}

	node.SetAttr("kind", "123")

	node.SetAttr("", "123")

	if err := gm.checkNode(node); err.Error() != "GraphError: Invalid data "+
		"(Node contains empty string attribute name)" {

		t.Error("Unexpected result:", err)
		return
	}

	delete(node.Data(), "")

	if err := gm.checkNode(node); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if err := gm.writeNodeCount("bla", 42, true); err != nil {
		t.Error(err)
		return
	}

	if cnt := gm.NodeCount("bla"); cnt != 42 {
		t.Error("Invalid node count:", cnt)
		return
	}

	if cnt := gm.NodeCount("bla1"); cnt != 0 {
		t.Error("Invalid node count:", cnt)
		return
	}

	if err := gm.writeEdgeCount("bla2", 55, true); err != nil {
		t.Error(err)
		return
	}

	if cnt := gm.EdgeCount("bla2"); cnt != 55 {
		t.Error("Invalid edge count:", cnt)
		return
	}

	if cnt := gm.EdgeCount("bla"); cnt != 0 {
		t.Error("Invalid edge count:", cnt)
		return
	}

	gs.StorageManager("blabla"+StorageSuffixNodes, true)
	gs.StorageManager("blabla"+StorageSuffixNodesIndex, true)
	gs.StorageManager("blabla"+StorageSuffixEdges, true)
	gs.StorageManager("blabla"+StorageSuffixEdgesIndex, true)

	storage.MsmRetFlush = errors.New("Test")

	if res := gm.flushNodeStorage("bla", "bla").Error(); res !=
		"GraphError: Failed to flush changes (Test)" {

		t.Error("Unexpected flush result:", res)
		return
	}

	if res := gm.flushNodeIndex("bla", "bla").Error(); res !=
		"GraphError: Failed to flush changes (Test)" {

		t.Error("Unexpected flush result:", res)
		return
	}

	if res := gm.flushEdgeStorage("bla", "bla").Error(); res !=
		"GraphError: Failed to flush changes (Test)" {

		t.Error("Unexpected flush result:", res)
		return
	}

	if res := gm.flushEdgeIndex("bla", "bla").Error(); res !=
		"GraphError: Failed to flush changes (Test)" {

		t.Error("Unexpected flush result:", res)
		return
	}

	storage.MsmRetFlush = nil

	storage.MsmRetRollback = errors.New("Test")

	if res := gm.rollbackNodeStorage("bla", "bla").Error(); res !=
		"GraphError: Failed to rollback changes (Test)" {

		t.Error("Unexpected rollback result:", res)
		return
	}

	if res := gm.rollbackNodeIndex("bla", "bla").Error(); res !=
		"GraphError: Failed to rollback changes (Test)" {

		t.Error("Unexpected rollback result:", res)
		return
	}

	if res := gm.rollbackEdgeStorage("bla", "bla").Error(); res !=
		"GraphError: Failed to rollback changes (Test)" {

		t.Error("Unexpected rollback result:", res)
		return
	}

	if res := gm.rollbackEdgeIndex("bla", "bla").Error(); res !=
		"GraphError: Failed to rollback changes (Test)" {

		t.Error("Unexpected rollback result:", res)
		return
	}

	storage.MsmRetRollback = nil

	if gm.flushNodeStorage("bla", "bla") != nil {
		t.Error("Unexpected flush result")
		return
	}

	if gm.flushNodeIndex("bla", "bla2") != nil {
		t.Error("Unexpected flush result")
		return
	}

	if gm.flushEdgeStorage("bla", "bla") != nil {
		t.Error("Unexpected flush result")
		return
	}

	if gm.flushEdgeIndex("bla", "bla2") != nil {
		t.Error("Unexpected flush result")
		return
	}

	if gm.rollbackNodeStorage("bla", "bla") != nil {
		t.Error("Unexpected rollback result")
		return
	}

	if gm.rollbackNodeIndex("bla", "bla2") != nil {
		t.Error("Unexpected rollback result")
		return
	}

	if gm.rollbackEdgeStorage("bla", "bla") != nil {
		t.Error("Unexpected rollback result")
		return
	}

	if gm.rollbackEdgeIndex("bla", "bla2") != nil {
		t.Error("Unexpected rollback result")
		return
	}

	if res, res2, err := gm.getNodeStorageHTree("my part", "mykind", false); res != nil ||
		res2 != nil || err == nil {
		t.Error("Unexpected Error", err)
		return
	}

	if res, res2, err := gm.getNodeStorageHTree("mypart", "my kind", false); res != nil ||
		res2 != nil || err.Error() !=
		"GraphError: Invalid data (Node kind my kind is not alphanumeric - can only contain [a-zA-Z0-9_])" {

		t.Error("Unexpected Error", err)
		return
	}

	if res, res2, err := gm.getNodeStorageHTree("mypart", "mykind", false); res != nil ||
		res2 != nil || err != nil {
		t.Error("Non existing node storage tree should not be created here:", res, err)
		return
	}

	res, res2, err := gm.getNodeStorageHTree("mypart", "mykind", true)
	if err != nil || res == nil || res2 == nil {
		t.Error(res, err)
		return
	}

	sm := gm.gs.StorageManager("mypart"+"mykind"+StorageSuffixNodes, false)

	oldroot := sm.Root(RootIDNodeHTree)
	sm.SetRoot(RootIDNodeHTree, 5)
	sm.(*storage.MemoryStorageManager).AccessMap[5] = storage.AccessCacheAndFetchError

	_, _, err = gm.getNodeStorageHTree("mypart", "mykind", true)
	if err.Error() != "GraphError: Failed to access graph storage component (Slot not found (test/mypartmykind.nodes - Location:5))" {
		t.Error(err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 5)
	sm.SetRoot(RootIDNodeHTree, oldroot)

	oldroot = sm.Root(RootIDNodeHTreeSecond)
	sm.SetRoot(RootIDNodeHTreeSecond, 5)
	sm.(*storage.MemoryStorageManager).AccessMap[5] = storage.AccessCacheAndFetchError

	_, _, err = gm.getNodeStorageHTree("mypart", "mykind", true)
	if err.Error() != "GraphError: Failed to access graph storage component (Slot not found (test/mypartmykind.nodes - Location:5))" {
		t.Error(err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 5)
	sm.SetRoot(RootIDNodeHTreeSecond, oldroot)

	if res, err := gm.getEdgeStorageHTree("my part", "mykind", false); res != nil || err == nil {
		t.Error("Unexpected Error", err)
		return
	}

	if res, err := gm.getEdgeStorageHTree("mypart", "my kind", false); res != nil || err.Error() !=
		"GraphError: Invalid data (Edge kind my kind is not alphanumeric - can only contain [a-zA-Z0-9_])" {

		t.Error("Unexpected Error", err)
		return
	}

	if res, err := gm.getEdgeStorageHTree("mypart", "mykind", false); res != nil || err != nil {
		t.Error("Non existing node storage tree should not be created here:", res, err)
		return
	}

	res, err = gm.getEdgeStorageHTree("mypart", "mykind", true)
	if err != nil || res == nil {
		t.Error(res, err)
		return
	}

	if cnt := len(gs.MainDB()); cnt != 11 {
		t.Error("Unexpected number of main db entries:", cnt)
		return
	}

	if _, ok := gs.MainDB()[MainDBNodeAttrs+"mykind"]; !ok {
		t.Error("Missing main db entry")
		return
	}
	if _, ok := gs.MainDB()[MainDBNodeEdges+"mykind"]; !ok {
		t.Error("Missing main db entry")
		return
	}
	if _, ok := gs.MainDB()[MainDBNodeCount+"mykind"]; !ok {
		t.Error("Missing main db entry")
		return
	}

	edge := data.NewGraphEdge()

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a key value)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.NodeKey, "123")
	edge.SetAttr(data.NodeKind, "myedge")

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a key value for end1)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd1Key, "456")

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a kind value for end1)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd1Kind, "mykind1")

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a role value for end1)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd1Role, "myrole1")
	edge.SetAttr(data.EdgeEnd1Cascading, "wrong")

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a cascading value for end1)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd1Cascading, false)

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a key value for end2)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd2Key, "456")

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a kind value for end2)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd2Kind, "mykind1")

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a role value for end2)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd2Role, "myrole1")

	if err := gm.checkEdge(edge); err.Error() != "GraphError: Invalid data (Edge is missing a cascading value for end2)" {
		t.Error("Unexpected result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd2Cascading, true)

	if err := gm.checkEdge(edge); err != nil {
		t.Error("Unexpected result:", err)
		return
	}
}

func TestStringLists(t *testing.T) {

	stringmap := map[string]string{
		"test1": "testval1",
	}

	str := mapToString(stringmap)

	stringmap2 := stringToMap(str)

	if fmt.Sprint(stringmap2) != "map[test1:testval1]" {
		t.Error("Unexpected decoded list:", stringmap2)
		return
	}

	testStringDecodePanic(t)

	gm := newGraphManagerNoRules(graphstorage.NewMemoryGraphStorage("test"))

	// Test map storage

	gm.storeMainDBMap("mycoolmap", stringmap)

	// Remove the cached

	delete(gm.mapCache, "mycoolmap")

	// Map is a copy

	map1 := gm.getMainDBMap("mycoolmap")
	if &map1 != &stringmap && fmt.Sprint(map1) != fmt.Sprint(stringmap) {
		t.Error("Unexpected map return:", map1)
		return
	}

	// Map comes now from the cache

	map2 := gm.getMainDBMap("mycoolmap")
	if &map1 == &map2 {
		t.Error("Unexpected map return:", map1)
		return
	}
}

func testStringDecodePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Decoding a non encoded string did not cause a panic.")
		}
	}()

	stringToMap("test")
}
