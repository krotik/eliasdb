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

	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
	"devt.de/krotik/eliasdb/graph/util"
	"devt.de/krotik/eliasdb/hash"
	"devt.de/krotik/eliasdb/storage"
)

func TestSimpleGraphStorage(t *testing.T) {
	if !RunDiskStorageTests {
		return
	}

	dgs, err := graphstorage.NewDiskGraphStorage(GraphManagerTestDBDir3, false)
	if err != nil {
		t.Error(err)
		return
	}

	gm := newGraphManagerNoRules(dgs)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mykind")
	node1.SetAttr("Name", "Node1")

	gm.StoreNode("main", node1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mykind")
	node2.SetAttr("Name", "Node2")

	gm.StoreNode("main", node2)

	node3 := data.NewGraphNode()
	node3.SetAttr("key", "789")
	node3.SetAttr("kind", "mykind")
	node3.SetAttr("Name", "Node3")

	gm.StoreNode("main", node3)

	edge := data.NewGraphEdge()

	edge.SetAttr("key", "abc")
	edge.SetAttr("kind", "myedge")

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "node1")
	edge.SetAttr(data.EdgeEnd1Cascading, true)

	edge.SetAttr(data.EdgeEnd2Key, node2.Key())
	edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "node2")
	edge.SetAttr(data.EdgeEnd2Cascading, false)

	err = gm.StoreEdge("main", edge)
	if err != nil {
		t.Error(err)
	}

	if gm.EdgeCount("myedge") != 1 {
		t.Error("Unexpected edge count")
		return
	}

	// Check that the correct data has been written

	edgeTree, _ := gm.getEdgeStorageHTree("main", "myedge", false)

	keyAttrs := PrefixNSAttrs + edge.Key()

	val, err := edgeTree.Get([]byte(keyAttrs))
	if err != nil || val == nil {
		t.Error("Unexpected result:", val, val)
		return
	}

	if res := len(val.([]string)); res != 8 {
		t.Error("Unexpected number of stored attributes:", res)
	}

	keyAttr := PrefixNSAttr + edge.Key() + gm.nm.Encode32(data.EdgeEnd2Key, false)

	if val, err := edgeTree.Get([]byte(keyAttr)); err != nil || val != node2.Key() {
		t.Error("Unexpected result:", err, val)
		return
	}

	_, nodeTree, _ := gm.getNodeStorageHTree("main", "mykind", false)

	specMap, err := nodeTree.Get([]byte(PrefixNSSpecs + node2.Key()))
	if err != nil || specMap == nil {
		t.Error("Unexpected result:", specMap, err)
	}

	if len(specMap.(map[string]string)) != 1 {
		t.Error("Unexpected size of spec map")
		return
	}

	if _, ok := specMap.(map[string]string)[gm.nm.Encode16(edge.End2Role(), false)+gm.nm.Encode16(edge.Kind(), false)+
		gm.nm.Encode16(edge.End1Role(), false)+gm.nm.Encode16(edge.End1Kind(), false)]; !ok {
		t.Error("Unexpected content of spec map")
		return
	}

	fetchedEdge, err := gm.FetchEdge("main", edge.Key(), edge.Kind())
	if err != nil {
		t.Error(err)
		return
	}

	if !data.NodeCompare(edge, fetchedEdge, nil) {
		t.Error("Fetched edge should contain the same data as the stored edge")
		return
	}

	// Try to change one of the endpoints

	edge.SetAttr(data.EdgeEnd1Key, node3.Key())

	err = gm.StoreEdge("main", edge)
	if err.Error() != "GraphError: Invalid data (Cannot update endpoints or spec of existing edge: abc)" {
		t.Error(err)
		return
	}

	// Try again to make sure it was not updated

	edge.SetAttr(data.EdgeEnd1Key, node3.Key())

	err = gm.StoreEdge("main", edge)
	if err.Error() != "GraphError: Invalid data (Cannot update endpoints or spec of existing edge: abc)" {
		t.Error(err)
	}

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr("Name", "Test")

	err = gm.StoreEdge("main", edge)
	if err != nil {
		t.Error(err)
		return
	}

	fetchedEdge, err = gm.FetchEdge("main", edge.Key(), edge.Kind())
	if err != nil {
		t.Error(err)
		return
	}

	if fetchedEdge.Attr("Name") != "Test" {
		t.Error("Unexpected attribute value")
		return
	}

	if gm.EdgeCount("myedge") != 1 {
		t.Error("Unexpected edge count")
		return
	}

	// Store more edges

	edge2 := data.NewGraphEdge()

	edge2.SetAttr("key", "def")
	edge2.SetAttr("kind", "myedge")

	edge2.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge2.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge2.SetAttr(data.EdgeEnd1Role, "node1")
	edge2.SetAttr(data.EdgeEnd1Cascading, true)

	edge2.SetAttr(data.EdgeEnd2Key, node3.Key())
	edge2.SetAttr(data.EdgeEnd2Kind, node3.Kind())
	edge2.SetAttr(data.EdgeEnd2Role, "node2")
	edge2.SetAttr(data.EdgeEnd2Cascading, false)

	err = gm.StoreEdge("main", edge2)
	if err != nil {
		t.Error(err)
		return
	}

	// Spec map has still one entry

	specMap, err = nodeTree.Get([]byte(PrefixNSSpecs + node1.Key()))
	if err != nil || specMap == nil {
		t.Error("Unexpected result:", specMap, err)
	}

	if len(specMap.(map[string]string)) != 1 {
		t.Error("Unexpected size of spec map")
		return
	}

	if gm.EdgeCount("myedge") != 2 {
		t.Error("Unexpected edge count")
		return
	}

	edge2.SetAttr("key", "ghi")

	err = gm.StoreEdge("main", edge2)
	if err != nil {
		t.Error(err)
		return
	}

	specMap, err = nodeTree.Get([]byte(PrefixNSSpecs + node1.Key()))
	if err != nil || specMap == nil {
		t.Error("Unexpected result:", specMap, err)
	}

	spec1 := gm.nm.Encode16(edge.End1Role(), true) + gm.nm.Encode16(edge.Kind(), true) +
		gm.nm.Encode16(edge.End2Role(), true) + gm.nm.Encode16(edge.End2Kind(), true)

	edgeInfo1Key := PrefixNSEdge + edge.End1Key() + spec1

	obj, err := nodeTree.Get([]byte(edgeInfo1Key))

	targetMap := obj.(map[string]*edgeTargetInfo)

	// There should be 3 entries in the target map at this point

	if len(targetMap) != 3 {
		t.Error("Unexpected size of target map")
		return
	}

	testInfo := targetMap["def"]

	if !testInfo.CascadeToTarget {
		t.Error("Edge should cascade to target from end1")
		return
	}
	if testInfo.TargetNodeKey != node3.Key() {
		t.Error("Edge should go to node3")
		return
	}
	if testInfo.TargetNodeKind != node3.Kind() {
		t.Error("Edge should go to node3")
		return
	}

	if len(specMap.(map[string]string)) != 1 {
		t.Error("Unexpected size of spec map")
		return
	}

	// At this point there are 3 relationships in the db:

	// node1 -> node2 [abc]
	// node1 -> node3 [def]
	// node1 -> node3 [ghi]

	if gm.EdgeCount("myedge") != 3 {
		t.Error("Unexpected edge count")
		return
	}

	// Test index lookup

	iq, _ := gm.EdgeIndexQuery("main", "myedge")
	res, _ := iq.LookupValue("Name", "test")

	if fmt.Sprint(res) != "["+edge.Key()+"]" {
		t.Error("Unexpected result:", res)
		return
	}

	// Test removal

	removedEdge, err := gm.RemoveEdge("main", edge.Key(), edge.Kind())

	if !data.NodeCompare(edge, removedEdge, nil) {
		t.Error("Unexpected result")
		return
	}

	if gm.EdgeCount("myedge") != 2 {
		t.Error("Unexpected edge count")
		return
	}

	// Check that the correct data has been removed

	edgeTree, _ = gm.getEdgeStorageHTree("main", "myedge", false)

	keyAttrs = PrefixNSAttrs + edge.Key()

	val, err = edgeTree.Get([]byte(keyAttrs))
	if err != nil || val != nil {
		t.Error("Unexpected result:", val, val)
		return
	}

	// Check that the spec entry is still there

	specMap, err = nodeTree.Get([]byte(PrefixNSSpecs + node1.Key()))
	if err != nil || specMap == nil {
		t.Error("Unexpected result:", specMap, err)
		return
	}

	if len(specMap.(map[string]string)) != 1 {
		t.Error("Unexpected size of spec map")
		return
	}

	obj, err = nodeTree.Get([]byte(edgeInfo1Key))

	targetMap = obj.(map[string]*edgeTargetInfo)

	// There should be 2 entries in the target map at this point

	if len(targetMap) != 2 {
		t.Error("Unexpected size of target map")
		return
	}

	removedEdge, err = gm.RemoveEdge("main", edge2.Key(), edge2.Kind())

	if !data.NodeCompare(edge2, removedEdge, nil) {
		t.Error("Unexpected result")
		return
	}

	if gm.EdgeCount("myedge") != 1 {
		t.Error("Unexpected edge count")
		return
	}

	// Check that the spec entry is still there

	specMap, err = nodeTree.Get([]byte(PrefixNSSpecs + node1.Key()))
	if err != nil || specMap == nil {
		t.Error("Unexpected result:", specMap, err)
		return
	}

	if len(specMap.(map[string]string)) != 1 {
		t.Error("Unexpected size of spec map")
		return
	}

	specMap, err = nodeTree.Get([]byte(PrefixNSSpecs + node3.Key()))
	if err != nil || specMap == nil {
		t.Error("Unexpected result:", specMap, err)
		return
	}

	if len(specMap.(map[string]string)) != 1 {
		t.Error("Unexpected size of spec map")
		return
	}

	obj, err = nodeTree.Get([]byte(edgeInfo1Key))

	targetMap = obj.(map[string]*edgeTargetInfo)

	// There should be 1 entries in the target map at this point

	if len(targetMap) != 1 {
		t.Error("Unexpected size of target map")
		return
	}

	edge2.SetAttr(data.NodeKey, "def")

	removedEdge, err = gm.RemoveEdge("main", edge2.Key(), edge2.Kind())

	if !data.NodeCompare(edge2, removedEdge, nil) {
		t.Error("Unexpected result")
		return
	}

	if gm.EdgeCount("myedge") != 0 {
		t.Error("Unexpected edge count")
		return
	}

	// Check that the spec entry has been removed

	specMap, err = nodeTree.Get([]byte(PrefixNSSpecs + node1.Key()))
	if err != nil || specMap != nil {
		t.Error("Unexpected result:", specMap, err)
		return
	}

	specMap, err = nodeTree.Get([]byte(PrefixNSSpecs + node2.Key()))
	if err != nil || specMap != nil {
		t.Error("Unexpected result:", specMap, err)
		return
	}

	specMap, err = nodeTree.Get([]byte(PrefixNSSpecs + node3.Key()))
	if err != nil || specMap != nil {
		t.Error("Unexpected result:", specMap, err)
		return
	}

	// Check that the target map has been removed

	obj, err = nodeTree.Get([]byte(edgeInfo1Key))
	if err != nil || obj != nil {
		t.Error("Unexpected result:", specMap, err)
		return
	}

	it := hash.NewHTreeIterator(edgeTree)
	if it.HasNext() {
		t.Error("Tree iterator should find no elements in the tree")
		return
	}

	dgs.Close()
}

func TestSimpleGraphStorageErrorCases(t *testing.T) {

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mykind")
	node1.SetAttr("Name", "Node1")

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynewkind")
	node2.SetAttr("Name", "Node2")

	// Creeate storage and insert test nodes

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := newGraphManagerNoRules(mgs)
	gm.StoreNode("main", node1)
	gm.StoreNode("main", node2)

	edge := data.NewGraphEdge()

	edge.SetAttr("key", "abc")
	edge.SetAttr("kind", "myedge")

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "node1-")
	edge.SetAttr(data.EdgeEnd1Cascading, true)

	edge.SetAttr(data.EdgeEnd2Key, node2.Key())
	edge.SetAttr(data.EdgeEnd2Kind, "xxx")
	edge.SetAttr(data.EdgeEnd2Role, "node2-")
	edge.SetAttr(data.NodeName, "Edge name")

	_, _, err := gm.Traverse("main", node1.Key(), node2.Kind(), "abc", false)
	if err.Error() != "GraphError: Invalid data (Invalid spec: abc)" {
		t.Error("Unexpected store result:", err)
		return
	}

	_, _, err = gm.Traverse("main", node1.Key(), node2.Kind(), ":::abc", false)
	if err.Error() != "GraphError: Invalid data (Invalid spec: :::abc - spec needs to be fully specified for direct traversal)" {
		t.Error("Unexpected store result:", err)
		return
	}

	_, _, err = gm.TraverseMulti("main", node1.Key(), node2.Kind(), "abc", false)
	if err.Error() != "GraphError: Invalid data (Invalid spec: abc)" {
		t.Error("Unexpected store result:", err)
		return
	}

	if err := gm.StoreEdge("main", edge); err.Error() !=
		"GraphError: Invalid data (Edge role node1- is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error("Unexpected store result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd1Role, "node1")

	if err := gm.StoreEdge("main", edge); err.Error() !=
		"GraphError: Invalid data (Edge role node2- is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error("Unexpected store result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd2Role, "node2")

	if err := gm.StoreEdge("main", edge); err.Error() !=
		"GraphError: Invalid data (Edge is missing a cascading value for end2)" {
		t.Error("Unexpected store result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd2Cascading, false)

	if err := gm.StoreEdge("main", edge); err.Error() != "GraphError: Invalid data (Can't store edge to non-existing node kind: xxx)" {
		t.Error("Unexpected store result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
	edge.SetAttr(data.EdgeEnd1Kind, "xxx")

	if err := gm.StoreEdge("main", edge); err.Error() != "GraphError: Invalid data (Can't store edge to non-existing node kind: xxx)" {
		t.Error("Unexpected store result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Key, "xxx")

	if err := gm.StoreEdge("main", edge); err.Error() != "GraphError: Invalid data (Can't find edge endpoint: xxx (mykind))" {
		t.Error("Unexpected store result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd2Key, "xxx")

	if err := gm.StoreEdge("main", edge); err.Error() != "GraphError: Invalid data (Can't find edge endpoint: xxx (mynewkind))" {
		t.Error("Unexpected store result:", err)
		return
	}

	edge.SetAttr(data.EdgeEnd2Key, node2.Key())

	// Test storage access failures

	sm := gm.gs.StorageManager("main"+"myedge"+StorageSuffixEdgesIndex, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	if err := gm.StoreEdge("main", edge); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}
	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)
	sm.(*storage.MemoryStorageManager).AccessMap[2] = storage.AccessInsertError

	if err := gm.StoreEdge("main", edge); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 2)

	// Reset storage and insert test nodes

	mgs = graphstorage.NewMemoryGraphStorage("mystorage")
	gm = newGraphManagerNoRules(mgs)
	gm.StoreNode("main", node1)
	gm.StoreNode("main", node2)

	// Test high level errors

	sm = gm.gs.StorageManager("main"+"mykind"+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	if err := gm.StoreEdge("main", edge); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	if _, _, err := gm.Traverse("main", "bla", "mykind", "", false); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	sm = gm.gs.StorageManager("main"+"myedge"+StorageSuffixEdges, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	if _, err := gm.FetchEdge("main", edge.Key(), edge.Kind()); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	sm = gm.gs.StorageManager("main"+"mynewkind"+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	if err := gm.StoreEdge("main", edge); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	graphstorage.MgsRetFlushMain = &util.GraphError{Type: util.ErrFlushing, Detail: errors.New("Test").Error()}

	if err := gm.StoreEdge("main", edge); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}
	graphstorage.MgsRetFlushMain = nil

	// Finally insert the edge

	if err := gm.StoreEdge("main", edge); err != nil {
		t.Error("Unexpected store result:", err)
		return
	}

	sm = gm.gs.StorageManager("main"+"myedge"+StorageSuffixEdges, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	traverseSpec := edge.End2Role() + ":" + edge.Kind() + ":" + edge.End1Role() + ":" + edge.End1Kind()
	_, _, err = gm.Traverse("main", edge.End2Key(), edge.End2Kind(), traverseSpec, true)

	if !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	sm = gm.gs.StorageManager("main"+"myedge"+StorageSuffixEdgesIndex, false)
	sm.(*storage.MemoryStorageManager).AccessMap[5] = storage.AccessInsertError

	edge.SetAttr("name", "New edge name")

	if err := gm.StoreEdge("main", edge); err == nil {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 5)

	resetStorage := func() {
		mgs = graphstorage.NewMemoryGraphStorage("mystorage")
		gm = newGraphManagerNoRules(mgs)
		gm.StoreNode("main", node1)
		gm.StoreNode("main", node2)
		gm.StoreEdge("main", edge)
	}

	// Test low level errors

	spec1 := gm.nm.Encode16(edge.End1Role(), true) + gm.nm.Encode16(edge.Kind(), true) +
		gm.nm.Encode16(edge.End2Role(), true) + gm.nm.Encode16(edge.End2Kind(), true)

	spec2 := gm.nm.Encode16(edge.End2Role(), true) + gm.nm.Encode16(edge.Kind(), true) +
		gm.nm.Encode16(edge.End1Role(), true) + gm.nm.Encode16(edge.End1Kind(), true)

	specsNode1Key := PrefixNSSpecs + edge.End1Key()
	edgeInfo1Key := PrefixNSEdge + edge.End1Key() + spec1

	specsNode2Key := PrefixNSSpecs + edge.End2Key()
	edgeInfo2Key := PrefixNSEdge + edge.End2Key() + spec2

	resetStorage()

	// Test error case of index lookup

	sm = gm.gs.StorageManager("main"+edge.Kind()+StorageSuffixEdgesIndex, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	if _, err := gm.EdgeIndexQuery("main", "myedge"); err == nil {
		t.Error(err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	_, nodeTree, _ := gm.getNodeStorageHTree("main", edge.End2Kind(), false)
	_, loc, _ := nodeTree.GetValueAndLocation([]byte(specsNode2Key))

	sm = gm.gs.StorageManager("main"+edge.End2Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.AccessCacheAndFetchError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	if err := gm.StoreEdge("main", edge); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, loc)

	resetStorage()

	_, nodeTree, _ = gm.getNodeStorageHTree("main", edge.End1Kind(), false)
	_, _ = nodeTree.Remove([]byte(specsNode1Key))

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Expected spec entry is missing") {
		t.Error("Unexpected store result:", err)
		return
	}

	resetStorage()

	_, nodeTree, _ = gm.getNodeStorageHTree("main", edge.End2Kind(), false)
	_, loc, _ = nodeTree.GetValueAndLocation([]byte(specsNode2Key))

	sm = gm.gs.StorageManager("main"+edge.End2Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.AccessFreeError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, loc)

	resetStorage()

	_, nodeTree, _ = gm.getNodeStorageHTree("main", edge.End1Kind(), false)
	val, loc, _ := nodeTree.GetValueAndLocation([]byte(specsNode1Key))
	val.(map[string]string)["test2"] = "test3"

	sm = gm.gs.StorageManager("main"+edge.End1Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.AccessUpdateError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	if err := gm.StoreEdge("main", edge); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, loc)

	resetStorage()

	_, nodeTree, _ = gm.getNodeStorageHTree("main", edge.End2Kind(), false)
	_, loc, _ = nodeTree.GetValueAndLocation([]byte(edgeInfo2Key))

	sm = gm.gs.StorageManager("main"+edge.End2Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.AccessCacheAndFetchError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	if err := gm.StoreEdge("main", edge); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	traverseSpec = edge.End2Role() + ":" + edge.Kind() + ":" + edge.End1Role() + ":" + edge.End1Kind()
	_, _, err = gm.Traverse("main", edge.End2Key(), edge.End2Kind(), traverseSpec, false)

	if !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	traverseSpec = ":" + edge.Kind() + ":" + edge.End1Role() + ":" + edge.End1Kind()

	_, _, err = gm.TraverseMulti("main", edge.End2Key(), edge.End2Kind(), traverseSpec, false)

	if !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, loc)

	resetStorage()

	_, nodeTree, _ = gm.getNodeStorageHTree("main", edge.End1Kind(), false)
	_, _ = nodeTree.Remove([]byte(edgeInfo1Key))

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Expected edgeTargetInfo entry is missing") {
		t.Error("Unexpected store result:", err)
		return
	}

	resetStorage()

	_, nodeTree, _ = gm.getNodeStorageHTree("main", edge.End2Kind(), false)
	_, loc, _ = nodeTree.GetValueAndLocation([]byte(edgeInfo2Key))

	sm = gm.gs.StorageManager("main"+edge.End2Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.AccessFreeError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, loc)

	resetStorage()

	nodeattTree, nodeTree, _ := gm.getNodeStorageHTree("main", edge.End1Kind(), false)
	val, loc, _ = nodeTree.GetValueAndLocation([]byte(edgeInfo1Key))
	val.(map[string]*edgeTargetInfo)["test2"] = nil

	sm = gm.gs.StorageManager("main"+edge.End1Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.AccessUpdateError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	if err := gm.StoreEdge("main", edge); !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, loc)

	_, loc, _ = nodeattTree.GetValueAndLocation([]byte(PrefixNSAttrs + edge.End1Key()))

	sm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.AccessCacheAndFetchError

	traverseSpec = edge.End2Role() + ":" + edge.Kind() + ":" + edge.End1Role() + ":" + edge.End1Kind()
	_, _, err = gm.Traverse("main", edge.End2Key(), edge.End2Kind(), traverseSpec, true)

	if !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, loc)

	mgs = graphstorage.NewMemoryGraphStorage("mystorage")
	gm = newGraphManagerNoRules(mgs)
	gm.StoreNode("main", node1)
	gm.StoreNode("main", node2)

	sm = gm.gs.StorageManager("main"+edge.Kind()+StorageSuffixEdges, true)
	sm.(*storage.MemoryStorageManager).AccessMap[11] = storage.AccessInsertError

	if err := gm.StoreEdge("main", edge); !strings.Contains(err.Error(), "Could not write graph information") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 11)

	resetStorage()

	sm = gm.gs.StorageManager("main"+edge.Kind()+StorageSuffixEdges, false)
	sm.(*storage.MemoryStorageManager).AccessMap[11] = storage.AccessCacheAndFetchError

	traverseSpec = edge.End1Role() + ":" + edge.Kind() + ":" + edge.End2Role() + ":" + edge.End2Kind()
	_, _, err = gm.Traverse("main", edge.End1Key(), edge.End1Kind(), traverseSpec, true)

	if !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	sm.(*storage.MemoryStorageManager).AccessMap[11] = storage.AccessFreeError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Could not write graph information") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 11)

	resetStorage()

	sm = gm.gs.StorageManager("main"+edge.End1Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Failed to access graph storage component") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	resetStorage()

	sm = gm.gs.StorageManager("main"+edge.End2Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	traverseSpec = edge.End1Role() + ":" + edge.Kind() + ":" + edge.End2Role() + ":" + edge.End2Kind()
	_, _, err = gm.Traverse("main", edge.End1Key(), edge.End1Kind(), traverseSpec, true)

	if !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected store result:", err)
		return
	}

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Failed to access graph storage component") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	resetStorage()

	sm = gm.gs.StorageManager("main"+edge.Kind()+StorageSuffixEdgesIndex, false)
	sm.(*storage.MemoryStorageManager).AccessMap[2] = storage.AccessFreeError

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); !strings.Contains(err.Error(), "Index error") {
		t.Error("Unexpected store result:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 2)

	// Test removal of non-existing edge

	resetStorage()

	if obj, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); obj == nil || err != nil {
		t.Error("Unexpected store result:", obj, err)
		return
	}

	if obj, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); obj != nil || err != nil {
		t.Error("Unexpected store result:", obj, err)
		return
	}
}

func TestEdgeOperations(t *testing.T) {

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mykind")
	node1.SetAttr("Name", "Node1")

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
		edge.SetAttr(data.EdgeEnd1CascadingLast, true)

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
	if err := gm.StoreNode("main", node1); err != nil {
		t.Error(err)
	}
	if err := gm.StoreNode("main", node2); err != nil {
		t.Error(err)
	}
	edge1 := constructEdge("myedge")
	if err := gm.StoreEdge("main", edge1); err != nil {
		t.Error(err)
	}
	edge2 := constructEdge("myotheredge")
	if err := gm.StoreEdge("main", edge2); err != nil {
		t.Error(err)
	}

	specs, err := gm.FetchNodeEdgeSpecs("main", node1.Key(), node1.Kind())
	if err != nil {
		t.Error(err)
		return
	}

	if fmt.Sprint(specs) != "[node1:myedge:node2:mynewkind node1:myotheredge:node2:mynewkind]" {
		t.Error("Unexpected specs result:", specs)
		return
	}

	sm := gm.gs.StorageManager("main"+node1.Kind()+StorageSuffixNodes, false)
	sm.(*storage.MemoryStorageManager).AccessMap[1] = storage.AccessCacheAndFetchError

	_, err = gm.FetchNodeEdgeSpecs("main", node1.Key(), node1.Kind())
	if err.Error() != "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainmykind.nodes - Location:1))" {
		t.Error("Unexpected error:", err)
		return
	}

	_, _, err = gm.TraverseMulti("main", node1.Key(), node1.Kind(), ":::", false)
	if err.Error() != "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainmykind.nodes - Location:1))" {
		t.Error("Unexpected error:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, 1)

	_, tree, _ := gm.getNodeStorageHTree("main", node1.Kind(), false)
	_, loc, _ := tree.GetValueAndLocation([]byte(PrefixNSSpecs + node1.Key()))

	sm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.AccessCacheAndFetchError

	_, err = gm.FetchNodeEdgeSpecs("main", node1.Key(), node1.Kind())
	if !strings.Contains(err.Error(), "Slot not found") {
		t.Error("Unexpected error:", err)
		return
	}

	delete(sm.(*storage.MemoryStorageManager).AccessMap, loc)

	nodes, edges, err := gm.TraverseMulti("main", node1.Key(), node1.Kind(),
		"node1:myotheredge:node2:mynewkind", false)
	if err != nil {
		t.Error(err)
		return
	}

	if len(nodes) != 1 || len(edges) != 1 {
		t.Error("Unexpected result:", nodes, edges)
		return
	}

	if !data.NodeCompare(edges[0], edge2, []string{data.NodeKey, data.NodeKind,
		data.EdgeEnd1Key, data.EdgeEnd1Kind, data.EdgeEnd1Role, data.EdgeEnd1Cascading,
		data.EdgeEnd2Key, data.EdgeEnd2Kind, data.EdgeEnd2Role, data.EdgeEnd2Cascading}) {

		t.Error("Edges should match:", edge2, edges[0])
		return
	}

	if !data.NodeCompare(nodes[0], node2, []string{data.NodeKey, data.NodeKind}) {
		t.Error("Nodes should match:", node2, nodes[0])
		return
	}

	// Now lookup from the other side

	nodes2, edges2, err := gm.Traverse("main", node2.Key(), node2.Kind(),
		"node2:myotheredge:node1:mykind", false)
	if err != nil {
		t.Error(err)
		return
	}

	if len(nodes2) != 1 || len(edges2) != 1 {
		t.Error("Unexpected result:", nodes2, edges2)
		return
	}

	if !data.NodeCompare(nodes2[0], node1, []string{data.NodeKey, data.NodeKind}) {
		t.Error("Nodes should match:", node1, nodes2[0])
		return
	}

	if !data.NodeCompare(edges2[0], edge2, []string{data.NodeKey, data.NodeKind}) {

		t.Error("Edges should match:", edge2, edges2[0])
		return
	}

	// Check that the correct ends have been set

	if edges2[0].End1Key() != node2.Key() {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End1Kind() != node2.Kind() {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End1Role() != "node2" {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End1IsCascading() != false {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End1IsCascadingLast() != false {
		t.Error("Unexpected value in traversed edge")
		return
	}

	if edges2[0].End2Key() != node1.Key() {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End2Kind() != node1.Kind() {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End2Role() != "node1" {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End2IsCascading() != true {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End2IsCascadingLast() != true {
		t.Error("Unexpected value in traversed edge")
		return
	}

	// Lookup from the other side getting all attributes

	nodes2, edges2, err = gm.Traverse("main", node2.Key(), node2.Kind(),
		"node2:myotheredge:node1:mykind", true)
	if err != nil {
		t.Error(err)
		return
	}

	if len(nodes2) != 1 || len(edges2) != 1 {
		t.Error("Unexpected result:", nodes2, edges2)
		return
	}

	if !data.NodeCompare(nodes2[0], node1, []string{data.NodeKey, data.NodeKind, data.NodeName}) {
		t.Error("Nodes should match:", node1, nodes2[0])
		return
	}

	if !data.NodeCompare(edges2[0], edge2, []string{data.NodeKey, data.NodeKind, data.NodeName}) {

		t.Error("Edges should match:", edge2, edges2[0])
		return
	}

	// Check that the correct ends have been set

	if edges2[0].End1Key() != node2.Key() {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End1Kind() != node2.Kind() {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End1Role() != "node2" {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End1IsCascading() != false {
		t.Error("Unexpected value in traversed edge")
		return
	}

	if edges2[0].End2Key() != node1.Key() {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End2Kind() != node1.Kind() {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End2Role() != "node1" {
		t.Error("Unexpected value in traversed edge")
		return
	} else if edges2[0].End2IsCascading() != true {
		t.Error("Unexpected value in traversed edge")
		return
	}

	// Lookup from the original side with all attributes

	nodes, edges, err = gm.Traverse("main", node1.Key(), node1.Kind(),
		"node1:myotheredge:node2:mynewkind", true)
	if err != nil {
		t.Error(err)
		return
	}

	if len(nodes) != 1 || len(edges) != 1 {
		t.Error("Unexpected result:", nodes, edges)
		return
	}

	if !data.NodeCompare(nodes[0], node2, nil) {
		t.Error("Nodes should match:", node2, nodes[0])
		return
	}

	if !data.NodeCompare(edges[0], edge2, nil) {

		t.Error("Edges should match:", edge2, edges[0])
		return
	}

	nodes, edges, err = gm.TraverseMulti("main", node1.Key(), node1.Kind(),
		":::", false)
	if err != nil {
		t.Error(err)
		return
	}

	if len(nodes) != 2 || len(edges) != 2 {
		t.Error("Unexpected result:", nodes, edges)
		return
	} else if nodes[0].Key() != node2.Key() || nodes[1].Key() != node2.Key() {
		t.Error("Unexpected result:", nodes, edges)
		return
	} else if edges[0].Key() != edge1.Key() || edges[1].Key() != edge2.Key() {
		t.Error("Unexpected result:", nodes, edges)
		return
	}

	nodes, edges, err = gm.TraverseMulti("main", node1.Key(), node1.Kind(),
		"node1::node2:mynewkind", false)
	if err != nil {
		t.Error(err)
		return
	}

	if len(nodes) != 2 || len(edges) != 2 {
		t.Error("Unexpected result:", nodes, edges)
		return
	} else if nodes[0].Key() != node2.Key() || nodes[1].Key() != node2.Key() {
		t.Error("Unexpected result:", nodes, edges)
		return
	} else if edges[0].Key() != edge1.Key() || edges[1].Key() != edge2.Key() {
		t.Error("Unexpected result:", nodes, edges)
		return
	}

	nodes, edges, err = gm.TraverseMulti("main", node1.Key(), node1.Kind(),
		"node1:myotheredge::mynewkind", false)
	if err != nil {
		t.Error(err)
		return
	}

	if len(nodes) != 1 || len(edges) != 1 {
		t.Error("Unexpected result:", nodes, edges)
		return
	} else if nodes[0].Key() != node2.Key() {
		t.Error("Unexpected result:", nodes, edges)
		return
	} else if edges[0].Key() != edge2.Key() {
		t.Error("Unexpected result:", nodes, edges)
		return
	}

	nodes, edges, err = gm.TraverseMulti("main", node1.Key(), node1.Kind(),
		"node2:myotheredge:node2:mynewkind", false)
	if err != nil {
		t.Error(err)
		return
	} else if len(nodes) != 0 || len(edges) != 0 {
		t.Error("Unexpected result:", nodes, edges)
		return
	}

	nodes, edges, err = gm.TraverseMulti("main", node1.Key(), node1.Kind(),
		"node1:myotheredge2:node2:mynewkind", false)
	if err != nil {
		t.Error(err)
		return
	} else if len(nodes) != 0 || len(edges) != 0 {
		t.Error("Unexpected result:", nodes, edges)
		return
	}

	nodes, edges, err = gm.TraverseMulti("main", node1.Key(), node1.Kind(),
		"node1:myotheredge:node3:mynewkind", false)
	if err != nil {
		t.Error(err)
		return
	} else if len(nodes) != 0 || len(edges) != 0 {
		t.Error("Unexpected result:", nodes, edges)
		return
	}

	nodes, edges, err = gm.TraverseMulti("main", node1.Key(), node1.Kind(),
		"node1:myotheredge:node2:mynewkind2", false)
	if err != nil {
		t.Error(err)
		return
	} else if len(nodes) != 0 || len(edges) != 0 {
		t.Error("Unexpected result:", nodes, edges)
		return
	}
}
