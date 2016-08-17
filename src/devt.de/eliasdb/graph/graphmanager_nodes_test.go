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
	"strconv"
	"strings"
	"testing"

	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/graph/util"
	"devt.de/eliasdb/hash"
	"devt.de/eliasdb/storage"
)

func TestSimpleNodeStorage(t *testing.T) {
	if !RUN_DISK_STORAGE_TESTS {
		return
	}

	dgs, err := graphstorage.NewDiskGraphStorage(GRAPHMANAGER_TEST_DBDIR2)
	if err != nil {
		t.Error(err)
		return
	}

	gm := newGraphManagerNoRules(dgs)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mykind")
	node1.SetAttr("Name", "Some name")
	node1.SetAttr("To Delete", "Some data")

	if cnt := gm.NodeCount("mykind"); cnt != 0 {
		t.Error("Invalid node count:", cnt)
		return
	}

	if err := gm.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	if cnt := gm.NodeCount("mykind"); cnt != 1 {
		t.Error("Invalid node count:", cnt)
		return
	}

	if gm.IsValidAttr("123") {
		t.Error("123 should not be a valid attribute")
	}
	if !gm.IsValidAttr("Name") {
		t.Error("Name should be a valid attribute")
	}
	if !gm.IsValidAttr(data.NODE_KEY) {
		t.Error("key should be a valid attribute")
	}
	if !gm.IsValidAttr(data.NODE_KIND) {
		t.Error("kind should be a valid attribute")
	}
	if !gm.IsValidAttr(data.EDGE_END1_CASCADING) {
		t.Error("end1cascading should be a valid attribute")
	}

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mykind")
	node2.SetAttr("Name", "Node2")
	node2.SetAttr("Data", "word1, word2, word3!")

	node1 = data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mykind")
	node1.SetAttr("Name", "Some new name")
	node1.SetAttr("Data", "word4, word5, word6!")

	if err := gm.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	if err := gm.StoreNode("main", node2); err != nil {
		t.Error(err)
		return
	}

	if cnt := gm.NodeCount("mykind"); cnt != 2 {
		t.Error("Invalid node count:", cnt)
		return
	}

	// Get only part of a node

	fnode1, err := gm.FetchNodePart("main", "123", "mykind", []string{"Data"})
	if err != nil {
		t.Error(err)
		return
	}

	// Check we got only the attributes we asked for

	if res := len(fnode1.Data()); res != 3 {
		t.Error("Unexpected number of attributes:", res)
		return
	}

	if fnode1.Key() != "123" || fnode1.Kind() != "mykind" {
		t.Error("Unexpected result:", fnode1)
		return
	}

	if fnode1.Attr("Data") != "word4, word5, word6!" {
		t.Error("Unexpected result:", fnode1)
		return
	}

	// Get the full node

	fnode2, err := gm.FetchNode("main", "123", "mykind")
	if err != nil {
		t.Error(err)
		return
	}

	// Check we got everything back

	if res := len(fnode2.Data()); res != 4 {
		t.Error("Unexpected number of attributes:", res)
		return
	}

	if fnode2.Key() != "123" || fnode2.Kind() != "mykind" {
		t.Error("Unexpected result:", fnode1)
		return
	}

	if fnode2.Attr("Name") != "Some new name" {
		t.Error("Unexpected result:", fnode1)
		return
	}

	if fnode2.Attr("Data") != "word4, word5, word6!" {
		t.Error("Unexpected result:", fnode1)
		return
	}

	dgs.Close()

	// Check that we can do the lookup with a new graph storage

	dgs2, err := graphstorage.NewDiskGraphStorage(GRAPHMANAGER_TEST_DBDIR2)
	if err != nil {
		t.Error(err)
		return
	}

	gm2 := newGraphManagerNoRules(dgs2)

	// Do an index lookup

	iq, err := gm2.NodeIndexQuery("main", "mykind")
	if err != nil {
		t.Error(err)
		return
	}

	res, err := iq.LookupWord("Data", "word5")
	if err != nil {
		t.Error(err)
		return
	}

	if fmt.Sprint(res) != "map[123:[2]]" {
		t.Error("Unexpected result:", res)
		return
	}

	fnode3, err := gm2.FetchNode("main", "123", "mykind")
	if err != nil {
		t.Error(err)
		return
	}

	// Check we got everything back

	if res := len(fnode3.Data()); res != 4 {
		t.Error("Unexpected number of attributes:", res)
		return
	}

	res2, err := iq.LookupPhrase("Data", "-....word5 ...word6")
	if err != nil {
		t.Error(err)
		return
	}

	if fmt.Sprint(res2) != "[123]" {
		t.Error("Unexpected result:", res)
		return
	}

	// Delete the nodes

	fnode4, err := gm2.RemoveNode("main", "123", "mykind")
	if err != nil {
		t.Error(err)
		return
	}

	if res := len(fnode4.Data()); res != 4 {
		t.Error("Unexpected number of attributes:", res)
		return
	}

	// Check that the node no longer exists

	fnode4, err = gm2.FetchNode("main", "123", "mykind")
	if err != nil || fnode4 != nil {
		t.Error("Unexpected lookup result:", fnode4, err)
		return
	}

	if cnt := gm2.NodeCount("mykind"); cnt != 1 {
		t.Error("Invalid node count:", cnt)
		return
	}

	_, err = gm2.RemoveNode("main", "456", "mykind")
	if err != nil {
		t.Error(err)
		return
	}

	if cnt := gm2.NodeCount("mykind"); cnt != 0 {
		t.Error("Invalid node count:", cnt)
		return
	}

	// Check that all datastructures are empty

	tree, _, _ := gm2.getNodeStorageHTree("main", "mykind", false)
	it := hash.NewHTreeIterator(tree)

	if it.HasNext() {
		t.Error("Node storage tree should be empty at this point")
		return
	}

	tree, _ = gm2.getNodeIndexHTree("main", "mykind", false)
	it = hash.NewHTreeIterator(tree)

	if it.HasNext() {
		t.Error("Node storage tree should be empty at this point")
		return
	}

	dgs2.Close()
}

func TestSimpleNodeUpdate(t *testing.T) {
	if !RUN_DISK_STORAGE_TESTS {
		return
	}

	dgs, err := graphstorage.NewDiskGraphStorage(GRAPHMANAGER_TEST_DBDIR2)
	if err != nil {
		t.Error(err)
		return
	}

	gm := newGraphManagerNoRules(dgs)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "nodeToUpdate")
	node1.SetAttr("kind", "nodeupdatekind")

	// Check that an update can do an actual insert

	err = gm.UpdateNode("main", node1)
	if err != nil {
		t.Error(err)
		return
	}

	// Check that we can lookup the node just by asking for the kind attribute

	n, err := gm.FetchNodePart("main", node1.Key(), node1.Kind(), []string{"kind"})
	if err != nil {
		t.Error(err)
	}

	if !data.NodeCompare(node1, n, nil) {
		t.Error("Nodes should match")
		return
	}

	node1.SetAttr("Name", "Some name")
	node1.SetAttr("Name2", "Some name2")
	node1.SetAttr("Name3", "Some name3")
	node1.SetAttr("Name4", "Some name4")

	err = gm.UpdateNode("main", node1)
	if err != nil {
		t.Error(err)
		return
	}

	fetchedNode, err := gm.FetchNode("main", "nodeToUpdate", "nodeupdatekind")
	if !data.NodeCompare(node1, fetchedNode, nil) {
		t.Error("Node should have been stored completely")
		return
	}

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "nodeToUpdate")
	node2.SetAttr("kind", "nodeupdatekind")
	node2.SetAttr("Name", "Some new name")
	node2.SetAttr("NewField", "Some new field value")

	err = gm.UpdateNode("main", node2)
	if err != nil {
		t.Error(err)
		return
	}

	fetchedNode, _ = gm.FetchNode("main", "nodeToUpdate", "nodeupdatekind")

	if len(fetchedNode.Data()) != len(node1.Data())+1 {
		t.Error("Unexpected number of attributes")
		return
	}

	if !data.NodeCompare(data.NodeMerge(node1, node2), fetchedNode, nil) {
		t.Error("Node should have been stored completely")
		return
	}

	dgs.Close()
}

func TestSimpleNodeStorageErrorCases(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")

	gm := newGraphManagerNoRules(mgs)

	if _, err := gm.FetchNodePart("in valid", "testkey", "testkind", nil); err.Error() !=
		"GraphError: Invalid data (Partition name in valid is not alphanumeric - can only contain [a-zA-Z0-9_])" {

		t.Error("Unexpected error:", err)
		return
	}

	if res, err := gm.FetchNodePart("testpart", "testkey", "testkind", nil); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	if _, err := gm.NodeIndexQuery("in valid", "testkind"); err.Error() !=
		"GraphError: Invalid data (Partition name in valid is not alphanumeric - can only contain [a-zA-Z0-9_])" {

		t.Error("Unexpected error:", err)
		return
	}

	if _, err := gm.NodeIndexQuery("testpart", "testkind-"); err.Error() !=
		"GraphError: Invalid data (Node kind testkind- is not alphanumeric - can only contain [a-zA-Z0-9_])" {

		t.Error("Unexpected error:", err)
		return
	}

	if _, err := gm.RemoveNode("in valid", "testkey", "testkind"); err.Error() !=
		"GraphError: Invalid data (Partition name in valid is not alphanumeric - can only contain [a-zA-Z0-9_])" {

		t.Error("Unexpected error:", err)
		return
	}

	if res, err := gm.RemoveNode("testpart", "testkey", "testkind"); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := gm.NodeIndexQuery("testpart", "testkind"); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := gm.NodeIndexQuery("testpart", "testkind"); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	attTree, valTree, _ := gm.getNodeStorageHTree("testpart", "testkind", true)

	if res, err := gm.readNode("123", "testkind", nil, attTree, valTree); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "testkind")
	node1.SetAttr("Name", "Some name")

	if err := gm.StoreNode("testpart", node1); err != nil {
		t.Error(err)
		return
	}

	msm := mgs.StorageManager("testpart"+"testkind"+STORAGE_SUFFIX_NODES,
		true).(*storage.MemoryStorageManager)

	msm.AccessMap[4] = storage.ACCESS_CACHE_AND_FETCH_ERROR

	if res, err := gm.readNode("123", "testkind", nil, attTree, valTree); res != nil ||
		err.Error() != "GraphError: Could not read graph information "+
			"(Slot not found (mystorage/testparttestkind.nodes - Location:4))" {

		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := gm.writeNode(node1, true, attTree, valTree, nodeAttributeFilter); res != nil ||
		err.Error() != "GraphError: Could not read graph information "+
			"(Slot not found (mystorage/testparttestkind.nodes - Location:4))" {

		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := gm.RemoveNode("testpart", "123", "testkind"); res != nil ||
		err.Error() != "GraphError: Could not write graph information "+
			"(Slot not found (mystorage/testparttestkind.nodes - Location:4))" {

		t.Error("Unexpected result:", res, err)
		return
	}

	delete(msm.AccessMap, 4)

	if res, err := gm.RemoveNode("testpart", "1234", "testkind"); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	msm.AccessMap[3] = storage.ACCESS_CACHE_AND_FETCH_ERROR

	if res, err := gm.readNode("123", "testkind", nil, attTree, valTree); res != nil ||
		err.Error() != "GraphError: Could not read graph information "+
			"(Slot not found (mystorage/testparttestkind.nodes - Location:3))" {

		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := gm.readNode("123", "testkind", []string{"Name"}, attTree, valTree); res != nil ||
		err.Error() != "GraphError: Could not read graph information "+
			"(Slot not found (mystorage/testparttestkind.nodes - Location:3))" {

		t.Error("Unexpected result:", res, err)
		return
	}

	delete(msm.AccessMap, 3)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "")
	node2.SetAttr("kind", "testkind")
	node2.SetAttr("Name", "Some name2")

	if err := gm.StoreNode("testpart", node2); err.Error() !=
		"GraphError: Invalid data (Node is missing a key value)" {

		t.Error(err)
		return
	}

	node2.SetAttr("key", "456")

	// Test edge to test common error cases with nodes

	edge := data.NewGraphEdge()
	edge.SetAttr("key", "abc")
	edge.SetAttr("kind", "myedge")
	edge.SetAttr(data.EDGE_END1_KEY, node1.Key())
	edge.SetAttr(data.EDGE_END1_KIND, node1.Kind())
	edge.SetAttr(data.EDGE_END1_ROLE, "node1")
	edge.SetAttr(data.EDGE_END1_CASCADING, false)

	edge.SetAttr(data.EDGE_END2_KEY, node2.Key())
	edge.SetAttr(data.EDGE_END2_KIND, node2.Kind())
	edge.SetAttr(data.EDGE_END2_ROLE, "node2")
	edge.SetAttr(data.EDGE_END2_CASCADING, false)

	if err := gm.StoreNode("testpart ", node2); err.Error() !=
		"GraphError: Invalid data (Partition name testpart  is not alphanumeric - can only contain [a-zA-Z0-9_])" {

		t.Error(err)
		return
	}

	delete(mgs.MainDB(), MAINDB_NODE_COUNT+"testkind")

	sm := mgs.StorageManager("testpart"+node2.Kind()+STORAGE_SUFFIX_NODES, false).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.ACCESS_CACHE_AND_FETCH_ERROR

	if err := gm.StoreNode("testpart", node2); err.Error() !=
		"GraphError: Failed to access graph storage component (Slot not found (mystorage/testparttestkind.nodes - Location:1))" {

		t.Error(err)
		return
	}

	delete(sm.AccessMap, 1)

	sm = mgs.StorageManager("testpart"+edge.Kind()+STORAGE_SUFFIX_EDGES, true).(*storage.MemoryStorageManager)
	sm.AccessMap[1] = storage.ACCESS_CACHE_AND_FETCH_ERROR

	sm.SetRoot(ROOT_ID_NODE_HTREE, 1)

	if err := gm.StoreEdge("testpart", edge); err.Error() !=
		"GraphError: Failed to access graph storage component (Slot not found (mystorage/testpartmyedge.edges - Location:1))" {
		t.Error(err)
		return
	}

	delete(sm.AccessMap, 1)

	msm.AccessMap[5] = storage.ACCESS_INSERT_ERROR

	if err := gm.StoreNode("testpart", node2); err.Error() !=
		"GraphError: Could not write graph information (Record is already in-use (? - ))" {

		t.Error(err)
		return
	}

	delete(msm.AccessMap, 5)

	msm.AccessMap[5] = storage.ACCESS_INSERT_ERROR

	if err := gm.StoreNode("testpart", node2); err.Error() !=
		"GraphError: Could not write graph information (Record is already in-use (? - ))" {

		t.Error(err)
		return
	}

	delete(msm.AccessMap, 5)

	node2.SetAttr("key", "123")
	node2.SetAttr("Name", nil)

	msm.AccessMap[3] = storage.ACCESS_FREE_ERROR

	if err := gm.StoreNode("testpart", node2); err.Error() !=
		"GraphError: Could not write graph information (Slot not found (mystorage/testparttestkind.nodes - Location:3))" {

		t.Error(err)
		return
	}

	delete(msm.AccessMap, 3)

	node2.SetAttr("key", "456")
	node2.SetAttr("Name", "A new name")

	graphstorage.MgsRetFlushMain = &util.GraphError{util.ErrFlushing, errors.New("Test").Error()}

	if err := gm.StoreNode("testpart", node2); err.Error() !=
		"GraphError: Failed to flush changes (Test)" {

		t.Error(err)
		return
	}

	graphstorage.MgsRetFlushMain = nil

	is := gm.gs.StorageManager("testpart"+"testkind"+STORAGE_SUFFIX_NODES_INDEX,
		false).(*storage.MemoryStorageManager)

	for i := 0; i < 10; i++ {
		is.AccessMap[uint64(i)] = storage.ACCESS_INSERT_ERROR
	}

	node2.SetAttr("key", "789")

	if err := gm.StoreNode("testpart", node2); err.Error() !=
		"GraphError: Index error (Record is already in-use (? - ))" {

		t.Error(err)
		return
	}

	node2.SetAttr("key", "123")

	if err := gm.StoreNode("testpart", node2); err.Error() !=
		"GraphError: Index error (Record is already in-use (? - ))" {

		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		is.AccessMap[uint64(i)] = storage.ACCESS_UPDATE_ERROR
	}

	if res, err := gm.RemoveNode("testpart", "789", "testkind"); !strings.Contains(err.Error(),
		"GraphError: Index error (Slot not found (mystorage/testparttestkind.nodeidx") {

		t.Error("Unexpected result:", res, err)
		return
	}

	for i := 0; i < 10; i++ {
		delete(is.AccessMap, uint64(i))
	}

	msm.AccessMap[9] = storage.ACCESS_CACHE_AND_FETCH_ERROR

	// This call does delete the node by blowing
	// away the attribute list - the node is removed though its attribute
	// values remain in the datastore

	if res, err := gm.deleteNode("123", "testkind", attTree, valTree); err.Error() !=
		"GraphError: Could not write graph information "+
			"(Slot not found (mystorage/testparttestkind.nodes - Location:9))" {

		t.Error("Unexpected result:", res, err)
		return
	}
	delete(msm.AccessMap, 9)

	if res, err := gm.FetchNodePart("testpart", "123", "testkind", nil); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	gm.StoreNode("testpart", node2)

	graphstorage.MgsRetFlushMain = &util.GraphError{util.ErrFlushing, errors.New("Test").Error()}

	if _, err := gm.RemoveNode("testpart", node2.Key(), node2.Kind()); err.Error() !=
		"GraphError: Failed to flush changes (Test)" {

		t.Error(err)
		return
	}

	graphstorage.MgsRetFlushMain = nil
}

func TestGraphManagerDiskStorage(t *testing.T) {
	if !RUN_DISK_STORAGE_TESTS {
		return
	}

	dgs, err := graphstorage.NewDiskGraphStorage(GRAPHMANAGER_TEST_DBDIR1)
	if err != nil {
		t.Error(err)
		return
	}

	gm := newGraphManagerNoRules(dgs)

	if gm.Name() != "Graph "+GRAPHMANAGER_TEST_DBDIR1 {
		t.Error("Unexpected name:", gm.Name())
		return
	}

	sm := dgs.StorageManager("my1", true)

	htree, err := gm.getHTree(sm, ROOT_ID_NODE_HTREE)
	if err != nil {
		t.Error(err)
		return
	}

	htree.Put([]byte("test1"), "testvalue1")

	dgs.Close()

	dgs2, err := graphstorage.NewDiskGraphStorage(GRAPHMANAGER_TEST_DBDIR1)
	if err != nil {
		t.Error(err)
		return
	}

	dgs2.MainDB()[MAINDB_VERSION] = strconv.Itoa(VERSION + 1)
	dgs2.FlushMain()

	testVersionPanic(t, dgs2)

	dgs2.MainDB()[MAINDB_VERSION] = strconv.Itoa(VERSION)
	dgs2.FlushMain()

	// This should now succeed

	newGraphManagerNoRules(dgs2)

	dgs2.MainDB()[MAINDB_VERSION] = strconv.Itoa(VERSION - 1)
	dgs2.FlushMain()

	gm = newGraphManagerNoRules(dgs2)

	if dgs2.MainDB()[MAINDB_VERSION] != strconv.Itoa(VERSION) {
		t.Error("Version should have been corrected")
		return
	}

	sm2 := dgs2.StorageManager("my1", true)

	htree2, err := gm.getHTree(sm2, ROOT_ID_NODE_HTREE)
	if err != nil {
		t.Error(err)
		return
	}

	if val, err := htree2.Get([]byte("test1")); val != "testvalue1" || err != nil {
		t.Error("Unexpected result:", val, err)
		return
	}

	dgs2.Close()

	// Test error cases

	msm := storage.NewMemoryStorageManager("mytest")

	msm.AccessMap[1] = storage.ACCESS_INSERT_ERROR

	_, err = gm.getHTree(msm, ROOT_ID_NODE_HTREE)

	if err.(*util.GraphError).Type != util.ErrAccessComponent {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, 1)

	msm.SetRoot(ROOT_ID_NODE_HTREE, 2)

	msm.AccessMap[2] = storage.ACCESS_INSERT_ERROR

	_, err = gm.getHTree(msm, ROOT_ID_NODE_HTREE)

	if err.(*util.GraphError).Type != util.ErrAccessComponent {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, 2)
}

func testVersionPanic(t *testing.T, gs graphstorage.GraphStorage) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Opening a graph with a newer version did not cause a panic.")
		}
	}()

	newGraphManagerNoRules(gs)
}
