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
	"testing"

	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/storage"
)

func TestNodeKeyIterator(t *testing.T) {

	mgs := graphstorage.NewMemoryGraphStorage("iterator test")

	gm := newGraphManagerNoRules(mgs)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mykind")
	node1.SetAttr("Name", "Node1")

	gm.StoreNode("main", node1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mykind2")
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

	edge.SetAttr(data.EDGE_END1_KEY, node1.Key())
	edge.SetAttr(data.EDGE_END1_KIND, node1.Kind())
	edge.SetAttr(data.EDGE_END1_ROLE, "node1")
	edge.SetAttr(data.EDGE_END1_CASCADING, true)

	edge.SetAttr(data.EDGE_END2_KEY, node2.Key())
	edge.SetAttr(data.EDGE_END2_KIND, node2.Kind())
	edge.SetAttr(data.EDGE_END2_ROLE, "node2")
	edge.SetAttr(data.EDGE_END2_CASCADING, false)

	gm.StoreEdge("main", edge)

	ni, err := gm.NodeKeyIterator("main", "mykind")
	if err != nil {
		t.Error(err)
		return
	}

	expected_keys := []string{"123", "789"}

	i := 0
	for ni.HasNext() {
		key := ni.Next()
		if key != expected_keys[i] {
			t.Error("Unexpected key:", key, "expected", expected_keys[i])
			return
		}

		if ni.LastError != nil {
			t.Error(ni.LastError)
			return
		}
		i++
	}

	ni, err = gm.NodeKeyIterator("main", "mykind")
	if err != nil {
		t.Error(err)
		return
	}

	msm := mgs.StorageManager("main"+"mykind"+STORAGE_SUFFIX_NODES, false)

	tree, _, _ := gm.getNodeStorageHTree("main", "mykind", false)
	_, loc, _ := tree.GetValueAndLocation([]byte(PREFIX_NS_ATTRS + "123"))

	msm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.ACCESS_CACHE_AND_FETCH_SERIOUS_ERROR

	ni.Next()
	if ni.LastError == nil {
		t.Error("Expected an error to occur")
		return
	}

	delete(msm.(*storage.MemoryStorageManager).AccessMap, loc)

	msm.(*storage.MemoryStorageManager).AccessMap[1] = storage.ACCESS_CACHE_AND_FETCH_ERROR

	ni, err = gm.NodeKeyIterator("main", "mykind")
	if ni != nil || err == nil {
		t.Error("Key iterator should not be created at this point")
		return
	}

	delete(msm.(*storage.MemoryStorageManager).AccessMap, 1)

	msm.(*storage.MemoryStorageManager).AccessMap[loc] = storage.ACCESS_CACHE_AND_FETCH_SERIOUS_ERROR

	ni, err = gm.NodeKeyIterator("main", "mykind")
	if ni != nil || err == nil {
		t.Error("Key iterator should not be created at this point")
		return
	}

	delete(msm.(*storage.MemoryStorageManager).AccessMap, loc)

	// Test iterator running out of items

	ni, err = gm.NodeKeyIterator("main", "mykind")
	if err != nil {
		t.Error(err)
		return
	}

	ni.Next()
	ni.Next()
	ni.Next()

	if ni.Next() != "" || ni.LastError != nil {
		t.Error("Expected iterator to run out of items:", ni.LastError)
		return
	}
}
