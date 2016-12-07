/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package v1

import (
	"encoding/json"
	"fmt"
	"testing"

	"devt.de/common/datautil"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/hash"
	"devt.de/eliasdb/storage"
)

func TestNestedStorage(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraph

	// Store a nested node

	st, _, res := sendTestRequest(queryURL+"main/n", "POST", []byte(`
[{
	"key":"nestedtest",
	"kind":"Test",
	"int":42,
	"float":3.1415926,
	"str":"foo bar",
	"nested":{
		"nested_int":12,
		"nested_float":1.234,
		"nested_str":"time flies like an arrow",
		"more nesting": {
			"atom" : "value42"
		}
	}
}]
`[1:]))

	if st != "200 OK" {
		t.Error("Unexpected response:", st, res)
		return
	}

	n, err := api.GM.FetchNode("main", "nestedtest", "Test")
	if err != nil {
		t.Error(err)
		return
	}

	// Check that the node was stored correctly

	nested := n.Attr("nested")

	if nt := fmt.Sprintf("%T", nested); nt != "map[string]interface {}" {
		t.Error("Unexpected type:", nt)
		return
	}

	nf, err := datautil.GetNestedValue(nested.(map[string]interface{}), []string{"nested_float"})

	if nft := fmt.Sprintf("%T %v", nf, nf); nft != "float64 1.234" {
		t.Error("Unexpected type:", nft)
		return
	}

	ns, err := datautil.GetNestedValue(nested.(map[string]interface{}), []string{"more nesting", "atom"})

	if nst := fmt.Sprintf("%T %v", ns, ns); nst != "string value42" {
		t.Error("Unexpected type:", nst)
		return
	}

	// Now try to retrieve the value

	st, _, res = sendTestRequest(queryURL+"/main/n/Test/nestedtest", "GET", nil)

	if st != "200 OK" || res != `
{
  "float": 3.1415926,
  "int": 42,
  "key": "nestedtest",
  "kind": "Test",
  "nested": {
    "more nesting": {
      "atom": "value42"
    },
    "nested_float": 1.234,
    "nested_int": 12,
    "nested_str": "time flies like an arrow"
  },
  "str": "foo bar"
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}
}

func TestGraphQuery(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraph

	// Test error message

	_, _, res := sendTestRequest(queryURL, "GET", nil)

	if res != "Need a partition, entity type (n or e) and a kind; optional key and traversal spec" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/main/t/Song", "GET", nil)

	if res != "Entity type must be n (nodes) or e (edges)" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/main/e/Song", "GET", nil)

	if res != "Entity type must be n (nodes) when requesting all items" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/main/n/SSong", "GET", nil)

	if res != "Unknown partition or node kind" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/xmain/n/Song", "GET", nil)

	if res != "Unknown partition or node kind" {
		t.Error("Unexpected response:", res)
		return
	}

	st, h, res := sendTestRequest(queryURL+"/main/n/Song", "GET", nil)

	if tc := h.Get(HTTPHeaderTotalCount); tc != "9" {
		t.Error("Unexpected total count header:", tc)
		return
	}

	if st != "200 OK" || res != `
[
  {
    "key": "StrangeSong1",
    "kind": "Song",
    "name": "StrangeSong1",
    "ranking": 5
  },
  {
    "key": "FightSong4",
    "kind": "Song",
    "name": "FightSong4",
    "ranking": 3
  },
  {
    "key": "DeadSong2",
    "kind": "Song",
    "name": "DeadSong2",
    "ranking": 6
  },
  {
    "key": "LoveSong3",
    "kind": "Song",
    "name": "LoveSong3",
    "ranking": 1
  },
  {
    "key": "MyOnlySong3",
    "kind": "Song",
    "name": "MyOnlySong3",
    "ranking": 19
  },
  {
    "key": "Aria1",
    "kind": "Song",
    "name": "Aria1",
    "ranking": 8
  },
  {
    "key": "Aria2",
    "kind": "Song",
    "name": "Aria2",
    "ranking": 2
  },
  {
    "key": "Aria3",
    "kind": "Song",
    "name": "Aria3",
    "ranking": 4
  },
  {
    "key": "Aria4",
    "kind": "Song",
    "name": "Aria4",
    "ranking": 18
  }
]`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Test offset and limit

	st, _, res = sendTestRequest(queryURL+"/main/n/Song?offset=3&limit=2", "GET", nil)
	if st != "200 OK" || res != `
[
  {
    "key": "LoveSong3",
    "kind": "Song",
    "name": "LoveSong3",
    "ranking": 1
  },
  {
    "key": "MyOnlySong3",
    "kind": "Song",
    "name": "MyOnlySong3",
    "ranking": 19
  }
]`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main/n/Song?offset=7&limit=200", "GET", nil)
	if st != "200 OK" || res != `
[
  {
    "key": "Aria3",
    "kind": "Song",
    "name": "Aria3",
    "ranking": 4
  },
  {
    "key": "Aria4",
    "kind": "Song",
    "name": "Aria4",
    "ranking": 18
  }
]`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main/n/Song?offset=p&limit=2", "GET", nil)
	if st != "400 Bad Request" || res != "Invalid parameter value: offset should be a positive integer number" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main/n/Song?offset=2&limit=p", "GET", nil)
	if st != "400 Bad Request" || res != "Invalid parameter value: limit should be a positive integer number" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main/n/Song?offset=700&limit=2", "GET", nil)
	if st != "500 Internal Server Error" || res != "Offset exceeds available nodes" {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Test error cases

	msm := gmMSM.StorageManager("main"+"Song"+graph.StorageSuffixNodes,
		true).(*storage.MemoryStorageManager)

	msm.AccessMap[2] = storage.AccessCacheAndFetchError

	st, _, res = sendTestRequest(queryURL+"/main/n/Song", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainSong.nodes - Location:2))" {
		t.Error("Unexpected response:", res)
		return
	}

	delete(msm.AccessMap, 2)

	msm.AccessMap[4] = storage.AccessCacheAndFetchError

	st, _, res = sendTestRequest(queryURL+"/main/n/Song", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Could not read graph information (Slot not found (mystorage/mainSong.nodes - Location:4))" {
		t.Error("Unexpected response:", res)
		return
	}

	delete(msm.AccessMap, 4)

	msm = gmMSM.StorageManager("main"+"Spam"+graph.StorageSuffixNodes,
		true).(*storage.MemoryStorageManager)

	loc := msm.Root(graph.RootIDNodeHTree)
	htree, _ := hash.LoadHTree(msm, loc)

	_, kloc, _ := htree.GetValueAndLocation([]byte(graph.PrefixNSAttrs + "00019"))

	msm.AccessMap[kloc] = storage.AccessCacheAndFetchSeriousError

	st, _, res = sendTestRequest(queryURL+"/main/n/Spam?offset=19&limit=1", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Could not read graph information (Record is already in-use (? - ))" {
		t.Error("Unexpected response:", res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main/n/Spam", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Could not read graph information (Record is already in-use (? - ))" {
		t.Error("Unexpected response:", res)
		return
	}

	delete(msm.AccessMap, kloc)
}

func TestGraphQuerySingleItem(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraph

	st, _, res := sendTestRequest(queryURL+"/main/n/Author/123", "GET", nil)

	if st != "200 OK" || res != `
{
  "key": "123",
  "kind": "Author",
  "name": "Mike"
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main/e/Wrote/LoveSong3", "GET", nil)

	if st != "200 OK" || res != `
{
  "end1cascading": true,
  "end1key": "123",
  "end1kind": "Author",
  "end1role": "Author",
  "end2cascading": false,
  "end2key": "LoveSong3",
  "end2kind": "Song",
  "end2role": "Song",
  "key": "LoveSong3",
  "kind": "Wrote",
  "number": 3
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Test error cases

	st, _, res = sendTestRequest(queryURL+"/main/n/Spam/x0005", "GET", nil)

	if st != "400 Bad Request" ||
		res != "Unknown partition or node kind" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main/e/xSpam/0005", "GET", nil)

	if st != "400 Bad Request" ||
		res != "Unknown partition or edge kind" {
		t.Error("Unexpected response:", st, res)
		return
	}

	msm := gmMSM.StorageManager("main"+"Spam"+graph.StorageSuffixNodes,
		true).(*storage.MemoryStorageManager)

	msm.AccessMap[2] = storage.AccessCacheAndFetchError

	st, _, res = sendTestRequest(queryURL+"/main/n/Spam/0005", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainSpam.nodes - Location:2))" {
		t.Error("Unexpected response:", res)
		return
	}

	delete(msm.AccessMap, 2)

	msm = gmMSM.StorageManager("main"+"Wrote"+graph.StorageSuffixEdges,
		true).(*storage.MemoryStorageManager)

	msm.AccessMap[1] = storage.AccessCacheAndFetchError

	st, _, res = sendTestRequest(queryURL+"/main/e/Wrote/LoveSong3", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainWrote.edges - Location:1))" {
		t.Error("Unexpected response:", res)
		return
	}

	delete(msm.AccessMap, 1)
}

func TestGraphQueryTraversal(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraph

	_, _, res := sendTestRequest(queryURL+"/main/n/Author/123/:::/aaa", "GET", nil)

	if res != "Invalid resource specification: n/Author/123/:::/aaa" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/main/e/Author/123/:::", "GET", nil)

	if res != "Entity type must be n (nodes) when requesting traversal results" {
		t.Error("Unexpected response:", res)
		return
	}

	st, _, res := sendTestRequest(queryURL+"/main/n/Author/123/:::", "GET", nil)

	if st != "200 OK" || res != `
[
  [
    {
      "key": "DeadSong2",
      "kind": "Song",
      "name": "DeadSong2",
      "ranking": 6
    },
    {
      "key": "FightSong4",
      "kind": "Song",
      "name": "FightSong4",
      "ranking": 3
    },
    {
      "key": "LoveSong3",
      "kind": "Song",
      "name": "LoveSong3",
      "ranking": 1
    },
    {
      "key": "StrangeSong1",
      "kind": "Song",
      "name": "StrangeSong1",
      "ranking": 5
    }
  ],
  [
    {
      "end1cascading": true,
      "end1key": "123",
      "end1kind": "Author",
      "end1role": "Author",
      "end2cascading": false,
      "end2key": "DeadSong2",
      "end2kind": "Song",
      "end2role": "Song",
      "key": "DeadSong2",
      "kind": "Wrote",
      "number": 2
    },
    {
      "end1cascading": true,
      "end1key": "123",
      "end1kind": "Author",
      "end1role": "Author",
      "end2cascading": false,
      "end2key": "FightSong4",
      "end2kind": "Song",
      "end2role": "Song",
      "key": "FightSong4",
      "kind": "Wrote",
      "number": 4
    },
    {
      "end1cascading": true,
      "end1key": "123",
      "end1kind": "Author",
      "end1role": "Author",
      "end2cascading": false,
      "end2key": "LoveSong3",
      "end2kind": "Song",
      "end2role": "Song",
      "key": "LoveSong3",
      "kind": "Wrote",
      "number": 3
    },
    {
      "end1cascading": true,
      "end1key": "123",
      "end1kind": "Author",
      "end1role": "Author",
      "end2cascading": false,
      "end2key": "StrangeSong1",
      "end2kind": "Song",
      "end2role": "Song",
      "key": "StrangeSong1",
      "kind": "Wrote",
      "number": 1
    }
  ]
]`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main/n/Spam/0005/:::", "GET", nil)

	if st != "200 OK" || res != `
[
  [],
  []
]`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Test error cases

	st, _, res = sendTestRequest(queryURL+"/main/n/Spam/x0005/:::", "GET", nil)

	if st != "400 Bad Request" ||
		res != "Unknown partition or node kind" {
		t.Error("Unexpected response:", st, res)
		return
	}

	msm := gmMSM.StorageManager("main"+"Song"+graph.StorageSuffixNodes,
		true).(*storage.MemoryStorageManager)

	msm.AccessMap[2] = storage.AccessCacheAndFetchError

	st, _, res = sendTestRequest(queryURL+"/main/n/Author/123/:::", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainSong.nodes - Location:2))" {
		t.Error("Unexpected response:", res)
		return
	}

	delete(msm.AccessMap, 2)

	msm = gmMSM.StorageManager("main"+"Spam"+graph.StorageSuffixNodes,
		true).(*storage.MemoryStorageManager)

	msm.AccessMap[2] = storage.AccessCacheAndFetchError

	st, _, res = sendTestRequest(queryURL+"/main/n/Spam/0005/:::", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainSpam.nodes - Location:2))" {
		t.Error("Unexpected response:", res)
		return
	}

	delete(msm.AccessMap, 2)
}

func TestGraphOperation(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraph

	// Test error message

	_, _, res := sendTestRequest(queryURL, "POST", nil)

	if res != "Need a partition; optional entity type (n or e)" {
		t.Error("Unexpected response:", res)
		return
	}

	// Test node creation

	node := data.NewGraphNode()
	node.SetAttr("key", "111")
	node.SetAttr("name", "node1")

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "112")
	node2.SetAttr("name", "node2")

	jsonString, err := json.Marshal([]map[string]interface{}{node.Data(), node2.Data()})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res := sendTestRequest(queryURL+"main/n", "POST", []byte(jsonString[5:]))

	if st != "400 Bad Request" ||
		res != "Could not decode request body as list of nodes: invalid character 'y' looking for beginning of value" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"main/n", "POST", []byte(jsonString))

	if st != "400 Bad Request" ||
		res != "GraphError: Invalid data (Node is missing a kind value)" {
		t.Error("Unexpected response:", st, res)
		return
	}

	node.SetAttr("kind", "graphtest")
	node2.SetAttr("kind", "graphtest")

	jsonString, err = json.Marshal([]map[string]interface{}{node.Data(), node2.Data()})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"main/n", "POST", []byte(jsonString))

	if st != "200 OK" {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Check that the node was stored

	n, err := api.GM.FetchNode("main", "111", "graphtest")
	if err != nil {
		t.Error(err)
		return
	}

	if !data.NodeCompare(n, node, nil) {
		t.Error("Stored node does not match given node")
		return
	}

	n, err = api.GM.FetchNode("main", "112", "graphtest")
	if err != nil {
		t.Error(err)
		return
	}

	if !data.NodeCompare(n, node2, nil) {
		t.Error("Stored node does not match given node")
		return
	}

	// Try to store a relationship

	edge := data.NewGraphEdge()

	edge.SetAttr("key", "123")
	edge.SetAttr("kind", "testrel")

	edge.SetAttr(data.EdgeEnd1Kind, node.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "node1")
	edge.SetAttr(data.EdgeEnd1Cascading, false)

	edge.SetAttr(data.EdgeEnd2Key, node2.Key())
	edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "node2")
	edge.SetAttr(data.EdgeEnd2Cascading, false)

	jsonString, err = json.Marshal([]map[string]interface{}{edge.Data()})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"main/e", "POST", []byte(jsonString))

	if st != "400 Bad Request" ||
		res != "GraphError: Invalid data (Edge is missing a key value for end1)" {
		t.Error("Unexpected response:", st, res)
		return
	}

	edge.SetAttr(data.EdgeEnd1Key, "foo")

	jsonString, err = json.Marshal([]map[string]interface{}{edge.Data()})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"main/e", "POST", []byte(jsonString))

	if st != "500 Internal Server Error" ||
		res != "GraphError: Invalid data (Can't find edge endpoint: foo (graphtest))" {
		t.Error("Unexpected response:", st, res)
		return
	}

	edge.SetAttr(data.EdgeEnd1Key, node.Key())

	jsonString, err = json.Marshal([]map[string]interface{}{edge.Data()})
	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"main/e", "POST", []byte(jsonString[5:]))

	if st != "400 Bad Request" ||
		res != "Could not decode request body as list of edges: invalid character 'd' looking for beginning of value" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"main/e", "POST", []byte(jsonString))

	if st != "200 OK" {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Check that the edge was stored

	tn, te, err := api.GM.TraverseMulti("main", node.Key(), node.Kind(), ":::", true)
	if err != nil {
		t.Error(err)
		return
	}

	if !data.NodeCompare(tn[0], node2, nil) {
		t.Error("Stored node does not match given node")
		return
	}

	if !data.NodeCompare(te[0], edge, nil) {
		t.Error("Stored edge does not match given node")
		return
	}

	// Update nodes

	st, _, res = sendTestRequest(queryURL+"main", "PUT", []byte(jsonString))

	if st != "400 Bad Request" ||
		res != "Could not decode request body as object with list of nodes and/or edges: json: cannot unmarshal array into Go value of type map[string][]map[string]interface {}" {
		t.Error("Unexpected response:", st, res)
		return
	}

	node.SetAttr("name", "updatenode1")
	node2.SetAttr("name", "updatenode2")
	edge.SetAttr("name", "updateedge")

	jsonString, err = json.Marshal(map[string][]map[string]interface{}{
		"nodes": []map[string]interface{}{node.Data(), node2.Data()},
		"edges": []map[string]interface{}{edge.Data()},
	})

	if err != nil {
		t.Error(err)
		return
	}

	st, _, res = sendTestRequest(queryURL+"main", "PUT", []byte(jsonString))

	if st != "200 OK" {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Check that the nodes and the edge were updated

	tn, te, err = api.GM.TraverseMulti("main", node.Key(), node.Kind(), ":::", true)
	if err != nil {
		t.Error(err)
		return
	}

	if !data.NodeCompare(tn[0], node2, nil) {
		t.Error("Stored node does not match given node")
		return
	}

	if !data.NodeCompare(te[0], edge, nil) {
		t.Error("Stored edge does not match given node")
		return
	}

	// Delete edge

	jsonString, err = json.Marshal(map[string][]map[string]interface{}{
		"nodes": []map[string]interface{}{},
		"edges": []map[string]interface{}{
			map[string]interface{}{
				"key":  edge.Key(),
				"kind": edge.Kind(),
			},
		},
	})

	st, _, res = sendTestRequest(queryURL+"main", "DELETE", []byte(jsonString))

	if st != "200 OK" {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Check that the edge no longer exists

	tn, _, err = api.GM.TraverseMulti("main", node.Key(), node.Kind(), ":::", true)
	if err != nil {
		t.Error(err)
		return
	}

	if len(tn) != 0 {
		t.Error("Unexpected traversal result: ", tn)
		return
	}

	// Delete node

	jsonString, err = json.Marshal(map[string][]map[string]interface{}{
		"edges": []map[string]interface{}{},
		"nodes": []map[string]interface{}{
			map[string]interface{}{
				"key":  node.Key(),
				"kind": node.Kind(),
			},
		},
	})

	st, _, res = sendTestRequest(queryURL+"main", "DELETE", []byte(jsonString))

	if st != "200 OK" {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Check that the node was deleted

	nres, err := api.GM.FetchNode("main", node.Key(), node.Kind())
	if err != nil {
		t.Error(err)
		return
	}

	if nres != nil {
		t.Error("Unexpected node query result:", nres)
		return
	}
}
