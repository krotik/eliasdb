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
	"bytes"
	"strings"
	"testing"

	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
	"devt.de/krotik/eliasdb/storage"
)

func TestImportExportError(t *testing.T) {
	var res bytes.Buffer

	// Create a memory only storage

	gs := graphstorage.NewMemoryGraphStorage("test")
	gm := NewGraphManager(gs)

	// Test incomplete import data

	err := ImportPartition(bytes.NewBufferString(`
{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "X",
`), "main", gm)

	if err == nil || err.Error() != "Could not decode file content as object with list of nodes and edges: unexpected EOF" {
		t.Error("Unexpected result:", err)
		return
	}

	// Export an empty graph

	err = ExportPartition(&res, "aaa", gm)

	if err != nil || res.String() != `{
  "nodes" : [
  ],
  "edges" : [
  ]
}` {
		t.Error("Unexpected result:", res.String(), err)
		return
	}

	// Try exporting nodes with unexportable attibutes

	err = gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "123",
		"kind": "bla",
		"test": data.NewGraphNode,
	}))

	if err != nil {
		t.Error(err)
		return
	}

	res.Reset()
	err = ExportPartition(&res, "main", gm)
	sortRes := SortDump(res.String())

	if err != nil || sortRes != `{
    "edges": [],
    "nodes": [
        {
            "key": "123",
            "kind": "bla",
            "test": null
        }
    ]
}` {
		t.Error("Unexpected result:", sortRes, err)
		return
	}

	// Error when reading a node

	msm := gs.StorageManager("main"+"bla"+StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	msm.AccessMap[1] = storage.AccessCacheAndFetchSeriousError

	res.Reset()
	err = ExportPartition(&res, "main", gm)
	if !strings.HasPrefix(err.Error(), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 1)

	err = gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "456",
		"kind": "bla",
		"test": data.NewGraphNode,
	}))

	msm = gs.StorageManager("main"+"bla"+StorageSuffixNodes, false).(*storage.MemoryStorageManager)
	msm.AccessMap[6] = storage.AccessCacheAndFetchSeriousError

	res.Reset()
	err = ExportPartition(&res, "main", gm)
	if !strings.HasPrefix(err.Error(), "GraphError: Could not read graph information") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 6)

	msm.AccessMap[5] = storage.AccessCacheAndFetchSeriousError

	res.Reset()
	err = ExportPartition(&res, "main", gm)
	if !strings.HasPrefix(err.Error(), "GraphError: Could not read graph information") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 5)

	gm.StoreEdge("main", data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(map[string]interface{}{
		"end1cascading": false,
		"end1key":       "123",
		"end1kind":      "bla",
		"end1role":      "node",
		"end2cascading": false,
		"end2key":       "456",
		"end2kind":      "bla",
		"end2role":      "node",
		"key":           "3",
		"kind":          "xxx",
	})))

	// Traverse to relationship should fail

	msm.AccessMap[7] = storage.AccessCacheAndFetchSeriousError

	res.Reset()
	err = ExportPartition(&res, "main", gm)
	if !strings.HasPrefix(err.Error(), "GraphError: Could not read graph information") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 7)

	// Lookup of relationship should fail

	msm = gs.StorageManager("main"+"xxx"+StorageSuffixEdges, false).(*storage.MemoryStorageManager)

	msm.AccessMap[1] = storage.AccessCacheAndFetchSeriousError

	res.Reset()
	err = ExportPartition(&res, "main", gm)
	if !strings.HasPrefix(err.Error(), "GraphError: Failed to access graph storage component") {
		t.Error("Unexpected graph error:", err)
		return
	}

	delete(msm.AccessMap, 1)

	// Test invalid import data

	err = ImportPartition(bytes.NewBufferString(`{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "X"
	    },
	    {
	      "key": "2"
	    }
	],
	"edges" : [
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "X",
	      "end1role": "node",
	      "end2cascading": false,
	      "end2key": "2",
	      "end2kind": "Y",
	      "end2role": "node",
	      "key": "4",
	      "kind": "A"
	    }
	]
}`), "main", gm)

	if err == nil || err.Error() != "GraphError: Invalid data (Node is missing a kind value)" {
		t.Error("Unexpected result:", err)
		return
	}

	err = ImportPartition(bytes.NewBufferString(`{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "X"
	    },
	    {
	      "key": "2",
	      "kind": "Y"
	    }
	],
	"edges" : [
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "X",
	      "end1role": "node",
	      "end2key": "2",
	      "end2kind": "Y",
	      "end2role": "node",
	      "key": "4",
	      "kind": "A"
	    }
	]
}`), "main", gm)

	if err == nil || err.Error() != "GraphError: Invalid data (Edge is missing a cascading value for end2)" {
		t.Error("Unexpected result:", err)
		return
	}

	// Do actual import and exports

	gs = graphstorage.NewMemoryGraphStorage("test")
	gm = NewGraphManager(gs)

	err = ImportPartition(bytes.NewBufferString(`{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "X"
	    },
	    {
	      "key": "2",
	      "kind": "Y"
	    }
	],
	"edges" : [
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "X",
	      "end1role": "node",
	      "end2cascading": false,
	      "end2key": "2",
	      "end2kind": "Y",
	      "end2role": "node",
	      "key": "4",
	      "kind": "A"
	    },
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "X",
	      "end1role": "node",
	      "end2cascading": false,
	      "end2key": "2",
	      "end2kind": "Y",
	      "end2role": "node",
	      "key": "5",
	      "kind": "B"
	    }
	]
}`), "main", gm)

	if err != nil {
		t.Error(err)
		return
	}

	err = ImportPartition(bytes.NewBufferString(`{
	"nodes" : [
	    {
	      "key": "1",
	      "kind": "Xfoo"
	    },
	    {
	      "key": "2",
	      "kind": "Yfoo"
	    }
	],
	"edges" : [
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "Xfoo",
	      "end1role": "node",
	      "end2cascading": false,
	      "end2key": "2",
	      "end2kind": "Yfoo",
	      "end2role": "node",
	      "key": "4",
	      "kind": "Afoo"
	    },
	    {
	      "end1cascading": false,
	      "end1key": "1",
	      "end1kind": "Xfoo",
	      "end1role": "node",
	      "end2cascading": false,
	      "end2key": "2",
	      "end2kind": "Yfoo",
	      "end2role": "node",
	      "key": "5",
	      "kind": "Bfoo"
	    }
	]
}`), "foo", gm)

	if err != nil {
		t.Error(err)
		return
	}

	res.Reset()
	err = ExportPartition(&res, "main", gm)
	sortRes = SortDump(res.String())

	if err != nil || sortRes != `{
    "edges": [
        {
            "end1cascading": false,
            "end1key": "1",
            "end1kind": "X",
            "end1role": "node",
            "end2cascading": false,
            "end2key": "2",
            "end2kind": "Y",
            "end2role": "node",
            "key": "4",
            "kind": "A"
        },
        {
            "end1cascading": false,
            "end1key": "1",
            "end1kind": "X",
            "end1role": "node",
            "end2cascading": false,
            "end2key": "2",
            "end2kind": "Y",
            "end2role": "node",
            "key": "5",
            "kind": "B"
        }
    ],
    "nodes": [
        {
            "key": "1",
            "kind": "X"
        },
        {
            "key": "2",
            "kind": "Y"
        }
    ]
}` {
		t.Error("Unexpected result:", sortRes, err)
		return
	}

	// Do an import with the export data and see that nothing changes

	err = ImportPartition(bytes.NewBufferString(sortRes), "main", gm)
	if err != nil {
		t.Error(err)
		return
	}

	res.Reset()
	err = ExportPartition(&res, "main", gm)
	if err != nil {
		t.Error(err)
		return
	}

	sortRes2 := SortDump(res.String())

	if sortRes2 != sortRes {
		t.Error("Export data differs from import data:", sortRes2)
		return
	}

}
