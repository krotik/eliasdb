/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package eql

import (
	"testing"

	"devt.de/eliasdb/eql/interpreter"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
)

func TestBugFixes(t *testing.T) {
	gm, _ := songGraph()

	// Bug 1 - Lookup only giving last AST main child node (show) to runtime instead of first non-value (where)

	res, err := RunQuery("test", "main", "lookup Author '000' traverse :::Song end show Song:key with ordering(ascending key)", gm)

	if err != nil || res.String() != `
Labels: Song Key
Format: auto
Data: 2:n:key
Aria1
Aria2
Aria3
Aria4
`[1:] {
		t.Error("Unexpected result: ", err, res)
		return
	}
}

func TestQuery(t *testing.T) {
	gm, _ := songGraph()

	res, _ := RunQuery("test", "main", "get Author with ordering(ascending key)", gm)
	if res.String() != `
Labels: Author Key, Author Name
Format: auto, auto
Data: 1:n:key, 1:n:name
000, John
123, Mike
456, Hans
`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	if res.Rows() == nil {
		t.Error("Unexpected result")
		return
	}

	if res.RowSources() == nil {
		t.Error("Unexpected result")
		return
	}

	if res.Header().(*interpreter.SearchHeader) != &res.(*queryResult).SearchHeader {
		t.Error("Unexpected header result")
		return
	}

	res, _ = RunQuery("test", "main", "lookup Author '000'", gm)
	if res.String() != `
Labels: Author Key, Author Name
Format: auto, auto
Data: 1:n:key, 1:n:name
000, John
`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	// Test error cases

	_, err := RunQuery("test", "main", "boo Author", gm)
	if err.Error() != "EQL error in test: Invalid construct (Unknown query type: boo) (Line:1 Pos:1)" {
		t.Error(err)
		return
	}

	_, err = RunQuery("test", "main", "get Author where", gm)
	if err.Error() != "Parse error in test: Unexpected end" {
		t.Error(err)
		return
	}

	_, err = RunQuery("test", "main", "get Author traverse ::", gm)
	if err.Error() != "EQL error in test: Invalid traversal spec (::) (Line:1 Pos:12)" {
		t.Error(err)
		return
	}
}

func TestQueryPlainGraph(t *testing.T) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	createNode := func(key string) {
		node0 := data.NewGraphNode()
		node0.SetAttr("key", key)
		node0.SetAttr("kind", "test")
		gm.StoreNode("main", node0)
	}

	createNode("123")
	createNode("1")
	createNode("2")
	createNode("3")

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "4")
	node0.SetAttr("kind", "test")
	node0.SetAttr("name", "bla")
	gm.StoreNode("main", node0)

	res, _ := RunQuery("test", "main", "get test", gm)
	if res.String() != `
Labels: Test Key, Test Name
Format: auto, auto
Data: 1:n:key, 1:n:name
123, <not set>
1, <not set>
2, <not set>
3, <not set>
4, bla
`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}
}

func TestParseQuery(t *testing.T) {
	res, _ := ParseQuery("test", "get Author with ordering(ascending key)")
	if res.String() != `
get
  value: "Author"
  with
    ordering
      asc
        value: "key"
`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	// Test error case

	_, err := ParseQuery("test", "get Author where")
	if err.Error() != "Parse error in test: Unexpected end" {
		t.Error(err)
		return
	}

}

func songGraph() (*graph.Manager, *graphstorage.MemoryGraphStorage) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	constructEdge := func(key string, node1 data.Node, node2 data.Node, number int) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", key)
		edge.SetAttr("kind", "Wrote")

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "Author")
		edge.SetAttr(data.EdgeEnd1Cascading, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "Song")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr("number", number)

		return edge
	}

	storeSong := func(node data.Node, name string, ranking int, number int) {
		node3 := data.NewGraphNode()
		node3.SetAttr("key", name)
		node3.SetAttr("kind", "Song")
		node3.SetAttr("name", name)
		node3.SetAttr("ranking", ranking)
		gm.StoreNode("main", node3)
		gm.StoreEdge("main", constructEdge(name, node, node3, number))
	}

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "Author")
	node0.SetAttr("name", "John")
	gm.StoreNode("main", node0)

	storeSong(node0, "Aria1", 8, 1)
	storeSong(node0, "Aria2", 2, 2)
	storeSong(node0, "Aria3", 4, 3)
	storeSong(node0, "Aria4", 18, 4)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "Author")
	node1.SetAttr("name", "Mike")
	gm.StoreNode("main", node1)

	storeSong(node1, "LoveSong3", 1, 3)
	storeSong(node1, "FightSong4", 3, 4)
	storeSong(node1, "DeadSong2", 6, 2)
	storeSong(node1, "StrangeSong1", 5, 1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "Author")
	node2.SetAttr("name", "Hans")
	gm.StoreNode("main", node2)

	storeSong(node2, "MyOnlySong3", 19, 3)

	return gm, mgs.(*graphstorage.MemoryGraphStorage)
}

func songGraphGroups() (*graph.Manager, *graphstorage.MemoryGraphStorage) {
	gm, mgs := songGraph()

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "Best")
	node0.SetAttr("kind", GroupNodeKind)
	gm.StoreNode("main", node0)

	constructEdge := func(songkey string) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", songkey)
		edge.SetAttr("kind", "Contains")

		edge.SetAttr(data.EdgeEnd1Key, node0.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node0.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "group")
		edge.SetAttr(data.EdgeEnd1Cascading, false)

		edge.SetAttr(data.EdgeEnd2Key, songkey)
		edge.SetAttr(data.EdgeEnd2Kind, "Song")
		edge.SetAttr(data.EdgeEnd2Role, "Song")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		return edge
	}

	gm.StoreEdge("main", constructEdge("LoveSong3"))
	gm.StoreEdge("main", constructEdge("Aria3"))
	gm.StoreEdge("main", constructEdge("MyOnlySong3"))
	gm.StoreEdge("main", constructEdge("StrangeSong1"))

	return gm, mgs
}
