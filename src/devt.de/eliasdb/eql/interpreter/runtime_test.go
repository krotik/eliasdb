/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package interpreter

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"devt.de/eliasdb/eql/parser"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/graph/util"
	"devt.de/eliasdb/hash"
	"devt.de/eliasdb/storage"
)

type testNodeInfo struct {
	*defaultNodeInfo
}

func (ni *testNodeInfo) SummaryAttributes(kind string) []string {
	return []string{"key"}
}

/*
Helper function to run a search and check against a result.
*/
func runSearch(query string, expectedResult string, rt parser.RuntimeProvider) error {
	ast, err := parser.ParseWithRuntime("test", query, rt)
	if err != nil {
		return err
	}

	res, err := ast.Runtime.Eval()
	if err != nil {
		return err
	}

	res.(*SearchResult).StableSort()
	if fmt.Sprint(res) != expectedResult {
		return errors.New(fmt.Sprint("Unexpected search result:", res, err))
	}

	return nil
}

func TestLookup(t *testing.T) {
	gm, _ := simpleGraph()
	rt := NewLookupRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if err := runSearch("lookup mynode '000', '123'", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
000, Node0
123, Node1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("lookup mynode '000', '123' where true", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
000, Node0
123, Node1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}
}

func TestMultiKindTraversal(t *testing.T) {
	gm := multiKindGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	// Test simple traversal and setting of primary kind

	if err := runSearch("get mynode0 traverse :::", `
Labels: Mynode0 Key, Name, Type, Key, Kind, Name
Format: auto, auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:Name, 1:n:Type, 2:n:key, 2:n:kind, 2:n:name
000, Node0, root, 123, mynode1, <not set>
000, Node0, root, 456, mynode2, <not set>
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	// Test simple traversal and access the edge

	if err := runSearch("get mynode0 traverse :myedge:: end show mynode0:key, 2:n:key, 2:e:key, myedge:name", `
Labels: Mynode0 Key, Key, Key, Myedge Name
Format: auto, auto, auto, auto
Data: 1:n:key, 2:n:key, 2:e:key, 2:e:name
000, 123, abc1, edge:abc1
000, 456, abc2, edge:abc2
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	// Test primary kind

	ast, err := parser.ParseWithRuntime("test", "get mynode0 traverse ::: end primary mynode2", rt)
	if err != nil {
		t.Error(err)
		return
	}

	res, err := ast.Runtime.Eval()
	if err != nil {
		t.Error(err)
		return
	}

	if pk := res.(*SearchResult).ResPrimaryKind; pk != "mynode2" {
		t.Error("Unexpected primary kind:", pk)
		return
	}

	ast, err = parser.ParseWithRuntime("test", "get mynode0 traverse ::: end primary bla", rt)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = ast.Runtime.Eval()
	if err.Error() != "EQL error in test: Unknown node kind (bla) (Line:1 Pos:38)" {
		t.Error(err)
		return
	}
}

func TestErrors(t *testing.T) {
	gm, mgs := simpleGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	// Test simple eval error

	generalProviderMap[parser.NodeGET] = invalidRuntimeInst

	if err := runSearch("get mynode", "", rt); err.Error() !=
		"EQL error in test: Invalid construct (get) (Line:1 Pos:1)" {
		t.Error(err)
		delete(generalProviderMap, parser.NodeGET)
		return
	}

	invast, err := parser.ParseWithRuntime("test", "get mynode", rt)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = invast.Runtime.(*invalidRuntime).CondEval(nil, nil)
	if err.Error() != "EQL error in test: Invalid construct (get) (Line:1 Pos:1)" {
		t.Error(err)
		return
	}

	delete(generalProviderMap, parser.NodeGET)

	// Test validation errors

	if err := runSearch("get mynode show x traverse :::", "", rt); err.Error() !=
		"EQL error in test: Invalid construct (traversals must be before show clause) (Line:1 Pos:19)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode traverse ::", "", rt); err.Error() !=
		"EQL error in test: Invalid traversal spec (::) (Line:1 Pos:12)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode show mynode0:bla", "", rt); err.Error() !=
		"EQL error in test: Invalid construct (Cannot determine data position for kind: mynode0) (Line:1 Pos:17)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode show 0:n:bla", "", rt); err.Error() !=
		"EQL error in test: Invalid construct (Invalid data index: 0:n:bla (index must be greater than 0)) (Line:1 Pos:17)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode show a:n:bla", "", rt); err.Error() !=
		"EQL error in test: Invalid construct (Invalid data index: a:n:bla (strconv.Atoi: parsing \"a\": invalid syntax)) (Line:1 Pos:17)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode show 1:b:bla", "", rt); err.Error() !=
		"EQL error in test: Invalid construct (Invalid data source 'b' (either n - Node or e - Edge)) (Line:1 Pos:17)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode show 2:n:bla", "", rt); err.Error() !=
		"EQL error in test: Invalid column data spec (Data index out of range: 2) (Line:1 Pos:17)" {
		t.Error(err)
		return
	}

	// Test datastore errors

	msm := mgs.StorageManager("main"+"mynode"+graph.StorageSuffixNodes, false).(*storage.MemoryStorageManager)

	msm.AccessMap[1] = storage.AccessCacheAndFetchError

	if err := runSearch("get mynode", "", rt); err.Error() !=
		"GraphError: Failed to access graph storage component (Slot not found (mystorage/mainmynode.nodes - Location:1))" {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, 1)

	// Test nextStartKey error

	msm.AccessMap[1] = storage.AccessCacheAndFetchSeriousError

	if _, err := rt.nextStartKey(); err.Error() != "GraphError: Could not read graph information (Record is already in-use (? - ))" {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, 1)

	// Test FetchNodePart error

	msm.AccessMap[3] = storage.AccessCacheAndFetchError

	if err := runSearch("get mynode", "", rt); err.Error() !=
		"GraphError: Could not read graph information (Slot not found (mystorage/mainmynode.nodes - Location:3))" {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, 3)

	// Test TraverseMulti errors

	msm = mgs.StorageManager("main"+"mynewnode"+graph.StorageSuffixNodes, false).(*storage.MemoryStorageManager)

	msm.AccessMap[5] = storage.AccessCacheAndFetchError // Node 3 attribute lookup

	if err := runSearch("get mynode traverse :::mynewnode traverse :::mynewnode end end", "", rt); err.Error() !=
		"GraphError: Could not read graph information (Slot not found (mystorage/mainmynewnode.nodes - Location:5))" {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, 5)

	msm.AccessMap[11] = storage.AccessCacheAndFetchError // Traversal spec error

	if err := runSearch("get mynode traverse :::mynewnode traverse :::mynewnode end end", "", rt); err.Error() !=
		"GraphError: Could not read graph information (Slot not found (mystorage/mainmynewnode.nodes - Location:11))" {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, 11)

	msm = mgs.StorageManager("main"+"myedge"+graph.StorageSuffixEdges, false).(*storage.MemoryStorageManager)

	msmtree, _ := hash.LoadHTree(msm, msm.Root(graph.RootIDNodeHTree))
	nm := util.NewNamesManager(mgs.MainDB())
	_, slot, _ := msmtree.GetValueAndLocation([]byte(graph.PrefixNSAttr + "abc2" + nm.Encode32("name", false)))

	msm.AccessMap[slot] = storage.AccessCacheAndFetchError // Edge attribute

	if err := runSearch("get mynode traverse :::mynewnode end show 1:n:key, 2:n:key, 2:e:key, 2:e:name", "", rt); strings.HasPrefix(err.Error(),
		"GraphError: Could not read graph information (Slot not found (mystorage/mainmynewnode.nodes - ") {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, slot)

	// Test unknown ast node

	ast, err := parser.ParseWithRuntime("test", "get mynode show x", rt)
	if err != nil {
		t.Error(err)
		return
	}

	unknownASTNode := &parser.ASTNode{}
	unknownASTNode.Name = "Unknown"
	unknownASTNode.Token = &parser.LexToken{}
	ast.Children[1].Children = append(ast.Children[1].Children, unknownASTNode)

	if err = ast.Runtime.Validate(); err.Error() != "EQL error in test: Invalid construct (Unknown)" {
		t.Error(err)
		return
	}

	ast, err = parser.ParseWithRuntime("test", "get mynode show x as y", rt)
	if err != nil {
		t.Error(err)
		return
	}

	ast.Children[1].Children[0].Children = append(ast.Children[1].Children[0].Children, unknownASTNode)

	if err = ast.Runtime.Validate(); err.Error() != "EQL error in test: Invalid construct (Unknown)" {
		t.Error(err)
		return
	}

	ast, err = parser.ParseWithRuntime("test", "get mynode traverse ::: traverse ::: end end", rt)
	if err != nil {
		t.Error(err)
		return
	}

	ast.Children[1].Children[1].Children = append(ast.Children[1].Children[1].Children, unknownASTNode)

	if err = ast.Runtime.Validate(); err.Error() != "EQL error in test: Invalid construct (Unknown)" {
		t.Error(err)
		return
	}

	ast, err = parser.ParseWithRuntime("test", "get mynode", rt)
	if err != nil {
		t.Error(err)
		return
	}

	ast.Children = append(ast.Children, unknownASTNode)

	if err = ast.Runtime.Validate(); err.Error() != "EQL error in test: Invalid construct (Unknown)" {
		t.Error(err)
		return
	}

	ast.Children = ast.Children[:1]

	// Test add row errors

	if err = ast.Runtime.Validate(); err != nil {
		t.Error(err)
		return
	}

	ast.Runtime.(*getRuntime).rtp.colData[0] = "uu"
	res := newSearchResult(ast.Runtime.(*getRuntime).rtp.eqlRuntimeProvider, "")

	if err = res.addRow([]data.Node{data.NewGraphNode()}, nil); err.Error() !=
		"EQL result error in test: Invalid column data spec (Column data spec must have 3 items: uu)" {
		t.Error(err)
		return
	}

	ast.Runtime.(*getRuntime).rtp.colData[0] = "0:p:test"
	res = newSearchResult(ast.Runtime.(*getRuntime).rtp.eqlRuntimeProvider, "")

	if err = res.addRow([]data.Node{data.NewGraphNode()}, nil); err.Error() !=
		"EQL result error in test: Invalid column data spec (Invalid data source 'p' (either n - Node or e - Edge))" {
		t.Error(err)
		return
	}

	ast.Runtime.(*getRuntime).rtp.colData[0] = "0:e:test"
	allowMultiEval = true

	if _, err = ast.Runtime.Eval(); err.Error() !=
		"EQL result error in test: Invalid column data spec (Invalid data index: 0:e:test)" {
		t.Error(err)
		return
	}

	allowMultiEval = false

	ast.Runtime.(*getRuntime).rtp.colData[0] = "1:e:test"

	i := 0
	oldNextStartKey := ast.Runtime.(*getRuntime).rtp.nextStartKey
	ast.Runtime.(*getRuntime).rtp.nextStartKey = func() (string, error) {
		i++
		if i == 3 {
			return "", errors.New("testerror")
		}
		return "000", nil
	}

	allowMultiEval = true

	if _, err = ast.Runtime.Eval(); err.Error() != "testerror" {
		t.Error(err)
		return
	}

	allowMultiEval = false

	ast.Runtime.(*getRuntime).rtp.nextStartKey = oldNextStartKey

	// Test valid result

	r, err := ast.Runtime.Eval()
	if err != nil || fmt.Sprint(r) != `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
123, Node1
000, Node0
`[1:] {
		t.Error(r, err)
		return
	}
}

func TestBasicTraversalAndShow(t *testing.T) {
	gm, _ := simpleGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, &testNodeInfo{&defaultNodeInfo{gm}})

	// Test unknown initial kind

	ast, err := parser.ParseWithRuntime("test", "get mymissingnode", rt)
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := ast.Runtime.Eval(); err.Error() !=
		"EQL error in test: Unknown node kind (mymissingnode) (Line:1 Pos:5)" {
		t.Error(err)
		return
	}

	// Test simple query with traversals

	ast, err = parser.ParseWithRuntime("test", `
get mynode 
	traverse :::mynewnode 
		traverse :::mynewnode 
		end 
	end 
	traverse :::mynewnode 
	end`, rt)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected result explanation:
	// Row 1 - Node 000 is alone so no traversals possible
	// Row 2, 3, 4, 5 - there are two edges from 123 to 456 so possible traversals are doubled
	//                  456 is also connected to 789 and 789-2
	// Node 123 has also a direct connection to xxx
	// 6 and 7 are duplicated because of the two edges from 123 to 456
	// 8 is the last combination of twice traversing from 123 to xxx

	expectedResult := `
Labels: Mynode Key, Mynewnode Key, Mynewnode Key, Mynewnode Key
Format: auto, auto, auto, auto
Data: 1:n:key, 2:n:key, 3:n:key, 4:n:key
123, 456, 789-2, 456
123, 456, 789-2, 456
123, 456, 789, 456
123, 456, 789, 456
123, xxx ⌘, 789, 456
123, xxx ⌘, 789, 456
123, xxx ⌘, 789, xxx ⌘
`[1:]

	res, err := ast.Runtime.Eval()
	if err != nil {
		t.Error(err)
		return
	}

	res.(*SearchResult).StableSort()

	if fmt.Sprint(res) != expectedResult {
		t.Error("Unexpected search result:", res, err)
		return
	}

	// Test showing empty traversals

	grt := ast.Runtime.(*getRuntime).rtp

	ast.Runtime.Validate()

	allowMultiEval = true
	grt.eqlRuntimeProvider.allowNilTraversal = true

	res, err = ast.Runtime.Eval()
	if err != nil {
		t.Error(err)
		return
	}

	res.(*SearchResult).StableSort()

	expectedResult = `
Labels: Mynode Key, Mynewnode Key, Mynewnode Key, Mynewnode Key
Format: auto, auto, auto, auto
Data: 1:n:key, 2:n:key, 3:n:key, 4:n:key
000, <not set>, <not set>, <not set>
123, 456, 789-2, 456
123, 456, 789-2, 456
123, 456, 789, 456
123, 456, 789, 456
123, xxx ⌘, 789, 456
123, xxx ⌘, 789, 456
123, xxx ⌘, 789, xxx ⌘
`[1:]

	if fmt.Sprint(res) != expectedResult {
		t.Error("Unexpected search result:", res, err)
		return
	}

	allowMultiEval = false

	// Test showing the traversal attributes in reverse order

	ast, err = parser.ParseWithRuntime("test", `
get mynode
	traverse :::mynewnode
		traverse :::mynewnode 
		end 
	end 
show 3:n:key AS key1,
2:n:key FORMAT keystring1,
key as key3 FORMAT x`, rt)
	if err != nil {
		t.Error(err)
		return
	}

	// Expected result explanation:
	// Row 1, 2, 3, 4 - there are two edges from 123 to 456 so possible traversals are doubled
	//                  456 is also connected to 789 and 789-2
	// Row 5 - Node 123 has also a direct connection to xxx
	// Row 6 - Node 000 is alone so no traversals possible

	expectedResult = `
Labels: key1, Key, key3
Format: auto, keystring1, x
Data: 3:n:key, 2:n:key, 1:n:key
789-2, 456, 123
789-2, 456, 123
789, 456, 123
789, 456, 123
789, xxx ⌘, 123
`[1:]

	res, err = ast.Runtime.Eval()
	if err != nil {
		t.Error(err)
		return
	}

	res.(*SearchResult).StableSort()

	if fmt.Sprint(res) != expectedResult {
		t.Error("Unexpected search result:", res, err)
		return
	}
}

func simpleGraph() (*graph.Manager, *graphstorage.MemoryGraphStorage) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	constructEdge := func(key string, node1 data.Node, node2 data.Node) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", key)
		edge.SetAttr("kind", "myedge")

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "node1")
		edge.SetAttr(data.EdgeEnd1Cascading, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "node2")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr(data.NodeName, "Edge1"+key)

		return edge
	}

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "mynode")
	node0.SetAttr("Name", "Node0")
	gm.StoreNode("main", node0)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("Name", "Node1")
	gm.StoreNode("main", node1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynewnode")
	node2.SetAttr("Na me", "Node2")
	gm.StoreNode("main", node2)

	node3 := data.NewGraphNode()
	node3.SetAttr("key", "789")
	node3.SetAttr("kind", "mynewnode")
	node3.SetAttr("Name", "Node3")
	gm.StoreNode("main", node3)

	node33 := data.NewGraphNode()
	node33.SetAttr("key", "789-2")
	node33.SetAttr("kind", "mynewnode")
	node33.SetAttr("Name", "Node3-2")
	gm.StoreNode("main", node33)

	node4 := data.NewGraphNode()
	node4.SetAttr("key", "xxx \xe2\x8c\x98")
	node4.SetAttr("kind", "mynewnode")
	node4.SetAttr("Nam \xe2\x8c\x98 e", "Node4")
	gm.StoreNode("main", node4)

	gm.StoreEdge("main", constructEdge("abc2", node1, node2))
	gm.StoreEdge("main", constructEdge("abc3", node1, node2))
	gm.StoreEdge("main", constructEdge("abc1", node2, node3))
	gm.StoreEdge("main", constructEdge("abc4", node2, node33))
	gm.StoreEdge("main", constructEdge("abc0", node3, node4))
	gm.StoreEdge("main", constructEdge("abc99", node4, node1))

	return gm, mgs.(*graphstorage.MemoryGraphStorage)
}

func multiKindGraph() *graph.Manager {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	constructEdge := func(key string, node1 data.Node, node2 data.Node) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", key)
		edge.SetAttr("kind", "myedge")

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "src")
		edge.SetAttr(data.EdgeEnd1Cascading, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "dest")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr(data.NodeName, "edge:"+key)

		return edge
	}

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "mynode0")
	node0.SetAttr("Name", "Node0")
	node0.SetAttr("Type", "root")
	gm.StoreNode("main", node0)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode1")
	gm.StoreNode("main", node1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynode2")
	gm.StoreNode("main", node2)

	node3 := data.NewGraphNode()
	node3.SetAttr("key", "789")
	node3.SetAttr("kind", "mynode3")
	gm.StoreNode("main", node3)

	gm.StoreEdge("main", constructEdge("abc1", node0, node1))
	gm.StoreEdge("main", constructEdge("abc2", node0, node2))
	gm.StoreEdge("main", constructEdge("abc3", node2, node3))

	return gm
}

func dataNodes() *graph.Manager {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "mynode")
	node0.SetAttr("name", "Node0")
	node0.SetAttr("type", "type1")
	gm.StoreNode("main", node0)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("name", "Node1")
	node1.SetAttr("nested.nest1.nest2.atom1", 1.46)
	node1.SetAttr("type", "type1")
	gm.StoreNode("main", node1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynode")
	node2.SetAttr("name", "Node2")
	node2.SetAttr("type", "type2")
	node2.SetAttr("nested", map[string]interface{}{
		"nest1": map[string]interface{}{
			"nest2": map[string]interface{}{
				"atom1": 1.45,
			},
		},
	})
	gm.StoreNode("main", node2)

	return gm
}
