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
	"testing"

	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
	"devt.de/krotik/eliasdb/graph/util"
)

type TestRule struct {
	handleError bool
	commitError bool
}

func (r *TestRule) Name() string {
	return "testrule"
}

func (r *TestRule) Handles() []int {
	return []int{EventNodeCreated, EventNodeUpdated, EventNodeDeleted,
		EventEdgeCreated, EventEdgeUpdated, EventEdgeDeleted}
}

func (r *TestRule) Handle(gm *Manager, trans Trans, event int, ed ...interface{}) error {
	if r.handleError {
		return &util.GraphError{Type: util.ErrAccessComponent, Detail: "Test error"}
	}

	if r.commitError {
		node := data.NewGraphNode()
		node.SetAttr("key", "123")
		node.SetAttr("kind", "bla")

		edge := data.NewGraphEdge()
		edge.SetAttr("key", "123")
		edge.SetAttr("kind", "myedge")

		edge.SetAttr(data.EdgeEnd1Key, node.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "node1")
		edge.SetAttr(data.EdgeEnd1Cascading, false)

		edge.SetAttr(data.EdgeEnd2Key, node.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "node2")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		trans.StoreEdge("test", edge)
	}

	return nil
}

func TestCascadingLastRules(t *testing.T) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := NewGraphManager(mgs)

	constructEdge := func(key string, node1 data.Node, node2 data.Node) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", key)
		edge.SetAttr("kind", "myedge")

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "node1")
		edge.SetAttr(data.EdgeEnd1Cascading, true)
		edge.SetAttr(data.EdgeEnd1CascadingLast, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "node2")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr(data.NodeName, "Edge1"+key)

		return edge
	}

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "Artist")
	node1.SetAttr("Name", "Artist1")
	gm.StoreNode("main", node1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "Song")
	node2.SetAttr("Name", "Song1")
	gm.StoreNode("main", node2)

	node3 := data.NewGraphNode()
	node3.SetAttr("key", "789")
	node3.SetAttr("kind", "Song")
	node3.SetAttr("Name", "Song2")
	gm.StoreNode("main", node3)

	gm.StoreEdge("main", constructEdge("abc1", node2, node1))
	gm.StoreEdge("main", constructEdge("abc2", node3, node1))

	gm.RemoveNode("main", "456", "Song")

	n, _ := gm.FetchNode("main", "123", "Artist")
	if n == nil {
		t.Error("Artist node should have not been deleted")
		return
	}

	gm.RemoveNode("main", "789", "Song")

	n, _ = gm.FetchNode("main", "123", "Artist")
	if n != nil {
		t.Error("Last removed Song node should have deleted the Artist node")
		return
	}

	// Now again but with multiple relationships

	mgs = graphstorage.NewMemoryGraphStorage("mystorage")
	gm = NewGraphManager(mgs)

	node1 = data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "Artist")
	node1.SetAttr("Name", "Artist1")
	gm.StoreNode("main", node1)

	node2 = data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "Song")
	node2.SetAttr("Name", "Song1")
	gm.StoreNode("main", node2)

	gm.StoreEdge("main", constructEdge("abc1", node2, node1))
	gm.StoreEdge("main", constructEdge("abc2", node2, node1))

	gm.RemoveNode("main", "456", "Song")

	n, _ = gm.FetchNode("main", "123", "Artist")
	if n != nil {
		t.Error("Artist node should have been deleted")
		return
	}
}

func TestRules(t *testing.T) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := NewGraphManager(mgs)

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

	constructGraph := func() {

		mgs = graphstorage.NewMemoryGraphStorage("mystorage")
		gm = NewGraphManager(mgs)

		node1 := data.NewGraphNode()
		node1.SetAttr("key", "123")
		node1.SetAttr("kind", "mynode")
		node1.SetAttr("Name", "Node1")
		gm.StoreNode("main", node1)

		node2 := data.NewGraphNode()
		node2.SetAttr("key", "456")
		node2.SetAttr("kind", "mynewnode")
		node2.SetAttr("Name", "Node2")
		gm.StoreNode("main", node2)

		node3 := data.NewGraphNode()
		node3.SetAttr("key", "789")
		node3.SetAttr("kind", "mynewnode")
		node3.SetAttr("Name", "Node3")
		gm.StoreNode("main", node3)

		node4 := data.NewGraphNode()
		node4.SetAttr("key", "xxx")
		node4.SetAttr("kind", "mynewnode")
		node4.SetAttr("Name", "Node4")
		gm.StoreNode("main", node4)

		gm.StoreEdge("main", constructEdge("abc2", node1, node2))
		gm.StoreEdge("main", constructEdge("abc3", node1, node2))
		gm.StoreEdge("main", constructEdge("abc1", node2, node3))
		gm.StoreEdge("main", constructEdge("abc0", node3, node4))
		gm.StoreEdge("main", constructEdge("abc99", node4, node1))
	}

	// Check rules on empty graph

	if len(gm.NodeKinds()) != 0 {
		t.Error("Unexpected node kinds result")
		return
	}
	if len(gm.Partitions()) != 0 {
		t.Error("Unexpected node kinds result")
		return
	}
	if len(gm.EdgeKinds()) != 0 {
		t.Error("Unexpected node kinds result")
		return
	}

	if len(gm.NodeAttrs("test")) != 0 {
		t.Error("Unexpected node attributes result")
		return
	}

	if _, err := gm.RemoveNode("test", "test", "test"); err != nil {
		t.Error(err)
		return
	}

	constructGraph()

	if res := fmt.Sprint(gm.NodeKinds()); res != "[mynewnode mynode]" {
		t.Error("Unexpected node kinds result:", res)
		return
	}

	if res := fmt.Sprint(gm.EdgeKinds()); res != "[myedge]" {
		t.Error("Unexpected edge kinds result:", res)
		return
	}

	if res := fmt.Sprint(gm.Partitions()); res != "[main]" {
		t.Error("Unexpected partitions result:", res)
		return
	}

	if res := fmt.Sprint(gm.NodeAttrs("mynode")); res != "[Name key kind]" {
		t.Error("Unexpected node attributes result:", res)
		return
	}

	// Test updating a node

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("Name2", "Node1Name2")

	gm.StoreNode("main", node1)

	if res := fmt.Sprint(gm.NodeAttrs("mynode")); res != "[Name Name2 key kind]" {
		t.Error("Unexpected node attributes result:", res)
		return
	}

	// Test edge specs

	if res := fmt.Sprint(gm.NodeEdges("mynode")); res != "[node1:myedge:node2:mynewnode node2:myedge:node1:mynewnode]" {
		t.Error("Unexpected node attributes result:", res)
		return
	}

	if res := fmt.Sprint(gm.EdgeAttrs("myedge")); res != "[end1cascading end1key end1kind end1role end2cascading end2key end2kind end2role key kind name]" {
		t.Error("Unexpected node attributes result:", res)
		return
	}

	if c := gm.NodeCount("mynode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	} else if c := gm.NodeCount("mynewnode"); c != 3 {
		t.Error("Unexpected node count:", c)
		return
	} else if c := gm.EdgeCount("myedge"); c != 5 {
		t.Error("Unexpected edge count:", c)
		return
	}

	gm.RemoveNode("main", "123", "mynode")

	if c := gm.NodeCount("mynode"); c != 0 {
		t.Error("Unexpected node count:", c)
		return
	} else if c := gm.NodeCount("mynewnode"); c != 0 {
		t.Error("Unexpected node count:", c)
		return
	} else if c := gm.EdgeCount("myedge"); c != 0 {
		t.Error("Unexpected edge count:", c)
		return
	}

	// Finally test a rather esotheric error case

	trans := NewConcurrentGraphTrans(gm)

	err := gm.gr.graphEvent(trans, EventNodeDeleted, "test 1", node1)

	if err.Error() !=
		"GraphError: Graph rule error (GraphError: Invalid data (Partition name "+
			"test 1 is not alphanumeric - can only contain [a-zA-Z0-9_]))" {

		t.Error(err)
		return
	}
}

func TestRulesTrans(t *testing.T) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := NewGraphManager(mgs)

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

	constructGraph := func() {

		mgs = graphstorage.NewMemoryGraphStorage("mystorage")
		gm = NewGraphManager(mgs)

		node1 := data.NewGraphNode()
		node1.SetAttr("key", "123")
		node1.SetAttr("kind", "mynode")
		node1.SetAttr("Name", "Node1")
		gm.StoreNode("main", node1)

		node2 := data.NewGraphNode()
		node2.SetAttr("key", "456")
		node2.SetAttr("kind", "mynewnode")
		node2.SetAttr("Name", "Node2")
		gm.StoreNode("main", node2)

		node3 := data.NewGraphNode()
		node3.SetAttr("key", "789")
		node3.SetAttr("kind", "mynewnode")
		node3.SetAttr("Name", "Node3")
		gm.StoreNode("main", node3)

		node4 := data.NewGraphNode()
		node4.SetAttr("key", "xxx")
		node4.SetAttr("kind", "mynewnode")
		node4.SetAttr("Name", "Node4")
		gm.StoreNode("main", node4)

		gm.StoreEdge("main", constructEdge("abc2", node1, node2))
		gm.StoreEdge("main", constructEdge("abc3", node1, node2))
		gm.StoreEdge("main", constructEdge("abc1", node2, node3))
		gm.StoreEdge("main", constructEdge("abc0", node3, node4))
		gm.StoreEdge("main", constructEdge("abc99", node4, node1))
	}

	// Check rules on empty graph

	if _, err := gm.RemoveNode("test", "test", "test"); err != nil {
		t.Error(err)
		return
	}

	constructGraph()

	if c := gm.NodeCount("mynode"); c != 1 {
		t.Error("Unexpected node count:", c)
		return
	} else if c := gm.NodeCount("mynewnode"); c != 3 {
		t.Error("Unexpected node count:", c)
		return
	} else if c := gm.EdgeCount("myedge"); c != 5 {
		t.Error("Unexpected edge count:", c)
		return
	}

	trans := NewConcurrentGraphTrans(gm)
	trans.RemoveNode("main", "123", "mynode")

	if err := trans.Commit(); err != nil {
		t.Error(err)
		return
	}

	if c := gm.NodeCount("mynode"); c != 0 {
		t.Error("Unexpected node count:", c)
		return
	} else if c := gm.NodeCount("mynewnode"); c != 0 {
		t.Error("Unexpected node count:", c)
		return
	} else if c := gm.EdgeCount("myedge"); c != 0 {
		t.Error("Unexpected edge count:", c)
		return
	}
}

func TestRulesErrors(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := NewGraphManager(mgs)

	tr := &TestRule{false, false}

	gm.SetGraphRule(tr)

	// Check that the test rule was added

	if rules := fmt.Sprint(gm.GraphRules()); rules !=
		"[system.deletenodeedges system.updatenodestats testrule]" {
		t.Error("unexpected graph rule list:", rules)
		return
	}

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "456")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("Name", "Node1")

	tr.handleError = true
	tr.commitError = false

	if err := gm.StoreNode("main", node1); err.Error() !=
		"GraphError: Graph rule error (GraphError: Failed to access graph storage component (Test error))" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = true

	if err := gm.StoreNode("main", node1); err.Error() !=
		"GraphError: Invalid data (Can't store edge to non-existing node kind: bla)" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = true
	tr.commitError = false

	if _, err := gm.RemoveNode("main", node1.Key(), node1.Kind()); err.Error() !=
		"GraphError: Graph rule error (GraphError: Failed to access graph storage component (Test error))" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = true

	if _, err := gm.RemoveNode("main", node1.Key(), node1.Kind()); err.Error() !=
		"GraphError: Invalid data (Can't store edge to non-existing node kind: bla)" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = true
	tr.commitError = false

	edge := data.NewGraphEdge()
	edge.SetAttr("key", "123")
	edge.SetAttr("kind", "myedge")

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "node1")
	edge.SetAttr(data.EdgeEnd1Cascading, false)

	edge.SetAttr(data.EdgeEnd2Key, node1.Key())
	edge.SetAttr(data.EdgeEnd2Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "node2")
	edge.SetAttr(data.EdgeEnd2Cascading, false)

	if err := gm.StoreEdge("main", edge); err.Error() !=
		"GraphError: Graph rule error (GraphError: Failed to access graph storage component (Test error))" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = true

	if err := gm.StoreEdge("main", edge); err.Error() !=
		"GraphError: Invalid data (Can't store edge to non-existing node kind: bla)" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreEdge("main", edge); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = true

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); err.Error() !=
		"GraphError: Invalid data (Can't store edge to non-existing node kind: bla)" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreEdge("main", edge); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = true
	tr.commitError = false

	if _, err := gm.RemoveEdge("main", edge.Key(), edge.Kind()); err.Error() !=
		"GraphError: Graph rule error (GraphError: Failed to access graph storage component (Test error))" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreEdge("main", edge); err != nil {
		t.Error(err)
		return
	}

	// Test transaction errors

	trans := NewConcurrentGraphTrans(gm)

	tr.handleError = true
	tr.commitError = false

	trans.StoreNode("main", node1)

	if err := trans.Commit(); err.Error() !=
		"GraphError: Graph rule error (GraphError: Failed to access graph storage component (Test error))" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = true

	trans.StoreNode("main", node1)

	if err := trans.Commit(); err.Error() !=
		"GraphError: Invalid data (Can't store edge to non-existing node kind: bla)" {
		t.Error(err)
		return
	}

	tr.handleError = true
	tr.commitError = false

	trans.RemoveNode("main", node1.Key(), node1.Kind())

	if err := trans.Commit(); err.Error() !=
		"GraphError: Graph rule error (GraphError: Failed to access graph storage component (Test error))" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = true

	trans.RemoveNode("main", node1.Key(), node1.Kind())

	if err := trans.Commit(); err.Error() !=
		"GraphError: Invalid data (Can't store edge to non-existing node kind: bla)" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreNode("main", node1); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = true
	tr.commitError = false

	trans.StoreEdge("main", edge)

	if err := trans.Commit(); err.Error() !=
		"GraphError: Graph rule error (GraphError: Failed to access graph storage component (Test error))" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = true

	trans.StoreEdge("main", edge)

	if err := trans.Commit(); err.Error() !=
		"GraphError: Invalid data (Can't store edge to non-existing node kind: bla)" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreEdge("main", edge); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = true

	trans.RemoveEdge("main", edge.Key(), edge.Kind())

	if err := trans.Commit(); err.Error() !=
		"GraphError: Invalid data (Can't store edge to non-existing node kind: bla)" {
		t.Error(err)
		return
	}

	tr.handleError = false
	tr.commitError = false

	if err := gm.StoreEdge("main", edge); err != nil {
		t.Error(err)
		return
	}

	tr.handleError = true
	tr.commitError = false

	trans.RemoveEdge("main", edge.Key(), edge.Kind())

	if err := trans.Commit(); err.Error() !=
		"GraphError: Graph rule error (GraphError: Failed to access graph storage component (Test error))" {
		t.Error(err)
		return
	}
}
