/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package rambazamba

import (
	"bytes"
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

type mockEventPublisher struct {
	buf bytes.Buffer
	err error
}

func (p *mockEventPublisher) AddEvent(name string, kind []string, state map[interface{}]interface{}) error {
	if p.err == nil {
		p.buf.WriteString(fmt.Sprintf("%v-%v-%v-%v-%v-%v-%v", name, kind, state["part"], state["node"], state["edge"],
			state["old_node"], state["old_edge"]))
	}
	return p.err
}

func TestEventSource(t *testing.T) {

	ep := &mockEventPublisher{bytes.Buffer{}, nil}
	log := bytes.Buffer{}

	mgs := graphstorage.NewMemoryGraphStorage("iterator test")
	gm := graph.NewGraphManager(mgs)

	AddEventPublisher(gm, ep, &log)

	if res := fmt.Sprint(gm.GraphRules()); res != "[rambazamba.eventbridge system.deletenodeedges system.updatenodestats]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := fmt.Sprint(ep.buf.String()); res != `
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	// Now generate some events

	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "foo",
		"kind": "bar",
	}))

	if res := fmt.Sprint(ep.buf.String()); res != `
db.node.created-[db.node.created]-main-GraphNode:
     key : foo
    kind : bar
-<nil>-<nil>-<nil>`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	ep.err = fmt.Errorf("foo")

	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "foo",
		"kind": "bar",
	}))

	if log.String() != "foo" {
		t.Error("Expected some errors:", log.String())
		return
	}

	ep.err = nil

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

	gm.StoreEdge("main", edge)

	if res := fmt.Sprint(ep.buf.String()); res != `
db.node.created-[db.node.created]-main-GraphNode:
     key : foo
    kind : bar
-<nil>-<nil>-<nil>db.node.created-[db.node.created]-main-GraphNode:
     key : 123
    kind : mykind
    Name : Node1
-<nil>-<nil>-<nil>db.node.created-[db.node.created]-main-GraphNode:
     key : 456
    kind : mykind
    Name : Node2
-<nil>-<nil>-<nil>db.node.created-[db.node.created]-main-GraphNode:
     key : 789
    kind : mykind
    Name : Node3
-<nil>-<nil>-<nil>db.edge.created-[db.edge.created]-main-<nil>-GraphEdge:
              key : abc
             kind : myedge
    end1cascading : true
          end1key : 123
         end1kind : mykind
         end1role : node1
    end2cascading : false
          end2key : 456
         end2kind : mykind
         end2role : node2
-<nil>-<nil>`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	ep.buf.Reset()

	// Do some updates

	edge = data.NewGraphEdge()

	edge.SetAttr("key", "abc")
	edge.SetAttr("kind", "myedge")
	edge.SetAttr("foo", "bar")
	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "node1")
	edge.SetAttr(data.EdgeEnd1Cascading, true)

	edge.SetAttr(data.EdgeEnd2Key, node2.Key())
	edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "node2")
	edge.SetAttr(data.EdgeEnd2Cascading, false)

	gm.StoreEdge("main", edge)

	node1 = data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mykind")
	node1.SetAttr("Name", "Node66")

	gm.StoreNode("main", node1)

	if res := fmt.Sprint(ep.buf.String()); res != `
db.edge.updated-[db.edge.updated]-main-<nil>-GraphEdge:
              key : abc
             kind : myedge
    end1cascading : true
          end1key : 123
         end1kind : mykind
         end1role : node1
    end2cascading : false
          end2key : 456
         end2kind : mykind
         end2role : node2
              foo : bar
-<nil>-GraphEdge:
              key : abc
             kind : myedge
    end1cascading : true
          end1key : 123
         end1kind : mykind
         end1role : node1
    end2cascading : false
          end2key : 456
         end2kind : mykind
         end2role : node2
db.node.updated-[db.node.updated]-main-GraphNode:
     key : 123
    kind : mykind
    Name : Node66
-<nil>-GraphNode:
     key : 123
    kind : mykind
    Name : Node1
-<nil>`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	ep.buf.Reset()

	// Do deletions

	gm.RemoveNode("main", "456", "mykind") // This should also delete the edge

	if res := fmt.Sprint(ep.buf.String()); res != `
db.node.deleted-[db.node.deleted]-main-GraphNode:
     key : 456
    kind : mykind
    Name : Node2
-<nil>-<nil>-<nil>db.edge.deleted-[db.edge.deleted]-main-<nil>-GraphEdge:
              key : abc
             kind : myedge
    end1cascading : true
          end1key : 123
         end1kind : mykind
         end1role : node1
    end2cascading : false
          end2key : 456
         end2kind : mykind
         end2role : node2
              foo : bar
-<nil>-<nil>`[1:] {
		t.Error("Unexpected result:", res)
		return
	}
}
