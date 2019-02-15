/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package data

import (
	"fmt"
	"testing"
)

func TestNodeCompare(t *testing.T) {
	gn1 := NewGraphNode()

	gn1.SetAttr("Test1", "test")

	gn2 := NewGraphNode()

	gn1.SetAttr("test1", "test")

	if NodeCompare(gn1, gn2, []string{"Test1"}) {
		t.Error("Unexpected compare result")
		return
	}

	gn2.SetAttr("Test1", "test")

	if !NodeCompare(gn1, gn2, []string{"Test1"}) {
		t.Error("Unexpected compare result")
		return
	}

	if NodeCompare(gn1, gn2, nil) {
		t.Error("Unexpected compare result")
		return
	}

	gn1.SetAttr("test1", nil)

	if !NodeCompare(gn1, gn2, nil) {
		t.Error("Unexpected compare result")
		return
	}
}

func TestNodeClone(t *testing.T) {
	gn1 := NewGraphNode()
	gn1.SetAttr("Test1", "test")

	gn2 := NodeClone(gn1)

	if !NodeCompare(gn1, gn2, nil) {
		t.Error("Node should be a clone")
		return
	}

	gn1.SetAttr("Test1", "test2")

	if NodeCompare(gn1, gn2, nil) {
		t.Error("Node should be different now")
		return
	}
}

func TestNodeMerge(t *testing.T) {
	gn1 := NewGraphNode()
	gn1.SetAttr("Test1", "test1")

	gn2 := NewGraphNode()
	gn2.SetAttr("Test2", "test2")

	gn3 := NodeMerge(gn1, gn2)

	if gn1.Attr("Test1") != gn3.Attr("Test1") {
		t.Error("Nodes should have been merged")
		return
	}

	if gn2.Attr("Test2") != gn3.Attr("Test2") {
		t.Error("Nodes should have been merged")
		return
	}
}

func TestNodeSort(t *testing.T) {
	nodes := []Node{
		NewGraphNodeFromMap(map[string]interface{}{
			"key":  "aaa",
			"kind": "bbb",
		}),
		NewGraphNodeFromMap(map[string]interface{}{
			"key":  "ddd",
			"kind": "bbb",
		}),
		NewGraphNodeFromMap(map[string]interface{}{
			"key":  "ggg",
			"kind": "bbb",
		}),
		NewGraphNodeFromMap(map[string]interface{}{
			"key":  "aaa",
			"kind": "ccc",
		}),
		NewGraphNodeFromMap(map[string]interface{}{
			"key":  "aaa",
			"kind": "000",
		}),
	}

	NodeSort(nodes)

	if res := fmt.Sprint(nodes); res != `
[GraphNode:
     key : aaa
    kind : 000
 GraphNode:
     key : aaa
    kind : bbb
 GraphNode:
     key : ddd
    kind : bbb
 GraphNode:
     key : ggg
    kind : bbb
 GraphNode:
     key : aaa
    kind : ccc
]`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}
}
