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

func TestGraphEdge(t *testing.T) {
	ge := NewGraphEdge()

	ge.SetAttr(NODE_KEY, "123")
	ge.SetAttr(NODE_KIND, "myedgekind")

	ge.SetAttr(EDGE_END1_KEY, "456")
	ge.SetAttr(EDGE_END1_KIND, "mynodekind1")
	ge.SetAttr(EDGE_END1_ROLE, "role1")
	ge.SetAttr(EDGE_END1_CASCADING, true)

	ge.SetAttr(EDGE_END2_KEY, "789")
	ge.SetAttr(EDGE_END2_KIND, "mynodekind2")
	ge.SetAttr(EDGE_END2_ROLE, "role2")
	ge.SetAttr(EDGE_END2_CASCADING, false)

	ge.SetAttr("name", "test")

	if ge.End1Key() != "456" {
		t.Error("Unexpected result")
		return
	}
	if ge.End1Kind() != "mynodekind1" {
		t.Error("Unexpected result")
		return
	}
	if ge.End1Role() != "role1" {
		t.Error("Unexpected result")
		return
	}
	if ge.End1IsCascading() != true {
		t.Error("Unexpected result")
		return
	}

	if ge.End2Key() != "789" {
		t.Error("Unexpected result")
		return
	}
	if ge.End2Kind() != "mynodekind2" {
		t.Error("Unexpected result")
		return
	}
	if ge.End2Role() != "role2" {
		t.Error("Unexpected result")
		return
	}
	if ge.End2IsCascading() != false {
		t.Error("Unexpected result")
		return
	}

	if ge.Spec("123") != "" {
		t.Error("Unexpected result")
		return
	}
	if ge.Spec("456") != "role1:myedgekind:role2:mynodekind2" {
		t.Error("Unexpected result")
		return
	}
	if ge.Spec("789") != "role2:myedgekind:role1:mynodekind1" {
		t.Error("Unexpected result")
		return
	}

	if ge.OtherEndKey("123") != "" {
		t.Error("Unexpected result")
		return
	}
	if ge.OtherEndKey("456") != "789" {
		t.Error("Unexpected result")
		return
	}
	if ge.OtherEndKey("789") != "456" {
		t.Error("Unexpected result")
		return
	}

	if ge.OtherEndKind("123") != "" {
		t.Error("Unexpected result")
		return
	}
	if ge.OtherEndKind("456") != "mynodekind2" {
		t.Error("Unexpected result")
		return
	}
	if ge.OtherEndKind("789") != "mynodekind1" {
		t.Error("Unexpected result")
		return
	}

	if fmt.Sprint(ge.IndexMap()) != "map[name:test]" {
		t.Error("Unexpected result")
		return
	}

	gn := NewGraphNode()
	gn.(*graphNode).data = ge.Data()

	newEdge := NewGraphEdgeFromNode(nil)
	if newEdge != nil {
		t.Error("Unexpected result")
		return
	}

	newEdge = NewGraphEdgeFromNode(gn)
	if !NodeCompare(ge, newEdge, nil) {
		t.Error("Unexpected result")
		return
	}

	if newEdge.String() != "GraphEdge:\n"+
		"              key : 123\n"+
		"             kind : myedgekind\n"+
		"    end1cascading : true\n"+
		"          end1key : 456\n"+
		"         end1kind : mynodekind1\n"+
		"         end1role : role1\n"+
		"    end2cascading : false\n"+
		"          end2key : 789\n"+
		"         end2kind : mynodekind2\n"+
		"         end2role : role2\n"+
		"             name : test\n" {
		t.Error("Unexpected edge string output:", newEdge)
		return
	}
}
