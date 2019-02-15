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

	ge.SetAttr(NodeKey, "123")
	ge.SetAttr(NodeKind, "myedgekind")

	ge.SetAttr(EdgeEnd1Key, "456")
	ge.SetAttr(EdgeEnd1Kind, "mynodekind1")
	ge.SetAttr(EdgeEnd1Role, "role1")
	ge.SetAttr(EdgeEnd1Cascading, true)
	ge.SetAttr(EdgeEnd1CascadingLast, true)

	ge.SetAttr(EdgeEnd2Key, "789")
	ge.SetAttr(EdgeEnd2Kind, "mynodekind2")
	ge.SetAttr(EdgeEnd2Role, "role2")
	ge.SetAttr(EdgeEnd2Cascading, false)
	ge.SetAttr(EdgeEnd2CascadingLast, false)

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
	if ge.End1IsCascadingLast() != true {
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
	if ge.End2IsCascadingLast() != false {
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

	if res := fmt.Sprint(ge.IndexMap()); res != "map[name:test]" {
		t.Error("Unexpected result:", res)
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

	if newEdge.String() != `GraphEdge:
                  key : 123
                 kind : myedgekind
        end1cascading : true
    end1cascadinglast : true
              end1key : 456
             end1kind : mynodekind1
             end1role : role1
        end2cascading : false
    end2cascadinglast : false
              end2key : 789
             end2kind : mynodekind2
             end2role : role2
                 name : test
` {
		t.Error("Unexpected edge string output:", newEdge)
		return
	}
}
