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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestGraphNode(t *testing.T) {
	gn := NewGraphNode()

	if res := gn.Key(); res != "" {
		t.Error("Unexpected key:", res)
		return
	}

	if res := gn.Name(); res != "" {
		t.Error("Unexpected name:", res)
		return
	}

	if res := gn.Kind(); res != "" {
		t.Error("Unexpected kind:", res)
		return
	}

	if res := gn.Attr("a"); res != nil {
		t.Error("Unexpected result:", res)
		return
	}

	gn.SetAttr(NodeKey, "123")

	if res := gn.Attr(NodeKey); res != "123" {
		t.Error("Unexpected key:", res)
		return
	}

	if res := gn.Key(); res != "123" {
		t.Error("Unexpected key:", res)
		return
	}

	gn.SetAttr(NodeKind, "mykind")

	if res := gn.Attr(NodeKind); res != "mykind" {
		t.Error("Unexpected kind:", res)
		return
	}

	if res := gn.Kind(); res != "mykind" {
		t.Error("Unexpected kind:", res)
		return
	}

	gn.SetAttr("myattr", 123)
	gn.SetAttr("myattr2", bytes.NewBuffer([]byte("abba")))
	gn.SetAttr("myattr3", "test123")

	if res := gn.Attr("myattr"); res != 123 {
		t.Error("Unexpected attr:", res)
		return
	}

	if res := gn.(*graphNode).stringAttr("myattr2"); res != "abba" {
		t.Error("Unexpected attr:", res)
		return
	}

	im := gn.IndexMap()
	if im["myattr"] != "123" || im["myattr2"] != "abba" ||
		im["myattr3"] != "test123" || len(im) != 3 {

		t.Error("Unexpected indexmap result:", gn.IndexMap())
		return
	}

	gn.SetAttr("myattr", nil)

	if res := gn.Attr("myattr"); res != nil {
		t.Error("Unexpected attr:", res)
		return
	}

	gn.SetAttr("amyattr", "another test")

	if res := gn.String(); res != "GraphNode:\n"+
		"        key : 123\n"+
		"       kind : mykind\n"+
		"    amyattr : another test\n"+
		"    myattr2 : abba\n"+
		"    myattr3 : test123\n" {
		t.Error("Unexpected string output:", res)
		return
	}

	nodedata := gn.Data()
	if nodedata["key"] != gn.(*graphNode).data["key"] {
		t.Error("Unexpected data reference")
		return
	}

	nnode := NewGraphNodeFromMap(gn.Data())
	if nnode.Data()["key"] != gn.(*graphNode).data["key"] {
		t.Error("Unexpected data reference")
		return
	}

	gn = CopyNode(gn)

	gn.SetAttr("key", []int{1, 2, 3})

	if res := gn.String(); res != "GraphNode:\n"+
		"        key : [1 2 3]\n"+
		"       kind : mykind\n"+
		"    amyattr : another test\n"+
		"    myattr2 : abba\n"+
		"    myattr3 : test123\n" {
		t.Error("Unexpected string output:", res)
		return
	}

	if gn.Key() != "[1 2 3]" {
		t.Error("Unexpected key as string:", gn.Key())
		return
	}

	if fmt.Sprintf("%T", gn.Attr("key")) != "[]int" {
		t.Errorf("Unexpected ckey type: %T", gn.Attr("key"))
		return
	}
}

func TestNestedNode(t *testing.T) {
	gn := NewGraphNode()

	gn.SetAttr("key", "456")
	gn.SetAttr("kind", "mynode")
	gn.SetAttr("name", "Node2")
	gn.SetAttr("type", "type2")
	gn.SetAttr("err", errors.New("bla"))
	gn.SetAttr("nested", map[string]interface{}{
		"nest1": map[string]interface{}{
			"nest2": map[string]interface{}{
				"atom1": 1.45,
			},
		},
	})

	im := gn.IndexMap()
	jsonString, err := json.MarshalIndent(im, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}

	if string(jsonString) != `
{
  "err": "bla",
  "name": "Node2",
  "nested": "{\"nest1\":{\"nest2\":{\"atom1\":1.45}}}",
  "nested.nest1": "{\"nest2\":{\"atom1\":1.45}}",
  "nested.nest1.nest2": "{\"atom1\":1.45}",
  "nested.nest1.nest2.atom1": "1.45",
  "type": "type2"
}`[1:] {
		t.Error("Unexpected result: ", string(jsonString))
		return
	}
}
