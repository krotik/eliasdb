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
	"encoding/json"
	"fmt"
	"testing"

	"devt.de/krotik/common/lang/graphql/parser"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func TestNamedQueries(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": "bar",
		"query": `
query foo {
  Song(key : "StrangeSong1") {
	name
  }
}
mutation bar {
  Song(key : "StrangeSong1"){
	key
  }
}
subscription foobar {
  Song(key : "StrangeSong1"){
	key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query["operationName"] = "foobar"

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "StrangeSong1",
        "name": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}
}

func TestInvalidRuntime(t *testing.T) {
	ast, _ := parser.ParseWithRuntime("", fmt.Sprint("{name}"), nil)
	rtp := NewGraphQLRuntimeProvider("test", "", nil, "", nil, nil, true)

	rt := &invalidRuntime{rtp, ast}

	if err := rt.Validate(); err.Error() != "Fatal GraphQL operation error in test: Invalid construct (Document) (Line:1 Pos:1)" {
		t.Error("Unexpected result:", err)
		return
	}

	if _, err := rt.Eval(); err.Error() != "Fatal GraphQL operation error in test: Invalid construct (Document) (Line:1 Pos:1)" {
		t.Error("Unexpected result:", err)
		return
	}
}

func TestDirectives(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
query ($foo : String = "bar") {
  Song(key : $foo) {
    song_key : key
    ...kindField @include(if : false)
	foo : bar(traverse : ":::") @skip(if : true) {
		key
		kind
		Name : name
	}
  }
}
fragment kindField on Song {
	kind
}
`,
		"variables": map[string]interface{}{
			"foo": "StrangeSong1",
		},
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "song_key": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
query ($foo : String = "bar") {
  Song(key : $foo) {
	foo : bar(traverse : ":::") @skip() {
		key
	}
  }
}
`,
		"variables": map[string]interface{}{
			"foo": "StrangeSong1",
		},
	}

	if rerr := checkResult(`{
  "data": {
    "Song": [
      {
        "foo": [
          {
            "key": "123"
          },
          {
            "key": "Best"
          }
        ]
      }
    ]
  },
  "errors": [
    {
      "locations": [
        {
          "column": 31,
          "line": 4
        }
      ],
      "message": "Directive skip is missing the 'if' argument",
      "path": [
        "Song"
      ]
    }
  ]
}`, query, gm); rerr != nil {
		t.Error(rerr)
		return
	}
}

func TestVariables(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
query ($foo : String = "bar", $traverse : String = ":::") {
  Song(key : $foo) {
    song_key : key
	foo : bar(traverse : $traverse, x : $y) {
		key
		kind
		Name : name
	}
  }
}
`,
		"variables": map[string]interface{}{
			"foo": "StrangeSong1",
		},
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "foo": [
          {
            "Name": "Mike",
            "key": "123",
            "kind": "Author"
          },
          {
            "Name": null,
            "key": "Best",
            "kind": "group"
          }
        ],
        "song_key": "StrangeSong1"
      }
    ]
  },
  "errors": [
    {
      "locations": [
        {
          "column": 40,
          "line": 5
        }
      ],
      "message": "Variable y was used but not declared",
      "path": []
    },
    {
      "locations": [
        {
          "column": 43,
          "line": 5
        }
      ],
      "message": "Unknown argument: x",
      "path": [
        "Song",
        ":::"
      ]
    }
  ]
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}
}

func TestRuntimeObjects(t *testing.T) {
	rtp := NewGraphQLRuntimeProvider("test", "", nil, "", nil, nil, true)

	ast, err := parser.ParseWithRuntime("test", `{
	     Song(matches : { name : {
	       IntValue : 1,
	       FloatValue : 2.2,
	       StringValue : "abc",
	       BooleanValue : true,
	       NullValue : null,
	       EnumValue : TEST,
	       ListValueConst : [1,2,3],
	       ObjectValueConst : { foo : "bar", foo1 : "bar1",  },
	   }}) {
	       song_key : key
	     }
	   }`, rtp)

	objectValue := ast.Children[0].Children[0].Children[0].Children[0].Children[1].Children[0].Children[1]
	rt := objectValue.Runtime.(*valueRuntime)
	actualResultBytes, _ := json.MarshalIndent(rt.Value(), "", "  ")
	actualResult := string(actualResultBytes)

	if err != nil || actualResult != `{
  "name": {
    "BooleanValue": true,
    "EnumValue": "TEST",
    "FloatValue": 2.2,
    "IntValue": 1,
    "ListValueConst": [
      1,
      2,
      3
    ],
    "NullValue": null,
    "ObjectValueConst": {
      "foo": "bar",
      "foo1": "bar1"
    },
    "StringValue": "abc"
  }
}` {
		t.Error("Unexpected result:", actualResult, err)
		return
	}
}

func runQuery(name string, part string, query map[string]interface{},
	gm *graph.Manager, callbackHandler SubscriptionCallbackHandler,
	readOnly bool) (map[string]interface{}, error) {

	var ok bool
	var vars map[string]interface{}

	// Nil pointer become empty strings

	if query["operationName"] == nil {
		query["operationName"] = ""
	}
	if query["query"] == nil {
		query["query"] = ""
	}

	if vars, ok = query["variables"].(map[string]interface{}); !ok {
		vars = make(map[string]interface{})
	}

	// Create runtime provider

	rtp := NewGraphQLRuntimeProvider(name, part, gm,
		fmt.Sprint(query["operationName"]), vars, callbackHandler, readOnly)

	// Parse the query and annotate the AST with runtime components

	ast, err := parser.ParseWithRuntime(name, fmt.Sprint(query["query"]), rtp)

	if err == nil {

		// Purposefully skipping Validate() here to ensure it is called by Eval()

		// Evaluate the query

		return ast.Runtime.Eval()
	}

	return nil, err
}

func formatData(data interface{}) string {
	actualResultBytes, _ := json.MarshalIndent(data, "", "  ")
	return string(actualResultBytes)
}

func checkResult(expectedResult string, query map[string]interface{}, gm *graph.Manager) error {
	actualResultObject, err := runQuery("test", "main", query, gm, nil, false)

	if err == nil {
		var actualResultBytes []byte
		actualResultBytes, err = json.MarshalIndent(actualResultObject, "", "  ")
		actualResult := string(actualResultBytes)

		if err == nil {
			if expectedResult != actualResult {
				err = fmt.Errorf("Unexpected result:\nExpected:\n%s\nActual:\n%s",
					expectedResult, actualResult)
			}
		}

	}

	if expectedResult != "" && err != nil {
		ast, _ := parser.ParseWithRuntime("test", fmt.Sprint(query["query"]), nil)
		fmt.Println(ast)
	}

	return err
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
	node0.SetAttr("kind", "group")
	node0.SetAttr("owner", "noowner")
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
