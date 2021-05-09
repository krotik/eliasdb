/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package ecal

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/eliasdb/config"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

const testScriptDir = "testscripts"

func TestMain(m *testing.M) {
	flag.Parse()

	defer func() {
		if res, _ := fileutil.PathExists(testScriptDir); res {
			if err := os.RemoveAll(testScriptDir); err != nil {
				fmt.Print("Could not remove test directory:", err.Error())
			}
		}
	}()

	if res, _ := fileutil.PathExists(testScriptDir); res {
		if err := os.RemoveAll(testScriptDir); err != nil {
			fmt.Print("Could not remove test directory:", err.Error())
		}
	}

	ensurePath(testScriptDir)

	data := make(map[string]interface{})
	for k, v := range config.DefaultConfig {
		data[k] = v
	}

	config.Config = data

	config.Config[config.EnableECALScripts] = true
	config.Config[config.ECALScriptFolder] = testScriptDir
	config.Config[config.ECALLogFile] = filepath.Join(testScriptDir, "interpreter.log")

	// Run the tests

	m.Run()
}

/*
ensurePath ensures that a given relative path exists.
*/
func ensurePath(path string) {
	if res, _ := fileutil.PathExists(path); !res {
		if err := os.Mkdir(path, 0770); err != nil {
			fmt.Print("Could not create directory:", err.Error())
			return
		}
	}
}

func writeScript(content string) {
	filename := filepath.Join(testScriptDir, config.Str(config.ECALEntryScript))
	err := ioutil.WriteFile(
		filename,
		[]byte(content), 0600)
	errorutil.AssertOk(err)
	os.Remove(config.Str(config.ECALLogFile))
}

func checkLog(expected string) error {
	var err error

	content, err := ioutil.ReadFile(config.Str(config.ECALLogFile))
	errorutil.AssertOk(err)

	logtext := string(content)

	if logtext != expected {
		err = fmt.Errorf("Unexpected log text:\n%v", logtext)
	}

	return err
}

func TestDebugInterpreter(t *testing.T) {

	config.Config[config.EnableECALDebugServer] = true
	defer func() {
		config.Config[config.EnableECALDebugServer] = false
		errorutil.AssertOk(os.Remove(config.Str(config.ECALLogFile)))

	}()

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	ds := NewScriptingInterpreter(testScriptDir, gm)

	filename := filepath.Join(testScriptDir, config.Str(config.ECALEntryScript))
	os.Remove(filename)

	if err := ds.Run(); err != nil {
		t.Error("Unexpected result:", err)
		return
	}
}

func TestInterpreter(t *testing.T) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	ds := NewScriptingInterpreter(testScriptDir, gm)

	// Test normal log output

	writeScript(`
log("test insert")
`)

	if err := ds.Run(); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if err := checkLog(`test insert
`); err != nil {
		t.Error(err)
	}

	// Test stack trace

	writeScript(`
raise("some error")
`)

	if err := ds.Run(); err == nil || err.Error() != `ECAL error in eliasdb-runtime (testscripts/main.ecal): some error () (Line:2 Pos:1)
  raise("some error") (testscripts/main.ecal:2)` {
		t.Error("Unexpected result:", err)
		return
	}

	// Test db functions

	writeScript(`
db.storeNode("main", {
  "key" : "foo",
  "kind" : "bar",
  "data" : 123,
})

db.storeNode("main", {
  "key" : "key2",
  "kind" : "kind2",
  "data" : 456,
})

db.storeEdge("main", {
  "key":           "123",
  "kind":          "myedges",
  "end1cascading": true,
  "end1key":       "foo",
  "end1kind":      "bar",
  "end1role":      "role1",
  "end2cascading": false,
  "end2key":       "key2",
  "end2kind":      "kind2",
  "end2role":      "role2",
})

[n, e] := db.traverse("main", "key2", "kind2", "role2:myedges:role1:bar")

log("nodes: ", n, " edges: ", e)
`)

	// The store statements should trigger the triggerCheck shortcut in the eventbridge
	// because no rules are defined to handle the events.

	if err := ds.Run(); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if err := checkLog(`nodes: [
  {
    "data": 123,
    "key": "foo",
    "kind": "bar"
  }
] edges: [
  {
    "end1cascading": false,
    "end1key": "key2",
    "end1kind": "kind2",
    "end1role": "role2",
    "end2cascading": true,
    "end2key": "foo",
    "end2kind": "bar",
    "end2role": "role1",
    "key": "123",
    "kind": "myedges"
  }
]
`); err != nil {
		t.Error(err)
	}
}

func TestEvents(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	ds := NewScriptingInterpreter(testScriptDir, gm)

	writeScript(`
sink mysink
  kindmatch [ "db.*.*" ],
{
  log("Got event: ", event)
  if event.state["node"] != NULL {
    if event.state.node.key == "foo2" {
      raise("Oh no")
    }
    if event.state.node.key == "foo3" {
      db.raiseGraphEventHandled()
    }
  } elif event.state["edge"] != NULL {
    if event.state.edge.key == "foo2" {
      raise("Oh no edge")
    }
    if event.state.edge.key == "foo3" and event.kind == "db.edge.created" {
      raise("Oh no edge2")
    }
    if event.state.edge.key == "foo3" and event.kind == "db.edge.updated" {
      raise("Oh no edge3")
    }
  } else {
    if event.state.key == "foo3" {
      db.raiseGraphEventHandled()
    }
  }
}
`)

	if err := ds.Run(); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	err := gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "foo",
		"kind": "bar",
		"data": 123,
	}))
	errorutil.AssertOk(err)

	if err := checkLog(`Got event: {
  "kind": "db.node.store",
  "name": "EliasDB: db.node.store",
  "state": {
    "node": {
      "data": 123,
      "key": "foo",
      "kind": "bar"
    },
    "part": "main",
    "trans": {}
  }
}
Got event: {
  "kind": "db.node.created",
  "name": "EliasDB: db.node.created",
  "state": {
    "node": {
      "data": 123,
      "key": "foo",
      "kind": "bar"
    },
    "part": "main",
    "trans": {}
  }
}
`); err != nil {
		t.Error(err)
	}

	// Test raising an error before node storage

	err = gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "foo2",
		"kind": "bar",
		"data": 123,
	}))

	if err == nil || err.Error() != `GraphError: Graph rule error (Taskerror:
EliasDB: db.node.store -> mysink : ECAL error in eliasdb-runtime (testscripts/main.ecal): Oh no () (Line:8 Pos:7))` {
		t.Error("Unexpected result:", err)
		return
	}

	if res, err := gm.FetchNode("main", "foo2", "bar"); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	err = gm.UpdateNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "foo",
		"kind": "bar",
		"data": 1234,
	}))

	if err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	err = gm.StoreEdge("main", data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(map[string]interface{}{
		"key":           "foo2",
		"kind":          "e",
		"end1cascading": true,
		"end1key":       "a",
		"end1kind":      "b",
		"end1role":      "role1",
		"end2cascading": false,
		"end2key":       "c",
		"end2kind":      "d",
		"end2role":      "role2",
	})))

	if err == nil || err.Error() != `GraphError: Graph rule error (Taskerror:
EliasDB: db.edge.store -> mysink : ECAL error in eliasdb-runtime (testscripts/main.ecal): Oh no edge () (Line:15 Pos:7))` {
		t.Error("Unexpected result:", err)
		return
	}

	err = gm.StoreEdge("main", data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(map[string]interface{}{
		"key":           "foo3",
		"kind":          "e",
		"end1cascading": true,
		"end1key":       "foo",
		"end1kind":      "bar",
		"end1role":      "role1",
		"end2cascading": false,
		"end2key":       "foo",
		"end2kind":      "bar",
		"end2role":      "role2",
	})))

	if err == nil || err.Error() != `GraphError: Graph rule error (Taskerror:
EliasDB: db.edge.created -> mysink : ECAL error in eliasdb-runtime (testscripts/main.ecal): Oh no edge2 () (Line:18 Pos:7))` {
		t.Error("Unexpected result:", err)
		return
	}

	err = gm.StoreEdge("main", data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(map[string]interface{}{
		"key":           "foo3",
		"kind":          "e",
		"end1cascading": true,
		"end1key":       "foo",
		"end1kind":      "bar",
		"end1role":      "role1",
		"end2cascading": false,
		"end2key":       "foo",
		"end2kind":      "bar",
		"end2role":      "role2",
	})))

	if err == nil || err.Error() != `GraphError: Graph rule error (Taskerror:
EliasDB: db.edge.updated -> mysink : ECAL error in eliasdb-runtime (testscripts/main.ecal): Oh no edge3 () (Line:21 Pos:7))` {
		t.Error("Unexpected result:", err)
		return
	}

	// Test preventing node storage without raising an error

	err = gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "foo3",
		"kind": "bar",
		"data": 123,
	}))

	if err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if res, err := gm.FetchNode("main", "foo2", "bar"); res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	err = gm.UpdateNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "foo3",
		"kind": "bar",
		"data": 123,
	}))

	if err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	_, err = gm.RemoveNode("main", "foo3", "bar")

	if err != nil {
		t.Error("Unexpected result:", err)
		return
	}
}
