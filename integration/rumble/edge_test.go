/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package rumble

import (
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func TestStoreAndRemoveEdge(t *testing.T) {

	mr := &mockRuntime{}
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	api.GM = gm

	se := &StoreEdgeFunc{}

	if se.Name() != "db.storeEdge" {
		t.Error("Unexpected result:", se.Name())
		return
	}

	if err := se.Validate(2, mr); err != nil {
		t.Error(err)
		return
	}

	if err := se.Validate(3, mr); err != nil {
		t.Error(err)
		return
	}

	if err := se.Validate(1, mr); err == nil || err.Error() != "Invalid construct Function storeEdge requires 2 or 3 parameters: partition, edge map and optionally a transaction" {
		t.Error(err)
		return
	}

	if _, err := se.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key": "foo",
	}}, nil, mr); err == nil || err.Error() != "Invalid state Cannot store edge: GraphError: Invalid data (Edge is missing a kind value)" {
		t.Error(err)
		return
	}

	if _, err := se.Execute([]interface{}{"main", "x"}, nil, mr); err == nil || err.Error() != "Operand is not a map Second parameter must be a map" {
		t.Error(err)
		return
	}

	if _, err := se.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key": "foo",
	}, "x"}, nil, mr); err == nil || err.Error() != "Invalid construct Third parameter must be a transaction" {
		t.Error(err)
		return
	}

	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "a",
		"kind": "b",
	}))
	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "c",
		"kind": "d",
	}))

	_, err := se.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":           "123",
		"kind":          "e",
		"end1cascading": true,
		"end1key":       "a",
		"end1kind":      "b",
		"end1role":      "role1",
		"end2cascading": false,
		"end2key":       "c",
		"end2kind":      "d",
		"end2role":      "role2",
	}}, nil, mr)

	if err != nil {
		t.Error(err)
		return
	}

	_, err = se.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":           "123",
		"kind":          "e",
		"end1cascading": true,
		"end1key":       "a",
		"end1kind":      "b1",
		"end1role":      "role1",
		"end2cascading": false,
		"end2key":       "c",
		"end2kind":      "d",
		"end2role":      "role2",
	}}, nil, mr)

	if err == nil || err.Error() != "Invalid state Cannot store edge: GraphError: Invalid data (Can't store edge to non-existing node kind: b1)" {
		t.Error(err)
		return
	}

	fe := &FetchEdgeFunc{}

	if fe.Name() != "db.fetchEdge" {
		t.Error("Unexpected result:", fe.Name())
		return
	}

	if err := fe.Validate(3, mr); err != nil {
		t.Error(err)
		return
	}

	if err := fe.Validate(1, mr); err == nil || err.Error() != "Invalid construct Function fetchEdge requires 3 parameters: partition, edge key and edge kind" {
		t.Error(err)
		return
	}

	if _, err := fe.Execute([]interface{}{"mai n", "123", "e"}, nil, mr); err == nil || err.Error() !=
		"Invalid state Cannot fetch edge: GraphError: Invalid data (Partition name mai n is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error(err)
		return
	}

	res, err := fe.Execute([]interface{}{"main", "123", "e"}, nil, mr)
	if fmt.Sprint(data.NewGraphEdgeFromNode(NewGraphNodeFromRumbleMap(res.(map[interface{}]interface{})))) != `
GraphEdge:
              key : 123
             kind : e
    end1cascading : true
          end1key : a
         end1kind : b
         end1role : role1
    end2cascading : false
          end2key : c
         end2kind : d
         end2role : role2
`[1:] || err != nil {
		t.Error("Unexpected result:", fmt.Sprint(data.NewGraphEdgeFromNode(NewGraphNodeFromRumbleMap(res.(map[interface{}]interface{})))), err)
		return
	}

	tr := &TraverseFunc{}

	if tr.Name() != "db.traverse" {
		t.Error("Unexpected result:", tr.Name())
		return
	}

	if err := tr.Validate(4, mr); err != nil {
		t.Error(err)
		return
	}

	if err := tr.Validate(1, mr); err == nil || err.Error() != "Invalid construct Function traverse requires 4 parameters: partition, node key, node kind and a traversal spec" {
		t.Error(err)
		return
	}

	_, err = tr.Execute([]interface{}{"main", "c", "d", "::"}, nil, mr)
	if err == nil || err.Error() != "Invalid state Cannot traverse: GraphError: Invalid data (Invalid spec: ::)" {
		t.Error(err)
		return
	}

	res, err = tr.Execute([]interface{}{"main", "c", "d", ":::"}, nil, mr)
	if err != nil {
		t.Error(err)
		return
	}

	if fmt.Sprint(data.NewGraphEdgeFromNode(NewGraphNodeFromRumbleMap(res.([]interface{})[1].([]interface{})[0].(map[interface{}]interface{})))) != `
GraphEdge:
              key : 123
             kind : e
    end1cascading : false
          end1key : c
         end1kind : d
         end1role : role2
    end2cascading : true
          end2key : a
         end2kind : b
         end2role : role1
`[1:] || err != nil {
		t.Error("Unexpected result:", fmt.Sprint(data.NewGraphEdgeFromNode(NewGraphNodeFromRumbleMap(res.([]interface{})[1].([]interface{})[0].(map[interface{}]interface{})))), err)
		return
	}

	if fmt.Sprint(NewGraphNodeFromRumbleMap(res.([]interface{})[0].([]interface{})[0].(map[interface{}]interface{}))) != `
GraphNode:
     key : a
    kind : b
`[1:] || err != nil {
		t.Error("Unexpected result:", fmt.Sprint(NewGraphNodeFromRumbleMap(res.([]interface{})[0].([]interface{})[0].(map[interface{}]interface{}))), err)
		return
	}

	re := &RemoveEdgeFunc{}

	if re.Name() != "db.removeEdge" {
		t.Error("Unexpected result:", re.Name())
		return
	}

	if err := re.Validate(3, mr); err != nil {
		t.Error(err)
		return
	}

	if err := re.Validate(1, mr); err == nil || err.Error() !=
		"Invalid construct Function removeEdge requires 3 or 4 parameters: partition, edge key, edge kind and optionally a transaction" {
		t.Error(err)
		return
	}

	if _, err := re.Execute([]interface{}{"mai n", "123", "e"}, nil, mr); err == nil || err.Error() !=
		"Invalid state Cannot remove edge: GraphError: Invalid data (Partition name mai n is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error(err)
		return
	}

	if _, err := re.Execute([]interface{}{"mai n", "123", "e", "bla"}, nil, mr); err == nil || err.Error() !=
		"Invalid construct Fourth parameter must be a transaction" {
		t.Error(err)
		return
	}

	if _, err := re.Execute([]interface{}{"main", "123", "e"}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	res, err = fe.Execute([]interface{}{"main", "123", "e"}, nil, mr)
	if res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
}

func TestStoreEdgeTrans(t *testing.T) {

	mr := &mockRuntime{}
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	api.GM = gm

	sn := &StoreNodeFunc{}
	se := &StoreEdgeFunc{}
	tc := &CommitTransFunc{}

	trans := graph.NewGraphTrans(gm)

	if _, err := sn.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":  "a",
		"kind": "b",
	}, trans}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":  "c",
		"kind": "d",
	}, trans}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	_, err := se.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":           "123",
		"kind":          "e",
		"end1cascading": true,
		"end1key":       "a",
		"end1kind":      "b",
		"end1role":      "role1",
		"end2cascading": false,
		"end2key":       "c",
		"end2kind":      "d",
		"end2role":      "role2",
	}, trans}, nil, mr)

	if err != nil {
		t.Error(err)
		return
	}

	if res := fmt.Sprint(trans.Counts()); res != "2 1 0 0" {
		t.Error("Unexpected result:", res)
		return
	}
	if _, err := tc.Execute([]interface{}{trans}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	// Check that the nodes have been committed

	if res := fmt.Sprint(trans.Counts()); res != "0 0 0 0" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := gm.EdgeCount("e"); res != 1 {
		t.Error("Unexpected result:", res)
		return
	}

	se.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":           "123",
		"kind":          "e",
		"end1cascading": true,
		"end1key":       "a",
		"end1kind":      "b",
		"end1role":      "role1",
		"end2cascading": false,
		"end2key":       "c1",
		"end2kind":      "d",
		"end2role":      "role2",
	}, trans}, nil, mr)

	if _, err := tc.Execute([]interface{}{trans}, nil, mr); err == nil || err.Error() !=
		"Invalid construct Cannot store node: GraphError: Invalid data (Can't find edge endpoint: c1 (d))" {
		t.Error(err)
		return
	}

	re := &RemoveEdgeFunc{}

	if _, err := re.Execute([]interface{}{"main", "123", "e", trans}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	if res := fmt.Sprint(trans.Counts()); res != "0 0 0 1" {
		t.Error("Unexpected result:", res)
		return
	}

	if _, err := tc.Execute([]interface{}{trans}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	if res := fmt.Sprint(trans.Counts()); res != "0 0 0 0" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := gm.EdgeCount("e"); res != 0 {
		t.Error("Unexpected result:", res)
		return
	}
}
