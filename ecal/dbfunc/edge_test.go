/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dbfunc

import (
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func TestStoreAndRemoveEdge(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	se := &StoreEdgeFunc{gm}

	if _, err := se.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := se.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 2 or 3 parameters: partition, edge map and optionally a transaction" {
		t.Error(err)
		return
	}

	if _, err := se.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key": "foo",
	}}); err == nil ||
		err.Error() != "GraphError: Invalid data (Edge is missing a kind value)" {
		t.Error(err)
		return
	}

	if _, err := se.Run("", nil, nil, 0, []interface{}{"main", "x"}); err == nil ||
		err.Error() != "Second parameter must be a map" {
		t.Error(err)
		return
	}

	if _, err := se.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key": "foo",
	}, "x"}); err == nil ||
		err.Error() != "Third parameter must be a transaction" {
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

	if _, err := se.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
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
	}}); err != nil {
		t.Error(err)
		return
	}

	_, err := se.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
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
	}})

	if err == nil || err.Error() != "GraphError: Invalid data (Can't store edge to non-existing node kind: b1)" {
		t.Error(err)
		return
	}

	fe := &FetchEdgeFunc{gm}

	if _, err := fe.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := fe.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 3 parameters: partition, edge key and edge kind" {
		t.Error(err)
		return
	}

	if _, err := fe.Run("", nil, nil, 0, []interface{}{"mai n", "123", "e"}); err == nil ||
		err.Error() != "GraphError: Invalid data (Partition name mai n is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error(err)
		return
	}

	res, err := fe.Run("", nil, nil, 0, []interface{}{"main", "123", "e"})

	if fmt.Sprint(data.NewGraphEdgeFromNode(NewGraphNodeFromECALMap(res.(map[interface{}]interface{})))) != `
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
		t.Error("Unexpected result:", fmt.Sprint(data.NewGraphEdgeFromNode(NewGraphNodeFromECALMap(res.(map[interface{}]interface{})))), err)
		return
	}

	tr := &TraverseFunc{gm}

	if _, err := tr.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := tr.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 4 parameters: partition, node key, node kind and a traversal spec" {
		t.Error(err)
		return
	}

	if _, err := tr.Run("", nil, nil, 0, []interface{}{"main", "c", "d", "::"}); err == nil ||
		err.Error() != "GraphError: Invalid data (Invalid spec: ::)" {
		t.Error(err)
		return
	}

	res, err = tr.Run("", nil, nil, 0, []interface{}{"main", "c", "d", ":::"})
	if err != nil {
		t.Error(err)
		return
	}

	if fmt.Sprint(data.NewGraphEdgeFromNode(NewGraphNodeFromECALMap(res.([]interface{})[1].([]interface{})[0].(map[interface{}]interface{})))) != `
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
		t.Error("Unexpected result:", fmt.Sprint(data.NewGraphEdgeFromNode(NewGraphNodeFromECALMap(res.([]interface{})[1].([]interface{})[0].(map[interface{}]interface{})))), err)
		return
	}

	if fmt.Sprint(NewGraphNodeFromECALMap(res.([]interface{})[0].([]interface{})[0].(map[interface{}]interface{}))) != `
GraphNode:
     key : a
    kind : b
`[1:] || err != nil {
		t.Error("Unexpected result:", fmt.Sprint(NewGraphNodeFromECALMap(res.([]interface{})[0].([]interface{})[0].(map[interface{}]interface{}))), err)
		return
	}

	re := &RemoveEdgeFunc{gm}

	if _, err := re.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := re.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 3 or 4 parameters: partition, edge key, edge kind and optionally a transaction" {
		t.Error(err)
		return
	}

	if _, err := re.Run("", nil, nil, 0, []interface{}{"mai n", "123", "e"}); err == nil ||
		err.Error() != "GraphError: Invalid data (Partition name mai n is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error(err)
		return
	}

	if _, err := re.Run("", nil, nil, 0, []interface{}{"mai n", "123", "e", "bla"}); err == nil ||
		err.Error() != "Fourth parameter must be a transaction" {
		t.Error(err)
		return
	}

	if _, err := re.Run("", nil, nil, 0, []interface{}{"main", "123", "e"}); err != nil {
		t.Error(err)
		return
	}

	res, err = fe.Run("", nil, nil, 0, []interface{}{"main", "123", "e"})

	if res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
}

func TestStoreEdgeTrans(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	sn := &StoreNodeFunc{gm}
	se := &StoreEdgeFunc{gm}
	tc := &CommitTransFunc{gm}

	if _, err := tc.DocString(); err != nil {
		t.Error(err)
		return
	}

	trans := graph.NewGraphTrans(gm)

	if _, err := sn.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key":  "a",
		"kind": "b",
	}, trans}); err != nil {
		t.Error(err)
		return
	}

	if _, err := sn.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key":  "c",
		"kind": "d",
	}, trans}); err != nil {
		t.Error(err)
		return
	}

	_, err := se.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
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
	}, trans})

	if err != nil {
		t.Error(err)
		return
	}

	if res := fmt.Sprint(trans.Counts()); res != "2 1 0 0" {
		t.Error("Unexpected result:", res)
		return
	}
	if _, err := tc.Run("", nil, nil, 0, []interface{}{trans}); err != nil {
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

	se.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
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
	}, trans})

	if _, err := tc.Run("", nil, nil, 0, []interface{}{trans}); err == nil || err.Error() !=
		"GraphError: Invalid data (Can't find edge endpoint: c1 (d))" {
		t.Error(err)
		return
	}

	re := &RemoveEdgeFunc{}

	if _, err := re.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := re.Run("", nil, nil, 0, []interface{}{"main", "123", "e", trans}); err != nil {
		t.Error(err)
		return
	}

	if res := fmt.Sprint(trans.Counts()); res != "0 0 0 1" {
		t.Error("Unexpected result:", res)
		return
	}

	if _, err := tc.Run("", nil, nil, 0, []interface{}{trans}); err != nil {
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
