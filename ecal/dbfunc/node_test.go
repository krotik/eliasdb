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
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func TestStoreAndRemoveNode(t *testing.T) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	sn := &StoreNodeFunc{gm}

	if _, err := sn.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := sn.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 2 or 3 parameters: partition, node map and optionally a transaction" {
		t.Error(err)
		return
	}

	if _, err := sn.Run("", nil, nil, 0, []interface{}{"", "bla"}); err == nil ||
		err.Error() != "Second parameter must be a map" {
		t.Error(err)
		return
	}

	if _, err := sn.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{}, "bla"}); err == nil ||
		err.Error() != "Third parameter must be a transaction" {
		t.Error(err)
		return
	}

	if _, err := sn.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key": "foo",
	}}); err == nil ||
		err.Error() != "GraphError: Invalid data (Node is missing a kind value)" {
		t.Error(err)
		return
	}

	if _, err := sn.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key":   "foo",
		"kind":  "bar",
		"data":  "123",
		"data2": "1234",
	}}); err != nil {
		t.Error(err)
		return
	}

	if res := gm.NodeCount("bar"); res != 1 {
		t.Error("Unexpected result:", res)
		return
	}

	un := &UpdateNodeFunc{gm}

	if _, err := un.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := un.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 2 or 3 parameters: partition, node map and optionally a transaction" {
		t.Error(err)
		return
	}

	if _, err := un.Run("", nil, nil, 0, []interface{}{"", "bla"}); err == nil ||
		err.Error() != "Second parameter must be a map" {
		t.Error(err)
		return
	}

	if _, err := un.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{}, "bla"}); err == nil ||
		err.Error() != "Third parameter must be a transaction" {
		t.Error(err)
		return
	}

	if _, err := un.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key": "foo",
	}}); err == nil ||
		err.Error() != "GraphError: Invalid data (Node is missing a kind value)" {
		t.Error(err)
		return
	}

	if _, err := un.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key":  "foo",
		"kind": "bar",
		"data": "1234",
	}}); err != nil {
		t.Error(err)
		return
	}

	if res := gm.NodeCount("bar"); res != 1 {
		t.Error("Unexpected result:", res)
		return
	}

	fn := &FetchNodeFunc{gm}

	if _, err := fn.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := fn.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 3 parameters: partition, node key node kind" {
		t.Error(err)
		return
	}

	if _, err := fn.Run("", nil, nil, 0, []interface{}{"main", "foo", "ba r"}); err == nil ||
		err.Error() != "GraphError: Invalid data (Node kind ba r is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error(err)
		return
	}

	res, err := fn.Run("", nil, nil, 0, []interface{}{"main", "foo", "bar"})

	if fmt.Sprint(NewGraphNodeFromECALMap(res.(map[interface{}]interface{}))) != `
GraphNode:
      key : foo
     kind : bar
     data : 1234
    data2 : 1234
`[1:] || err != nil {
		t.Error("Unexpected result:\n", res, err)
		return
	}

	rn := &RemoveNodeFunc{gm}

	if _, err := rn.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := rn.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 3 or 4 parameters: partition, node key node kind and optionally a transaction" {
		t.Error(err)
		return
	}

	if _, err := rn.Run("", nil, nil, 0, []interface{}{"mai n", "foo", "bar"}); err == nil ||
		err.Error() != "GraphError: Invalid data (Partition name mai n is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error(err)
		return
	}

	_, err = rn.Run("", nil, nil, 0, []interface{}{"main", "foo", "bar"})
	if err != nil {
		t.Error(err)
		return
	}

	res, err = fn.Run("", nil, nil, 0, []interface{}{"main", "foo", "bar"})
	if res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
}

func TestStoreNodeTrans(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	tn := &NewTransFunc{gm}

	if _, err := tn.DocString(); err != nil {
		t.Error(err)
		return
	}

	tn2 := &NewRollingTransFunc{gm}

	if _, err := tn2.DocString(); err != nil {
		t.Error(err)
		return
	}

	tc := &CommitTransFunc{gm}

	if _, err := tc.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := tn.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function does not require any parameters" {
		t.Error(err)
		return
	}
	if _, err := tn2.Run("", nil, nil, 0, []interface{}{"", ""}); err == nil ||
		err.Error() != "Function requires the rolling threshold (number of operations before rolling)" {
		t.Error(err)
		return
	}
	if _, err := tc.Run("", nil, nil, 0, []interface{}{"", ""}); err == nil ||
		err.Error() != "Function requires the transaction to commit as parameter" {
		t.Error(err)
		return
	}

	if _, err := tc.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Parameter must be a transaction" {
		t.Error(err)
		return
	}

	trans, err := tn.Run("", nil, nil, 0, []interface{}{})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = tn2.Run("", nil, nil, 0, []interface{}{"foo"})
	if err == nil || err.Error() != "Rolling threshold must be a number not: foo" {
		t.Error(err)
		return
	}

	_, err = tn2.Run("", nil, nil, 0, []interface{}{1})
	if err != nil {
		t.Error(err)
		return
	}

	sn := &StoreNodeFunc{gm}

	if _, err := sn.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key":  "foo1",
		"kind": "bar",
	}, trans}); err != nil {
		t.Error(err)
		return
	}

	if _, err := sn.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key":  "foo2",
		"kind": "bar",
	}, trans}); err != nil {
		t.Error(err)
		return
	}

	un := &UpdateNodeFunc{gm}

	if _, err := un.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key":  "foo3",
		"kind": "bar",
	}, trans}); err != nil {
		t.Error(err)
		return
	}

	// Check that the nodes are in the transaction

	if res := fmt.Sprint(trans.(graph.Trans).Counts()); res != "3 0 0 0" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := gm.NodeCount("bar"); res != 0 {
		t.Error("Unexpected result:", res)
		return
	}

	// Commit the nodes

	if _, err := tc.Run("", nil, nil, 0, []interface{}{"main", map[interface{}]interface{}{
		"key":  "foo3",
		"kind": "bar",
	}, trans}); err == nil || err.Error() != "Function requires the transaction to commit as parameter" {
		t.Error(err)
		return
	}

	if _, err := tc.Run("", nil, nil, 0, []interface{}{trans}); err != nil {
		t.Error(err)
		return
	}

	// Check that the nodes have been committed

	if res := fmt.Sprint(trans.(graph.Trans).Counts()); res != "0 0 0 0" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := gm.NodeCount("bar"); res != 3 {
		t.Error("Unexpected result:", res)
		return
	}

	// Remove the nodes

	rn := &RemoveNodeFunc{gm}

	_, err = rn.Run("", nil, nil, 0, []interface{}{"main", "foo1", "bar", nil})
	if err == nil || err.Error() != "Fourth parameter must be a transaction" {
		t.Error(err)
		return
	}

	_, err = rn.Run("", nil, nil, 0, []interface{}{"main", "foo1", "bar", trans})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = rn.Run("", nil, nil, 0, []interface{}{"main", "foo2", "bar", trans})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = rn.Run("", nil, nil, 0, []interface{}{"main", "foo3", "bar", trans})
	if err != nil {
		t.Error(err)
		return
	}

	// Check that the nodes are in the transaction

	if res := fmt.Sprint(trans.(graph.Trans).Counts()); res != "0 0 3 0" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := gm.NodeCount("bar"); res != 3 {
		t.Error("Unexpected result:", res)
		return
	}

	// Commit the nodes

	if _, err := tc.Run("", nil, nil, 0, []interface{}{trans}); err != nil {
		t.Error(err)
		return
	}

	// Check that the nodes have been committed

	if res := fmt.Sprint(trans.(graph.Trans).Counts()); res != "0 0 0 0" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := gm.NodeCount("bar"); res != 0 {
		t.Error("Unexpected result:", res)
		return
	}
}
