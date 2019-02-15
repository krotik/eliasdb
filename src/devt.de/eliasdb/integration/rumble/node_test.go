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

	"devt.de/common/defs/rumble"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/graphstorage"
)

type mockRuntime struct {
}

func (mr *mockRuntime) NewRuntimeError(t error, d string) rumble.RuntimeError {
	return fmt.Errorf("%v %v", t, d)
}

func TestStoreAndRemoveNode(t *testing.T) {

	mr := &mockRuntime{}
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	api.GM = gm

	sn := &StoreNodeFunc{}

	if sn.Name() != "db.storeNode" {
		t.Error("Unexpected result:", sn.Name())
		return
	}

	if err := sn.Validate(2, mr); err != nil {
		t.Error(err)
		return
	}

	if err := sn.Validate(3, mr); err != nil {
		t.Error(err)
		return
	}

	if err := sn.Validate(1, mr); err == nil || err.Error() != "Invalid construct Function storeNode requires 2 or 3 parameters: partition, node map and optionally a transaction" {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", "bla"}, nil, mr); err == nil || err.Error() != "Operand is not a map Second parameter must be a map" {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", map[interface{}]interface{}{}, "bla"}, nil, mr); err == nil || err.Error() != "Invalid construct Third parameter must be a transaction" {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", "bla"}, nil, mr); err == nil || err.Error() != "Operand is not a map Second parameter must be a map" {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key": "foo",
	}}, nil, mr); err == nil || err.Error() != "Invalid state Cannot store node: GraphError: Invalid data (Node is missing a kind value)" {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":  "foo",
		"kind": "bar",
	}}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	if res := gm.NodeCount("bar"); res != 1 {
		t.Error("Unexpected result:", res)
		return
	}

	fn := &FetchNodeFunc{}

	if fn.Name() != "db.fetchNode" {
		t.Error("Unexpected result:", fn.Name())
		return
	}

	if err := fn.Validate(3, mr); err != nil {
		t.Error(err)
		return
	}

	if err := fn.Validate(1, mr); err == nil || err.Error() != "Invalid construct Function fetchNode requires 3 parameters: partition, node key node kind" {
		t.Error(err)
		return
	}

	_, err := fn.Execute([]interface{}{"main", "foo", "ba r"}, nil, mr)
	if err == nil || err.Error() !=
		"Invalid state Cannot fetch node: GraphError: Invalid data (Node kind ba r is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error(err)
		return
	}

	res, err := fn.Execute([]interface{}{"main", "foo", "bar"}, nil, mr)
	if fmt.Sprint(NewGraphNodeFromRumbleMap(res.(map[interface{}]interface{}))) != `
GraphNode:
     key : foo
    kind : bar
`[1:] || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	rn := &RemoveNodeFunc{}

	if err := rn.Validate(1, mr); err == nil || err.Error() !=
		"Invalid construct Function removeNode requires 3 or 4 parameters: partition, node key node kind and optionally a transaction" {
		t.Error(err)
		return
	}

	_, err = rn.Execute([]interface{}{"mai n", "foo", "bar"}, nil, mr)
	if err == nil || err.Error() != "Invalid state Cannot remove node: GraphError: Invalid data (Partition name mai n is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error(err)
		return
	}

	_, err = rn.Execute([]interface{}{"main", "foo", "bar"}, nil, mr)
	if err != nil {
		t.Error(err)
		return
	}

	res, err = fn.Execute([]interface{}{"main", "foo", "bar"}, nil, mr)

	if res != nil || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
}

func TestStoreNodeTrans(t *testing.T) {

	mr := &mockRuntime{}
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	api.GM = gm

	sn := &StoreNodeFunc{}
	tn := &NewTransFunc{}
	tn2 := &NewRollingTransFunc{}
	tc := &CommitTransFunc{}

	if tn.Name() != "db.newTrans" {
		t.Error("Unexpected result:", tn.Name())
		return
	}

	if tn2.Name() != "db.newRollingTrans" {
		t.Error("Unexpected result:", tn2.Name())
		return
	}

	if tc.Name() != "db.commitTrans" {
		t.Error("Unexpected result:", tc.Name())
		return
	}

	if err := tn.Validate(0, mr); err != nil {
		t.Error(err)
		return
	}

	if err := tn2.Validate(1, mr); err != nil {
		t.Error(err)
		return
	}

	if err := tn.Validate(1, mr); err == nil || err.Error() != "Invalid construct Function newTrans does not require any parameters" {
		t.Error(err)
		return
	}

	if err := tn2.Validate(0, mr); err == nil || err.Error() != "Invalid construct Function newRollingTrans requires the rolling threshold (number of operations before rolling)" {
		t.Error(err)
		return
	}

	if err := tc.Validate(1, mr); err != nil {
		t.Error(err)
		return
	}

	if err := tc.Validate(0, mr); err == nil || err.Error() != "Invalid construct Function commitTrans	 requires the transaction to commit as parameter" {
		t.Error(err)
		return
	}

	trans, err := tn.Execute(nil, nil, mr)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = tn2.Execute([]interface{}{"foo"}, nil, mr)
	if err == nil || err.Error() != "Operand is not a number Rolling threshold must be a number not: foo" {
		t.Error(err)
		return
	}

	_, err = tn2.Execute([]interface{}{1}, nil, mr)
	if err != nil {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":  "foo1",
		"kind": "bar",
	}, trans}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":  "foo2",
		"kind": "bar",
	}, trans}, nil, mr); err != nil {
		t.Error(err)
		return
	}

	if _, err := sn.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":  "foo3",
		"kind": "bar",
	}, trans}, nil, mr); err != nil {
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

	if _, err := tc.Execute([]interface{}{"main", map[interface{}]interface{}{
		"key":  "foo3",
		"kind": "bar",
	}, trans}, nil, mr); err == nil || err.Error() != "Invalid construct Parameter must be a transaction" {
		t.Error(err)
		return
	}

	if _, err := tc.Execute([]interface{}{trans}, nil, mr); err != nil {
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

	rn := &RemoveNodeFunc{}

	if rn.Name() != "db.removeNode" {
		t.Error("Unexpected result:", rn.Name())
		return
	}

	_, err = rn.Execute([]interface{}{"main", "foo1", "bar", nil}, nil, mr)
	if err == nil || err.Error() != "Invalid construct Fourth parameter must be a transaction" {
		t.Error(err)
		return
	}

	_, err = rn.Execute([]interface{}{"main", "foo1", "bar", trans}, nil, mr)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = rn.Execute([]interface{}{"main", "foo2", "bar", trans}, nil, mr)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = rn.Execute([]interface{}{"main", "foo3", "bar", trans}, nil, mr)
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

	if _, err := tc.Execute([]interface{}{trans}, nil, mr); err != nil {
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
