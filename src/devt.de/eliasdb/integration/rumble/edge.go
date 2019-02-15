/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
Package rumble contains Rumble functions which interface with EliasDB.
*/
package rumble

import (
	"fmt"

	"devt.de/common/defs/rumble"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
)

// Function: storeEdge
// ===================

/*
StoreEdgeFunc inserts or updates an edge in EliasDB.
*/
type StoreEdgeFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *StoreEdgeFunc) Name() string {
	return "db.storeEdge"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *StoreEdgeFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 2 && argsNum != 3 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function storeEdge requires 2 or 3 parameters: partition, edge"+
				" map and optionally a transaction")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *StoreEdgeFunc) Execute(argsVal []interface{}, vars rumble.Variables,
	rt rumble.Runtime) (interface{}, rumble.RuntimeError) {

	var trans graph.Trans
	var err rumble.RuntimeError

	part := fmt.Sprint(argsVal[0])
	nodeMap, ok := argsVal[1].(map[interface{}]interface{})

	// Check parameters

	if !ok {
		err = rt.NewRuntimeError(rumble.ErrNotAMap,
			"Second parameter must be a map")
	}

	if err == nil && len(argsVal) > 2 {
		if trans, ok = argsVal[2].(graph.Trans); !ok {
			err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
				"Third parameter must be a transaction")
		}
	}

	// Build up node to store

	edge := data.NewGraphEdgeFromNode(NewGraphNodeFromRumbleMap(nodeMap))

	// Store the edge

	if err == nil {

		if trans != nil {
			err = trans.StoreEdge(part, edge)
		} else {
			err = api.GM.StoreEdge(part, edge)
		}

		if err != nil {

			// Wrap error message in RuntimeError

			err = rt.NewRuntimeError(rumble.ErrInvalidState,
				fmt.Sprintf("Cannot store edge: %v", err.Error()))
		}
	}

	return nil, err
}

// Function: removeEdge
// ====================

/*
RemoveEdgeFunc removes an edge in EliasDB.
*/
type RemoveEdgeFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *RemoveEdgeFunc) Name() string {
	return "db.removeEdge"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *RemoveEdgeFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 3 && argsNum != 4 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function removeEdge requires 3 or 4 parameters: partition, edge key,"+
				" edge kind and optionally a transaction")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *RemoveEdgeFunc) Execute(argsVal []interface{}, vars rumble.Variables,
	rt rumble.Runtime) (interface{}, rumble.RuntimeError) {

	var trans graph.Trans
	var err rumble.RuntimeError

	part := fmt.Sprint(argsVal[0])
	key := fmt.Sprint(argsVal[1])
	kind := fmt.Sprint(argsVal[2])

	// Check parameters

	if len(argsVal) > 3 {
		var ok bool

		if trans, ok = argsVal[3].(graph.Trans); !ok {
			err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
				"Fourth parameter must be a transaction")
		}
	}

	// Remove the edge

	if err == nil {

		if trans != nil {
			err = trans.RemoveEdge(part, key, kind)
		} else {
			_, err = api.GM.RemoveEdge(part, key, kind)
		}

		if err != nil {

			// Wrap error message in RuntimeError

			err = rt.NewRuntimeError(rumble.ErrInvalidState,
				fmt.Sprintf("Cannot remove edge: %v", err.Error()))
		}
	}

	return nil, err
}

// Function: fetchEdge
// ===================

/*
FetchEdgeFunc fetches an edge in EliasDB.
*/
type FetchEdgeFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *FetchEdgeFunc) Name() string {
	return "db.fetchEdge"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *FetchEdgeFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 3 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function fetchEdge requires 3 parameters: partition, edge key and"+
				" edge kind")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *FetchEdgeFunc) Execute(argsVal []interface{}, vars rumble.Variables,
	rt rumble.Runtime) (interface{}, rumble.RuntimeError) {

	var node data.Node
	var res interface{}
	var err rumble.RuntimeError

	part := fmt.Sprint(argsVal[0])
	key := fmt.Sprint(argsVal[1])
	kind := fmt.Sprint(argsVal[2])

	conv := func(m map[string]interface{}) map[interface{}]interface{} {
		c := make(map[interface{}]interface{})
		for k, v := range m {
			c[k] = v
		}
		return c
	}

	// Fetch the node

	if node, err = api.GM.FetchEdge(part, key, kind); node != nil {
		res = conv(node.Data())
	}

	if err != nil {

		// Wrap error message in RuntimeError

		err = rt.NewRuntimeError(rumble.ErrInvalidState,
			fmt.Sprintf("Cannot fetch edge: %v", err.Error()))
	}

	return res, err
}

// Function: traverse
// ==================

/*
TraverseFunc traverses an edge in EliasDB.
*/
type TraverseFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *TraverseFunc) Name() string {
	return "db.traverse"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *TraverseFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 4 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function traverse requires 4 parameters: partition, node key,"+
				" node kind and a traversal spec")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *TraverseFunc) Execute(argsVal []interface{}, vars rumble.Variables,
	rt rumble.Runtime) (interface{}, rumble.RuntimeError) {

	var nodes []data.Node
	var edges []data.Edge
	var res interface{}
	var err rumble.RuntimeError

	part := fmt.Sprint(argsVal[0])
	key := fmt.Sprint(argsVal[1])
	kind := fmt.Sprint(argsVal[2])
	spec := fmt.Sprint(argsVal[3])

	conv := func(m map[string]interface{}) map[interface{}]interface{} {
		c := make(map[interface{}]interface{})
		for k, v := range m {
			c[k] = v
		}
		return c
	}

	// Do the traversal

	if nodes, edges, err = api.GM.TraverseMulti(part, key, kind, spec, true); err == nil {

		resNodes := make([]interface{}, len(nodes))
		for i, n := range nodes {
			resNodes[i] = conv(n.Data())
		}
		resEdges := make([]interface{}, len(edges))
		for i, e := range edges {
			resEdges[i] = conv(e.Data())
		}
		res = []interface{}{resNodes, resEdges}
	}

	if err != nil {

		// Wrap error message in RuntimeError

		err = rt.NewRuntimeError(rumble.ErrInvalidState,
			fmt.Sprintf("Cannot traverse: %v", err.Error()))
	}

	return res, err
}
