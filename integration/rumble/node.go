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

	"devt.de/krotik/common/defs/rumble"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
)

// Function: storeNode
// ===================

/*
StoreNodeFunc inserts or updates a node in EliasDB.
*/
type StoreNodeFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *StoreNodeFunc) Name() string {
	return "db.storeNode"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *StoreNodeFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 2 && argsNum != 3 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function storeNode requires 2 or 3 parameters: partition, node"+
				" map and optionally a transaction")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *StoreNodeFunc) Execute(argsVal []interface{}, vars rumble.Variables,
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

	node := NewGraphNodeFromRumbleMap(nodeMap)

	// Store the node

	if err == nil {

		if trans != nil {
			err = trans.StoreNode(part, node)
		} else {
			err = api.GM.StoreNode(part, node)
		}

		if err != nil {

			// Wrap error message in RuntimeError

			err = rt.NewRuntimeError(rumble.ErrInvalidState,
				fmt.Sprintf("Cannot store node: %v", err.Error()))
		}
	}

	return nil, err
}

// Function: removeNode
// ====================

/*
RemoveNodeFunc removes a node in EliasDB.
*/
type RemoveNodeFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *RemoveNodeFunc) Name() string {
	return "db.removeNode"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *RemoveNodeFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 3 && argsNum != 4 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function removeNode requires 3 or 4 parameters: partition, node key"+
				" node kind and optionally a transaction")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *RemoveNodeFunc) Execute(argsVal []interface{}, vars rumble.Variables,
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

	// Remove the node

	if err == nil {

		if trans != nil {
			err = trans.RemoveNode(part, key, kind)
		} else {
			_, err = api.GM.RemoveNode(part, key, kind)
		}

		if err != nil {

			// Wrap error message in RuntimeError

			err = rt.NewRuntimeError(rumble.ErrInvalidState,
				fmt.Sprintf("Cannot remove node: %v", err.Error()))
		}
	}

	return nil, err
}

// Function: fetchNode
// ===================

/*
FetchNodeFunc fetches a node in EliasDB.
*/
type FetchNodeFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *FetchNodeFunc) Name() string {
	return "db.fetchNode"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *FetchNodeFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 3 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function fetchNode requires 3 parameters: partition, node key"+
				" node kind")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *FetchNodeFunc) Execute(argsVal []interface{}, vars rumble.Variables,
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

	if node, err = api.GM.FetchNode(part, key, kind); node != nil {
		res = conv(node.Data())
	}

	if err != nil {

		// Wrap error message in RuntimeError

		err = rt.NewRuntimeError(rumble.ErrInvalidState,
			fmt.Sprintf("Cannot fetch node: %v", err.Error()))
	}

	return res, err
}

// Helper functions
// ================

/*
NewGraphNodeFromRumbleMap creates a new Node instance.
*/
func NewGraphNodeFromRumbleMap(d map[interface{}]interface{}) data.Node {
	node := data.NewGraphNode()

	for k, v := range d {
		node.SetAttr(fmt.Sprint(k), v)
	}

	return node
}
