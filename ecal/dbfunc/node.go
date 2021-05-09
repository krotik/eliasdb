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

	"devt.de/krotik/ecal/parser"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
StoreNodeFunc inserts a node in EliasDB.
*/
type StoreNodeFunc struct {
	GM *graph.Manager
}

/*
Run executes the ECAL function.
*/
func (f *StoreNodeFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	var err error

	if arglen := len(args); arglen != 2 && arglen != 3 {
		err = fmt.Errorf("Function requires 2 or 3 parameters: partition, node" +
			" map and optionally a transaction")
	}

	if err == nil {
		var trans graph.Trans

		part := fmt.Sprint(args[0])
		nodeMap, ok := args[1].(map[interface{}]interface{})

		// Check parameters

		if !ok {
			err = fmt.Errorf("Second parameter must be a map")
		}

		if err == nil && len(args) > 2 {
			if trans, ok = args[2].(graph.Trans); !ok {
				err = fmt.Errorf("Third parameter must be a transaction")
			}
		}

		// Store the node

		if err == nil {
			node := NewGraphNodeFromECALMap(nodeMap)

			if trans != nil {
				err = trans.StoreNode(part, node)
			} else {
				err = f.GM.StoreNode(part, node)
			}
		}
	}

	return nil, err
}

/*
DocString returns a descriptive string.
*/
func (f *StoreNodeFunc) DocString() (string, error) {
	return "Inserts a node in EliasDB.", nil
}

/*
UpdateNodeFunc updates a node in EliasDB (only update the given values of the node).
*/
type UpdateNodeFunc struct {
	GM *graph.Manager
}

/*
Run executes the ECAL function.
*/
func (f *UpdateNodeFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	var err error

	if arglen := len(args); arglen != 2 && arglen != 3 {
		err = fmt.Errorf("Function requires 2 or 3 parameters: partition, node" +
			" map and optionally a transaction")
	}

	if err == nil {
		var trans graph.Trans

		part := fmt.Sprint(args[0])
		nodeMap, ok := args[1].(map[interface{}]interface{})

		// Check parameters

		if !ok {
			err = fmt.Errorf("Second parameter must be a map")
		}

		if err == nil && len(args) > 2 {
			if trans, ok = args[2].(graph.Trans); !ok {
				err = fmt.Errorf("Third parameter must be a transaction")
			}
		}

		// Store the node

		if err == nil {
			node := NewGraphNodeFromECALMap(nodeMap)

			if trans != nil {
				err = trans.UpdateNode(part, node)
			} else {
				err = f.GM.UpdateNode(part, node)
			}
		}
	}

	return nil, err
}

/*
DocString returns a descriptive string.
*/
func (f *UpdateNodeFunc) DocString() (string, error) {
	return "Updates a node in EliasDB (only update the given values of the node).", nil
}

/*
RemoveNodeFunc removes a node in EliasDB.
*/
type RemoveNodeFunc struct {
	GM *graph.Manager
}

/*
Run executes the ECAL function.
*/
func (f *RemoveNodeFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	var err error

	if arglen := len(args); arglen != 3 && arglen != 4 {
		err = fmt.Errorf("Function requires 3 or 4 parameters: partition, node key" +
			" node kind and optionally a transaction")
	}

	if err == nil {
		var trans graph.Trans

		part := fmt.Sprint(args[0])
		key := fmt.Sprint(args[1])
		kind := fmt.Sprint(args[2])

		// Check parameters

		if len(args) > 3 {
			var ok bool

			if trans, ok = args[3].(graph.Trans); !ok {
				err = fmt.Errorf("Fourth parameter must be a transaction")
			}
		}

		// Remove the node

		if err == nil {

			if trans != nil {
				err = trans.RemoveNode(part, key, kind)
			} else {
				_, err = f.GM.RemoveNode(part, key, kind)
			}
		}
	}

	return nil, err
}

/*
DocString returns a descriptive string.
*/
func (f *RemoveNodeFunc) DocString() (string, error) {
	return "Removes a node in EliasDB.", nil
}

/*
FetchNodeFunc fetches a node in EliasDB.
*/
type FetchNodeFunc struct {
	GM *graph.Manager
}

/*
Run executes the ECAL function.
*/
func (f *FetchNodeFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	var res interface{}
	var err error

	if arglen := len(args); arglen != 3 {
		err = fmt.Errorf("Function requires 3 parameters: partition, node key" +
			" node kind")
	}

	if err == nil {
		var node data.Node

		part := fmt.Sprint(args[0])
		key := fmt.Sprint(args[1])
		kind := fmt.Sprint(args[2])

		conv := func(m map[string]interface{}) map[interface{}]interface{} {
			c := make(map[interface{}]interface{})
			for k, v := range m {
				c[k] = v
			}
			return c
		}

		// Fetch the node

		if node, err = f.GM.FetchNode(part, key, kind); node != nil {
			res = conv(node.Data())
		}
	}

	return res, err
}

/*
DocString returns a descriptive string.
*/
func (f *FetchNodeFunc) DocString() (string, error) {
	return "Fetches a node in EliasDB.", nil
}

// Helper functions
// ================

/*
NewGraphNodeFromECALMap creates a new Node instance from a given map.
*/
func NewGraphNodeFromECALMap(d map[interface{}]interface{}) data.Node {
	node := data.NewGraphNode()

	for k, v := range d {
		node.SetAttr(fmt.Sprint(k), v)
	}

	return node
}
