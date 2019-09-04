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
	"devt.de/krotik/common/datautil"
	"devt.de/krotik/eliasdb/eql/parser"
	"devt.de/krotik/eliasdb/graph/data"
)

// Not Implemented Runtime
// =======================

/*
Special runtime for not implemented constructs.
*/
type invalidRuntime struct {
	rtp  *eqlRuntimeProvider
	node *parser.ASTNode
}

/*
invalidRuntimeInst returns a new runtime component instance.
*/
func invalidRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &invalidRuntime{rtp, node}
}

/*
Validate this node and all its child nodes.
*/
func (rt *invalidRuntime) Validate() error {
	return rt.rtp.newRuntimeError(ErrInvalidConstruct, rt.node.Name, rt.node)
}

/*
Eval evaluate this runtime component.
*/
func (rt *invalidRuntime) Eval() (interface{}, error) {
	return nil, rt.rtp.newRuntimeError(ErrInvalidConstruct, rt.node.Name, rt.node)
}

/*
Evaluate the value as a condition component.
*/
func (rt *invalidRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return nil, rt.rtp.newRuntimeError(ErrInvalidConstruct, rt.node.Name, rt.node)
}

// Value Runtime
// =============

/*
Runtime for values
*/
type valueRuntime struct {
	rtp             *eqlRuntimeProvider
	node            *parser.ASTNode
	isNodeAttrValue bool
	isEdgeAttrValue bool
	nestedValuePath []string
	condVal         string
}

/*
valueRuntimeInst returns a new runtime component instance.
*/
func valueRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &valueRuntime{rtp, node, false, false, nil, ""}
}

/*
Validate this node and all its child nodes.
*/
func (rt *valueRuntime) Validate() error {
	return nil
}

/*
Eval evaluate this runtime component.
*/
func (rt *valueRuntime) Eval() (interface{}, error) {
	return rt.node.Token.Val, nil
}

/*
Evaluate the value as a condition component.
*/
func (rt *valueRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {

	// Check known constants

	if rt.node.Token.ID == parser.TokenAT {

		// Try to lookup a function

		funcName := rt.node.Children[0].Token.Val

		funcInst, ok := whereFunc[funcName]
		if !ok {
			return nil, rt.rtp.newRuntimeError(ErrInvalidConstruct,
				"Unknown function: "+funcName, rt.node)
		}

		// Execute the function and return its result value

		return funcInst(rt.node, rt.rtp, node, edge)

	} else if rt.node.Token.ID == parser.TokenTRUE {
		return true, nil

	} else if rt.node.Token.ID == parser.TokenFALSE {
		return false, nil

	} else if rt.node.Token.ID == parser.TokenNULL {
		return nil, nil

	} else if rt.node.Name == parser.NodeLIST {

		// Collect items of a list

		var list []interface{}

		for _, item := range rt.node.Children {
			val, _ := item.Runtime.(CondRuntime).CondEval(node, edge)
			list = append(list, val)
		}

		return list, nil
	}

	// Check if this is describing a node or edge value

	var valRet interface{}

	if rt.isNodeAttrValue {

		// Check for nested values

		if rt.nestedValuePath != nil {
			if valMap, ok := node.Attr(rt.nestedValuePath[0]).(map[string]interface{}); ok {
				valRet, _ = datautil.GetNestedValue(valMap, rt.nestedValuePath[1:])
			}
		} else {
			valRet = node.Attr(rt.condVal)
		}

		return valRet, nil

	} else if rt.isEdgeAttrValue {
		if edge == nil {
			return nil, rt.rtp.newRuntimeError(ErrInvalidWhere,
				"No edge data available at this level", rt.node)
		}

		return edge.Attr(rt.condVal), nil
	}

	// Must be a constant value

	return rt.condVal, nil
}
