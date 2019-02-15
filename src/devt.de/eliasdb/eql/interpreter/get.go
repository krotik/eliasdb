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
	"devt.de/eliasdb/eql/parser"
	"devt.de/eliasdb/graph"
)

// Runtime provider for GET queries
// ================================

/*
Instance function for GET query components
*/
type getInst func(*GetRuntimeProvider, *parser.ASTNode) parser.Runtime

/*
Runtime map for GET query specific components
*/
var getProviderMap = map[string]getInst{
	parser.NodeGET: getRuntimeInst,
}

/*
GetRuntimeProvider data structure
*/
type GetRuntimeProvider struct {
	*eqlRuntimeProvider
}

/*
NewGetRuntimeProvider creates a new GetRuntimeProvider object. This provider
can interpret GET queries.
*/
func NewGetRuntimeProvider(name string, part string, gm *graph.Manager, ni NodeInfo) *GetRuntimeProvider {
	return &GetRuntimeProvider{&eqlRuntimeProvider{name, part, gm, ni, "", false, nil, "",
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}}
}

/*
Runtime returns a runtime component for a given ASTNode.
*/
func (rtp *GetRuntimeProvider) Runtime(node *parser.ASTNode) parser.Runtime {
	if pinst, ok := generalProviderMap[node.Name]; ok {
		return pinst(rtp.eqlRuntimeProvider, node)
	} else if pinst, ok := getProviderMap[node.Name]; ok {
		return pinst(rtp, node)
	}
	return invalidRuntimeInst(rtp.eqlRuntimeProvider, node)
}

// GET Runtime
// ===========

type getRuntime struct {
	rtp  *GetRuntimeProvider
	node *parser.ASTNode
}

func getRuntimeInst(rtp *GetRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &getRuntime{rtp, node}
}

/*
 Validate and reset this runtime component and all its child components.
*/
func (rt *getRuntime) Validate() error {

	// First child is always the first node kind to query
	// (validation of this value was done during lexing)

	startKind := rt.node.Children[0].Token.Val

	initErr := rt.rtp.init(startKind, rt.node.Children[1:])

	if rt.rtp.groupScope == "" {

		// Start keys can be provided by a simple node key iterator

		startKeyIterator, err := rt.rtp.gm.NodeKeyIterator(rt.rtp.part, startKind)

		if err != nil {
			return err
		} else if startKeyIterator == nil {
			return rt.rtp.newRuntimeError(ErrUnknownNodeKind, startKind, rt.node.Children[0])
		}

		rt.rtp.nextStartKey = func() (string, error) {
			nextKey := startKeyIterator.Next()
			if startKeyIterator.LastError != nil {
				return "", startKeyIterator.LastError
			}
			return nextKey, nil
		}

	} else {

		// Try to lookup group node

		nodes, _, err := rt.rtp.gm.TraverseMulti(rt.rtp.part, rt.rtp.groupScope,
			GroupNodeKind, ":::"+startKind, false)

		if err != nil {
			return err
		}

		nodePtr := len(nodes)

		// Iterate over all traversed nodes

		rt.rtp.nextStartKey = func() (string, error) {
			nodePtr--

			if nodePtr >= 0 {
				return nodes[nodePtr].Key(), nil

			}

			return "", nil
		}
	}

	return initErr
}

/*
Eval evaluate this runtime component.
*/
func (rt *getRuntime) Eval() (interface{}, error) {

	// First validate the query and reset the runtime provider datastructures

	if rt.rtp.specs == nil || !allowMultiEval {
		if err := rt.Validate(); err != nil {
			return nil, err
		}
	}

	return rt.gaterResult(rt.node)
}

func (rt *getRuntime) gaterResult(topNode *parser.ASTNode) (interface{}, error) {

	// Generate query

	query, err := parser.PrettyPrint(topNode)

	// Create result object

	res := newSearchResult(rt.rtp.eqlRuntimeProvider, query)

	if err == nil {
		var more bool

		// Go through all rows

		more, err = rt.rtp.next()
		for more && err == nil {

			// Add row to the result

			if err := res.addRow(rt.rtp.rowNode, rt.rtp.rowEdge); err != nil {
				return nil, err
			}

			// More on to the next row

			more, err = rt.rtp.next()
		}

		// Finish the result

		res.finish()
	}

	return res, err
}
