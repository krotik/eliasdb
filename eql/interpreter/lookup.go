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
	"devt.de/krotik/eliasdb/eql/parser"
	"devt.de/krotik/eliasdb/graph"
)

// Runtime provider for LOOKUP queries
// ===================================

/*
Instance function for LOOKUP query components
*/
type lookupInst func(*LookupRuntimeProvider, *parser.ASTNode) parser.Runtime

/*
Runtime map for LOOKUP query specific components
*/
var lookupProviderMap = map[string]lookupInst{
	parser.NodeLOOKUP: lookupRuntimeInst,
}

/*
LookupRuntimeProvider data structure
*/
type LookupRuntimeProvider struct {
	*eqlRuntimeProvider
}

/*
NewLookupRuntimeProvider creates a new LookupRuntimeProvider object. This provider
can interpret LOOKUP queries.
*/
func NewLookupRuntimeProvider(name string, part string, gm *graph.Manager, ni NodeInfo) *LookupRuntimeProvider {
	return &LookupRuntimeProvider{&eqlRuntimeProvider{name, part, gm, ni, "", false, nil, "",
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}}
}

/*
Runtime returns a runtime component for a given ASTNode.
*/
func (rtp *LookupRuntimeProvider) Runtime(node *parser.ASTNode) parser.Runtime {
	if pinst, ok := generalProviderMap[node.Name]; ok {
		return pinst(rtp.eqlRuntimeProvider, node)
	} else if pinst, ok := lookupProviderMap[node.Name]; ok {
		return pinst(rtp, node)
	}
	return invalidRuntimeInst(rtp.eqlRuntimeProvider, node)
}

// LOOKUP Runtime
// ==============

type lookupRuntime struct {
	*getRuntime
	rtp  *LookupRuntimeProvider
	node *parser.ASTNode
}

func lookupRuntimeInst(rtp *LookupRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &lookupRuntime{&getRuntime{&GetRuntimeProvider{rtp.eqlRuntimeProvider}, node}, rtp, node}
}

/*
 Validate and reset this runtime component and all its child components.
*/
func (rt *lookupRuntime) Validate() error {

	// First child is always the first node kind to query
	// (validation of this value was done during lexing)

	startKind := rt.node.Children[0].Token.Val

	// Check how many keys were given

	var keys []string

	// Assume initially that only keys where given

	initIndex := len(rt.node.Children) - 1

	for i, child := range rt.node.Children[1:] {
		if child.Token.ID != parser.TokenVALUE {

			// We have a first non-id child

			initIndex = i
			break

		} else {

			// Collect all given keys

			keys = append(keys, child.Token.Val)
		}
	}

	// Initialise the runtime provider

	initErr := rt.rtp.init(startKind, rt.node.Children[initIndex+1:])

	if rt.rtp.groupScope == "" {

		nodePtr := len(keys)

		if nodePtr > 0 {

			// Iterate over all traversed nodes

			rt.rtp.nextStartKey = func() (string, error) {
				nodePtr--
				if nodePtr >= 0 {
					return keys[nodePtr], nil

				}

				return "", nil
			}
		}

	} else {

		// Build a map of keys

		keyMap := make(map[string]string)
		for _, key := range keys {
			keyMap[key] = ""
		}

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
				nodeKey := nodes[nodePtr].Key()

				if _, ok := keyMap[nodeKey]; ok {
					return nodeKey, nil
				}

				return rt.rtp.nextStartKey()
			}

			return "", nil
		}
	}

	return initErr
}

/*
Eval evaluate this runtime component.
*/
func (rt *lookupRuntime) Eval() (interface{}, error) {

	if err := rt.Validate(); err != nil {
		return nil, err
	}

	return rt.getRuntime.gaterResult(rt.node)
}
