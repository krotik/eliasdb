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
	"strings"

	"devt.de/krotik/eliasdb/eql/parser"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
traversalRuntime is the runtime for traversals.
*/
type traversalRuntime struct {
	rtp  *eqlRuntimeProvider
	node *parser.ASTNode

	where *parser.ASTNode // Traversal where clause

	sourceNode data.Node   // Source node for traversal - should be injected by the parent
	spec       string      // Spec for this traversal
	specIndex  int         // Index of this traversal in the traversals array
	nodes      []data.Node // Nodes of the last traversal result
	edges      []data.Edge // Edges of the last traversal result
	curptr     int         // Pointer to the next node in the last traversal result
}

/*
traversalRuntimeInst returns a new runtime component instance.
*/
func traversalRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &traversalRuntime{rtp, node, nil, nil, "", -1, nil, nil, 0}
}

/*
Validate this node and all its child nodes.
*/
func (rt *traversalRuntime) Validate() error {

	spec := rt.node.Children[0].Token.Val
	rt.specIndex = -1

	// Check traversal spec

	sspec := strings.Split(spec, ":")
	if len(sspec) != 4 {
		return rt.rtp.newRuntimeError(ErrInvalidSpec, spec, rt.node)
	}

	rt.spec = spec
	rt.specIndex = len(rt.rtp.specs)
	rt.where = nil
	rt.rtp.specs = append(rt.rtp.specs, spec)
	rt.rtp.attrsNodes = append(rt.rtp.attrsNodes, make(map[string]string))
	rt.rtp.attrsEdges = append(rt.rtp.attrsEdges, make(map[string]string))

	// Go through all deeper traversals

	for _, child := range rt.node.Children[1:] {

		if child.Name == parser.NodeTRAVERSE {

			if err := child.Runtime.Validate(); err != nil {
				return err
			}

		} else if child.Name == parser.NodeWHERE {

			whereRuntime := child.Runtime.(*whereRuntime)

			whereRuntime.specIndex = rt.specIndex

			// Reset state of where and store it

			if err := whereRuntime.Validate(); err != nil {
				return err
			}

			rt.where = child

		} else {
			return rt.rtp.newRuntimeError(ErrInvalidConstruct, child.Name, child)
		}
	}

	return nil
}

/*
hasMoreNodes returns true if this traversal runtime component can produce more
nodes. If the result is negative then a new source node is required.
*/
func (rt *traversalRuntime) hasMoreNodes() bool {
	for _, child := range rt.node.Children[1:] {
		if child.Name == parser.NodeTRAVERSE {
			childRuntime := child.Runtime.(*traversalRuntime)
			if childRuntime.hasMoreNodes() {
				return true
			}
		}
	}
	return rt.curptr < len(rt.nodes)
}

/*
newSource assigns a new source node to this traversal component and
traverses it.
*/
func (rt *traversalRuntime) newSource(node data.Node) error {
	var nodes []data.Node
	var edges []data.Edge

	rt.sourceNode = node

	// Do the actual traversal if we got a node

	if node != nil {
		var err error

		// Do a simple traversal without getting any node data first

		nodes, edges, err = rt.rtp.gm.TraverseMulti(rt.rtp.part, rt.sourceNode.Key(),
			rt.sourceNode.Kind(), rt.spec, false)

		if err != nil {
			return err
		}

		// Now get the attributes which are required

		for _, node := range nodes {
			attrs := rt.rtp._attrsNodesFetch[rt.specIndex]

			if len(attrs) > 0 {
				n, err := rt.rtp.gm.FetchNodePart(rt.rtp.part, node.Key(), node.Kind(), attrs)

				if err != nil {
					return err
				} else if n != nil {
					for _, attr := range attrs {
						node.SetAttr(attr, n.Attr(attr))
					}
				}
			}
		}
		for _, edge := range edges {
			attrs := rt.rtp._attrsEdgesFetch[rt.specIndex]

			if len(attrs) > 0 {
				e, err := rt.rtp.gm.FetchEdgePart(rt.rtp.part, edge.Key(), edge.Kind(), attrs)

				if err != nil {
					return err
				} else if e != nil {
					for _, attr := range attrs {
						edge.SetAttr(attr, e.Attr(attr))
					}
				}
			}
		}
	}

	// Apply where clause

	if rt.where != nil {

		fNodes := make([]data.Node, 0, len(nodes))
		fEdges := make([]data.Edge, 0, len(edges))

		for i, node := range nodes {
			edge := edges[i]

			res, err := rt.where.Runtime.(CondRuntime).CondEval(node, edge)
			if err != nil {
				return err
			}

			if res.(bool) {
				fNodes = append(fNodes, node)
				fEdges = append(fEdges, edge)
			}
		}

		nodes = fNodes
		edges = fEdges
	}

	rt.nodes = nodes
	rt.edges = edges
	rt.curptr = 0

	// Check if there are no nodes to display and return an error if
	// empty traversals are not allowed

	if len(rt.nodes) == 0 && !rt.rtp.allowNilTraversal {
		return ErrEmptyTraversal
	}

	// Evaluate the new source

	_, err := rt.Eval()

	return err
}

/*
Eval evaluate this runtime component.
*/
func (rt *traversalRuntime) Eval() (interface{}, error) {

	// Check if a child can handle the call

	for _, child := range rt.node.Children[1:] {
		if child.Name == parser.NodeTRAVERSE {
			childRuntime := child.Runtime.(*traversalRuntime)
			if childRuntime.hasMoreNodes() {
				return childRuntime.Eval()
			}
		}
	}

	// Get the next node and fill the row entry in the provider

	var rowNode data.Node
	var rowEdge data.Edge

	if rt.curptr < len(rt.nodes) {

		// Get a new node from our node list if possible

		rowNode = rt.nodes[rt.curptr]
		rowEdge = rt.edges[rt.curptr]
		rt.curptr++

	}

	if len(rt.rtp.rowNode) == rt.specIndex {
		rt.rtp.rowNode = append(rt.rtp.rowNode, rowNode)
		rt.rtp.rowEdge = append(rt.rtp.rowEdge, rowEdge)
	} else {
		rt.rtp.rowNode[rt.specIndex] = rowNode
		rt.rtp.rowEdge[rt.specIndex] = rowEdge
	}

	// Give the new source to the children and let them evaluate

	for _, child := range rt.node.Children[1:] {
		if child.Name == parser.NodeTRAVERSE {
			childRuntime := child.Runtime.(*traversalRuntime)

			if err := childRuntime.newSource(rt.rtp.rowNode[rt.specIndex]); err != nil {
				return nil, err
			}
		}
	}

	return nil, nil
}
