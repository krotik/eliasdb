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
Package interpreter contains the EQL interpreter.
*/
package interpreter

import (
	"errors"
	"fmt"
	"strconv"

	"devt.de/eliasdb/eql/parser"
	"devt.de/eliasdb/graph/data"
)

// Where related functions
// =======================

/*
FuncWhere represents a where related function.
*/
type FuncWhere func(astNode *parser.ASTNode, rtp *eqlRuntimeProvider,
	node data.Node, edge data.Edge) (interface{}, error)

/*
Runtime map for where related functions
*/
var whereFunc = map[string]FuncWhere{
	"count": whereCount,
}

/*
whereCount counts reachable nodes via a given traversal.
*/
func whereCount(astNode *parser.ASTNode, rtp *eqlRuntimeProvider,
	node data.Node, edge data.Edge) (interface{}, error) {

	// Check parameters

	if len(astNode.Children) != 2 {
		return nil, rtp.newRuntimeError(ErrInvalidConstruct,
			"Count function requires 1 parameter: traversal spec", astNode)
	}

	spec := astNode.Children[1].Token.Val

	nodes, _, err := rtp.gm.TraverseMulti(rtp.part, node.Key(), node.Kind(), spec, false)

	return len(nodes), err
}

// Show related functions
// ======================

/*
Runtime map for show related functions
*/
var showFunc = map[string]FuncShowInst{
	"count": showCountInst,
}

/*
FuncShow is the interface definition for show related functions
*/
type FuncShow interface {

	/*
	   name returns the name of the function.
	*/
	name() string

	/*
		eval runs the function. Returns the result and a source for the result.
		The source should be a concrete node/edge key and kind or a query and
		should be returned in either of the following formats:
		n:<key>:<kind> for a node
		e:<key>:<kind> for an edge
		q:<query> for a query
	*/
	eval(node data.Node, edge data.Edge) (interface{}, string, error)
}

/*
FuncShowInst creates a function object. Returns which column data should be queried and
how the colummn should be named.
*/
type FuncShowInst func(astNode *parser.ASTNode, rtp *eqlRuntimeProvider) (FuncShow, string, string, error)

// Show Count
// ----------

/*
showCountInst creates a new showCount object.
*/
func showCountInst(astNode *parser.ASTNode, rtp *eqlRuntimeProvider) (FuncShow, string, string, error) {

	// Check parameters

	if len(astNode.Children) != 3 {
		return nil, "", "", errors.New("Count function requires 2 parameters: data index, traversal spec")
	}

	pos := astNode.Children[1].Token.Val
	spec := astNode.Children[2].Token.Val

	return &showCount{rtp, spec}, pos + ":n:key", "Count", nil
}

/*
showCount is the number of reachable nodes via a given traversal spec.
*/
type showCount struct {
	rtp  *eqlRuntimeProvider
	spec string
}

/*
name returns the name of the function.
*/
func (sc *showCount) name() string {
	return "count"
}

/*
showCount counts reachable nodes via a given traversal.
*/
func (sc *showCount) eval(node data.Node, edge data.Edge) (interface{}, string, error) {

	nodes, _, err := sc.rtp.gm.TraverseMulti(sc.rtp.part, node.Key(), node.Kind(), sc.spec, false)
	if err != nil {
		return nil, "", err
	}

	srcQuery := fmt.Sprintf("q:lookup %s \"%s\" traverse %s end show 2:n:%s, 2:n:%s, 2:n:%s",
		node.Kind(), strconv.Quote(node.Key()), sc.spec, data.NodeKey, data.NodeKind, data.NodeName)

	return len(nodes), srcQuery, nil
}
