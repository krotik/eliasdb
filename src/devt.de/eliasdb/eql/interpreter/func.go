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
	"strings"
	"time"

	"devt.de/common/datautil"
	"devt.de/common/errorutil"
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
	"count":     whereCount,
	"parseDate": whereParseDate,
}

/*
whereCount counts reachable nodes via a given traversal.
*/
func whereCount(astNode *parser.ASTNode, rtp *eqlRuntimeProvider,
	node data.Node, edge data.Edge) (interface{}, error) {

	// Check parameters

	np := len(astNode.Children)

	if np != 2 && np != 3 {
		return nil, rtp.newRuntimeError(ErrInvalidConstruct,
			"Count function requires 1 or 2 parameters: traversal spec, condition clause", astNode)
	}

	spec := astNode.Children[1].Token.Val

	// Only need to retrieve full node values if there is a where clause

	nodes, _, err := rtp.gm.TraverseMulti(rtp.part, node.Key(), node.Kind(), spec, np == 3)

	if np == 3 {
		var filteredNodes []data.Node

		// If a where clause was given parse it and evaluate it

		conditionString := astNode.Children[2].Token.Val

		ast, err := parser.ParseWithRuntime("count condition", "get _ where "+conditionString, &GetRuntimeProvider{rtp})
		if err != nil {
			return nil, rtp.newRuntimeError(ErrInvalidConstruct,
				fmt.Sprintf("Invalid condition clause in count function: %s", err), astNode)
		}

		cond := ast.Children[1] // This should always pick out just the where clause

		errorutil.AssertOk(cond.Runtime.Validate()) // Validation should alwasys succeed

		for _, n := range nodes {
			res, err := cond.Children[0].Runtime.(CondRuntime).CondEval(n, nil)

			if err != nil {
				return nil, rtp.newRuntimeError(ErrInvalidConstruct,
					fmt.Sprintf("Invalid condition clause in count function: %s", err), astNode)
			} else if b, ok := res.(bool); ok {
				if b {
					filteredNodes = append(filteredNodes, n)
				}
			} else {
				return nil, rtp.newRuntimeError(ErrInvalidConstruct,
					"Could not evaluate condition clause in count function", astNode)
			}
		}

		nodes = filteredNodes
	}

	return len(nodes), err
}

/*
whereParseDate converts a date string into a unix time value.
*/
func whereParseDate(astNode *parser.ASTNode, rtp *eqlRuntimeProvider,
	node data.Node, edge data.Edge) (interface{}, error) {

	var datestr interface{}
	var t time.Time
	var ret int64
	var err error

	// Define default layout

	layout := time.RFC3339

	// Check parameters

	if len(astNode.Children) < 2 {
		return nil, rtp.newRuntimeError(ErrInvalidConstruct,
			"parseDate function requires 1 parameter: date string", astNode)
	}

	if len(astNode.Children) > 2 {
		datestr, err = astNode.Children[2].Runtime.(CondRuntime).CondEval(node, edge)
		layout = fmt.Sprint(datestr)
	}

	// Convert the date string

	datestr, err = astNode.Children[1].Runtime.(CondRuntime).CondEval(node, edge)

	if err == nil {

		t, err = time.Parse(layout, fmt.Sprint(datestr))

		if err == nil {
			ret = t.Unix()
		}
	}

	return ret, err
}

// Show related functions
// ======================

/*
Runtime map for show related functions
*/
var showFunc = map[string]FuncShowInst{
	"count":  showCountInst,
	"objget": showObjgetInst,
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
	var cond *parser.ASTNode

	// Check parameters

	np := len(astNode.Children)

	if np != 3 && np != 4 {
		return nil, "", "", errors.New("Count function requires 2 or 3 parameters: traversal step, traversal spec, condition clause")
	}

	pos := astNode.Children[1].Token.Val
	spec := astNode.Children[2].Token.Val

	if np == 4 {

		// If a condition clause was given parse it

		condString := astNode.Children[3].Token.Val

		ast, err := parser.ParseWithRuntime("count condition", "get _ where "+condString, &GetRuntimeProvider{rtp})
		if err != nil {
			return nil, "", "", fmt.Errorf("Invalid condition clause in count function: %s", err)
		}

		cond = ast.Children[1] // This should always pick out just the condition clause

		errorutil.AssertOk(cond.Runtime.Validate()) // Validation should alwasys succeed
	}

	return &showCount{rtp, astNode, spec, cond}, pos + ":n:key", "Count", nil
}

/*
showCount is the number of reachable nodes via a given traversal spec.
*/
type showCount struct {
	rtp       *eqlRuntimeProvider
	astNode   *parser.ASTNode
	spec      string
	condition *parser.ASTNode
}

/*
name returns the name of the function.
*/
func (sc *showCount) name() string {
	return "count"
}

/*
eval counts reachable nodes via a given traversal.
*/
func (sc *showCount) eval(node data.Node, edge data.Edge) (interface{}, string, error) {
	condString := ""

	// Only need to retrieve full node values if there is a where clause

	nodes, _, err := sc.rtp.gm.TraverseMulti(sc.rtp.part, node.Key(), node.Kind(), sc.spec, sc.condition != nil)
	if err != nil {
		return nil, "", err
	}

	if sc.condition != nil {
		var filteredNodes []data.Node

		// If there is a condition clause filter the result

		condString, _ = parser.PrettyPrint(sc.condition)

		for _, n := range nodes {
			res, err := sc.condition.Children[0].Runtime.(CondRuntime).CondEval(n, nil)

			if err != nil {
				return nil, "", err
			} else if b, ok := res.(bool); ok {
				if b {
					filteredNodes = append(filteredNodes, n)
				}
			} else {

				return nil, "", sc.rtp.newRuntimeError(ErrInvalidConstruct,
					"Could not evaluate condition clause in count function", sc.astNode)
			}

		}

		nodes = filteredNodes
	}

	srcQuery := fmt.Sprintf("q:lookup %s %s traverse %s %s end show 2:n:%s, 2:n:%s, 2:n:%s",
		node.Kind(), strconv.Quote(node.Key()), sc.spec, condString, data.NodeKey, data.NodeKind, data.NodeName)

	return len(nodes), srcQuery, nil
}

// Show Objget
// -----------

/*
showObjgetInst creates a new showObjget object.
*/
func showObjgetInst(astNode *parser.ASTNode, rtp *eqlRuntimeProvider) (FuncShow, string, string, error) {

	// Check parameters

	if len(astNode.Children) != 4 {
		return nil, "", "",
			fmt.Errorf("Objget function requires 3 parameters: traversal step, attribute name, path to value")
	}

	pos := astNode.Children[1].Token.Val
	attr := astNode.Children[2].Token.Val
	path := astNode.Children[3].Token.Val

	return &showObjget{rtp, attr, strings.Split(path, ".")}, pos + ":n:" + attr,
		rtp.ni.AttributeDisplayString("", attr) + "." + path, nil
}

/*
showObjget reaches into an object and extracts a value.
*/
type showObjget struct {
	rtp  *eqlRuntimeProvider
	attr string
	path []string
}

/*
name returns the name of the function.
*/
func (so *showObjget) name() string {
	return "objget"
}

/*
eval reaches into an object and extracts a value.
*/
func (so *showObjget) eval(node data.Node, edge data.Edge) (interface{}, string, error) {

	val := node.Attr(so.attr)

	if valMap, ok := val.(map[string]interface{}); ok {
		val, _ = datautil.GetNestedValue(valMap, so.path)
	}

	return val, "n:" + node.Kind() + ":" + node.Key(), nil
}
