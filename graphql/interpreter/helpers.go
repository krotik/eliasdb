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
Package interpreter contains the GraphQL interpreter for EliasDB.
*/
package interpreter

import (
	"fmt"
	"sort"
	"strconv"

	"devt.de/krotik/common/lang/graphql/parser"
)

// Not Implemented Runtime
// =======================

/*
Special runtime for not implemented constructs.
*/
type invalidRuntime struct {
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
invalidRuntimeInst returns a new runtime component instance.
*/
func invalidRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &invalidRuntime{rtp, node}
}

/*
Validate this node and all its child nodes.
*/
func (rt *invalidRuntime) Validate() error {
	return rt.rtp.newFatalRuntimeError(ErrInvalidConstruct, rt.node.Name, rt.node)
}

/*
Eval evaluate this runtime component.
*/
func (rt *invalidRuntime) Eval() (map[string]interface{}, error) {
	return nil, rt.rtp.newFatalRuntimeError(ErrInvalidConstruct, rt.node.Name, rt.node)
}

// Value Runtime
// =============

/*
Special runtime for values.
*/
type valueRuntime struct {
	*invalidRuntime
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
valueRuntimeInst returns a new runtime component instance.
*/
func valueRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &valueRuntime{&invalidRuntime{rtp, node}, rtp, node}
}

/*
Value returns the calculated value of the expression.
*/
func (rt *valueRuntime) Value() interface{} {

	if rt.node.Name == parser.NodeVariable {
		val, ok := rt.rtp.VariableValues[rt.node.Token.Val]

		if !ok {
			rt.rtp.handleRuntimeError(fmt.Errorf(
				"Variable %s was used but not declared", rt.node.Token.Val),
				[]string{}, rt.node)
		}

		return val
	} else if rt.node.Name == parser.NodeValue || rt.node.Name == parser.NodeDefaultValue {
		val := rt.node.Token.Val

		if rt.node.Token.ID == parser.TokenIntValue {
			i, _ := strconv.ParseInt(val, 10, 64)
			return i
		} else if rt.node.Token.ID == parser.TokenFloatValue {
			f, _ := strconv.ParseFloat(val, 64)
			return f
		} else if rt.node.Token.ID == parser.TokenStringValue {
			return rt.node.Token.Val
		} else if val == "true" {
			return true
		} else if val == "false" {
			return false
		} else if val == "null" {
			return nil
		}

	} else if rt.node.Name == parser.NodeObjectValue {

		res := make(map[string]interface{})
		for _, c := range rt.node.Children {
			res[c.Token.Val] = c.Children[0].Runtime.(*valueRuntime).Value()
		}
		return res
	} else if rt.node.Name == parser.NodeListValue {

		res := make([]interface{}, 0)
		for _, c := range rt.node.Children {
			res = append(res, c.Runtime.(*valueRuntime).Value())
		}
		return res
	}

	// Default (e.g. enum type)

	return rt.node.Token.Val
}

// Data sorting
// ============

/*
dataSort sorts a list of maps.
*/
func dataSort(list []map[string]interface{}, attr string, ascending bool) {
	sort.Sort(&DataSlice{list, attr, ascending})
}

/*
DataSlice attaches the methods of sort.Interface to []map[string]interface{},
sorting in ascending or descending order by a given attribute.
*/
type DataSlice struct {
	data      []map[string]interface{}
	attr      string
	ascending bool
}

/*
Len belongs to the sort.Interface.
*/
func (d DataSlice) Len() int { return len(d.data) }

/*
Less belongs to the sort.Interface.
*/
func (d DataSlice) Less(i, j int) bool {
	ia, ok1 := d.data[i][d.attr]
	ja, ok2 := d.data[j][d.attr]

	if ok1 && ok2 {
		if d.ascending {
			return fmt.Sprint(ia) < fmt.Sprint(ja)
		}

		return fmt.Sprint(ia) > fmt.Sprint(ja)
	}

	return false
}

/*
Swap belongs to the sort.Interface.
*/
func (d DataSlice) Swap(i, j int) {
	d.data[i], d.data[j] = d.data[j], d.data[i]
}
