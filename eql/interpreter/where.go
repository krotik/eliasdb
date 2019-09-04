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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"devt.de/krotik/eliasdb/eql/parser"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
CondRuntime is a component of a condition which can be evaluated
with a node and an edge.
*/
type CondRuntime interface {

	/*
	   CondEval evaluates this condition runtime element.
	*/
	CondEval(node data.Node, edge data.Edge) (interface{}, error)
}

/*
Abstract runtime for condition components
*/
type whereItemRuntime struct {
	rtp     *eqlRuntimeProvider
	astNode *parser.ASTNode
}

/*
Validate this node and all its child nodes.
*/
func (rt *whereItemRuntime) Validate() error {
	return rt.rtp.newRuntimeError(ErrInvalidConstruct, rt.astNode.Name, rt.astNode)
}

/*
Eval evaluate this condition component.
*/
func (rt *whereItemRuntime) Eval() (interface{}, error) {
	return nil, rt.rtp.newRuntimeError(ErrInvalidConstruct, rt.astNode.Name, rt.astNode)
}

/*
valOp executes an operation on two abstract values.
*/
func (rt *whereItemRuntime) valOp(node data.Node, edge data.Edge, op func(interface{}, interface{}) interface{}) (interface{}, error) {

	res1, err := rt.astNode.Children[0].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	res2, err := rt.astNode.Children[1].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	return op(res1, res2), nil
}

/*
stringOp executes an operation on two strings.
*/
func (rt *whereItemRuntime) stringOp(node data.Node, edge data.Edge, op func(string, string) interface{}) (interface{}, error) {

	res1, err := rt.astNode.Children[0].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	res2, err := rt.astNode.Children[1].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	return op(fmt.Sprint(res1), fmt.Sprint(res2)), nil
}

/*
regexOp executes an operation on a string and a regex.
*/
func (rt *whereItemRuntime) regexOp(node data.Node, edge data.Edge, op func(string, *regexp.Regexp) interface{}) (interface{}, error) {

	res1, err := rt.astNode.Children[0].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	res2, err := rt.astNode.Children[1].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	// Try to compile the regex

	res2String := fmt.Sprint(res2)

	regexp, err := regexp.Compile(res2String)
	if err != nil {
		return nil, rt.rtp.newRuntimeError(ErrNotARegex,
			fmt.Sprintf("%#v - %s", res2String, err.Error()), rt.astNode.Children[1])
	}

	return op(fmt.Sprint(res1), regexp), nil
}

/*
numOp executes an operation on two number values.
*/
func (rt *whereItemRuntime) numOp(node data.Node, edge data.Edge, op func(float64, float64) interface{}) (interface{}, error) {

	res1, err := rt.astNode.Children[0].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	res2, err := rt.astNode.Children[1].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	errDetail := func(tokenVal string, opVal string) string {
		if tokenVal == opVal {
			return opVal
		}

		return tokenVal + "=" + opVal
	}

	// Parse the values to numbers

	res1Str := fmt.Sprint(res1)
	res1Num, err := strconv.ParseFloat(res1Str, 64)
	if err != nil {
		return nil, rt.rtp.newRuntimeError(ErrNotANumber, errDetail(rt.astNode.Children[0].Token.Val, res1Str), rt.astNode.Children[0])
	}

	res2Str := fmt.Sprint(res2)
	res2Num, err := strconv.ParseFloat(res2Str, 64)
	if err != nil {
		return nil, rt.rtp.newRuntimeError(ErrNotANumber, errDetail(rt.astNode.Children[1].Token.Val, res2Str), rt.astNode.Children[1])
	}

	return op(res1Num, res2Num), nil
}

/*
listOp executes a list operation on a single value and a list.
*/
func (rt *whereItemRuntime) listOp(node data.Node, edge data.Edge, op func(interface{}, []interface{}) interface{}) (interface{}, error) {

	res1, err := rt.astNode.Children[0].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	res2, err := rt.astNode.Children[1].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	errDetail := func(tokenVal string, opVal string) string {
		if tokenVal == opVal {
			return opVal
		}

		return tokenVal + "=" + opVal
	}

	// Parse right value to a list

	res2List, ok := res2.([]interface{})
	if !ok {
		return nil, rt.rtp.newRuntimeError(ErrNotAList, errDetail(rt.astNode.Children[1].Token.Val, fmt.Sprint(res2)), rt.astNode.Children[1])
	}

	return op(res1, res2List), nil
}

/*
boolOp executes an operation on two boolean values. Can optionally try a
short circuit operation.
*/
func (rt *whereItemRuntime) boolOp(node data.Node, edge data.Edge, op func(bool, bool) interface{},
	scop func(bool) interface{}) (interface{}, error) {

	res1, err := rt.astNode.Children[0].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	if len(rt.astNode.Children) == 1 {

		// Special case for "not" operation

		return op(toBool(res1), false), nil
	}

	// Try short circuit

	res1bool := toBool(res1)

	if scop != nil {
		if ret := scop(res1bool); ret != nil {
			return ret, nil
		}
	}

	res2, err := rt.astNode.Children[1].Runtime.(CondRuntime).CondEval(node, edge)
	if err != nil {
		return nil, err
	}

	return op(res1bool, toBool(res2)), nil
}

/*
toBool is a helper function to turn any value into a boolean.
*/
func toBool(res interface{}) bool {

	switch res := res.(type) {

	default:
		return res != nil

	case bool:
		return res

	case float64:
		return res > 0

	case string:

		// Try to convert the string into a number

		num, err := strconv.ParseFloat(res, 64)
		if err == nil {
			return num > 0
		}

		return res != ""
	}
}

func equals(res1 interface{}, res2 interface{}) bool {

	// Try to convert the string into a number

	num1, err := strconv.ParseFloat(fmt.Sprint(res1), 64)
	if err == nil {
		num2, err := strconv.ParseFloat(fmt.Sprint(res2), 64)
		if err == nil {
			return num1 == num2
		}
	}

	return fmt.Sprintf("%v", res1) == fmt.Sprintf("%v", res2)
}

// Where runtime
// =============

/*
Runtime for where
*/
type whereRuntime struct {
	rtp     *eqlRuntimeProvider
	astNode *parser.ASTNode

	specIndex int // Index of this traversal in the traversals array
}

/*
whereRuntimeInst returns a new runtime component instance.
*/
func whereRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &whereRuntime{rtp, node, 0}
}

/*
Validate this node and all its child nodes.
*/
func (rt *whereRuntime) Validate() error {
	var visitChildren func(astNode *parser.ASTNode) error

	visitChildren = func(astNode *parser.ASTNode) error {

		// Determine which values should be interpreted as node attributes

		if astNode.Name == parser.NodeVALUE {
			val := astNode.Token.Val
			lcval := strings.ToLower(val)

			valRuntime, ok := astNode.Runtime.(*valueRuntime)
			if !ok {
				return astNode.Runtime.Validate()
			}

			if strings.HasPrefix(lcval, "eattr:") {
				valRuntime.condVal = val[6:]
				valRuntime.isNodeAttrValue = false
				valRuntime.isEdgeAttrValue = true

			} else if strings.HasPrefix(lcval, "attr:") {
				valRuntime.condVal = val[5:]
				valRuntime.isNodeAttrValue = true
				valRuntime.isEdgeAttrValue = false

			} else if strings.HasPrefix(lcval, "val:") {
				valRuntime.condVal = val[4:]
				valRuntime.isNodeAttrValue = false
				valRuntime.isEdgeAttrValue = false

			} else {
				valRuntime.condVal = val
				valRuntime.isNodeAttrValue = rt.rtp.ni.IsValidAttr(val)
				valRuntime.isEdgeAttrValue = false

				// Check if we have a nested value

				if strings.Contains(val, ".") {

					nestedValuePath := strings.Split(val, ".")

					if rt.rtp.ni.IsValidAttr(nestedValuePath[0]) {
						valRuntime.condVal = nestedValuePath[0]
						valRuntime.nestedValuePath = nestedValuePath
						valRuntime.isNodeAttrValue = true
					}
				}
			}

			// Make sure attributes are queried

			if valRuntime.isNodeAttrValue {
				rt.rtp.attrsNodes[rt.specIndex][valRuntime.condVal] = ""
			} else if valRuntime.isEdgeAttrValue {
				rt.rtp.attrsEdges[rt.specIndex][valRuntime.condVal] = ""
			}
		}

		for _, child := range astNode.Children {
			if err := visitChildren(child); err != nil {
				return err
			}
		}

		return nil
	}

	return visitChildren(rt.astNode)
}

/*
Eval evaluates the where clause a
*/
func (rt *whereRuntime) Eval() (interface{}, error) {
	return nil, rt.rtp.newRuntimeError(ErrInvalidConstruct, rt.astNode.Name, rt.astNode)
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *whereRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	res, err := rt.astNode.Children[0].Runtime.(CondRuntime).CondEval(node, edge)
	return toBool(res), err
}

// Where related runtimes
// ======================

/*
Equal runtime
*/
type equalRuntime struct {
	*whereItemRuntime
}

/*
equalRuntimeInst returns a new runtime component instance.
*/
func equalRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &equalRuntime{&whereItemRuntime{rtp, node}}
}

/*
Evaluate this condition runtime element.
*/
func (rt *equalRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.valOp(node, edge, func(res1 interface{}, res2 interface{}) interface{} { return equals(res1, res2) })
}

/*
CondEval evaluates this condition runtime element.
*/
type notEqualRuntime struct {
	*whereItemRuntime
}

/*
notEqualRuntimeInst returns a new runtime component instance.
*/
func notEqualRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &notEqualRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *notEqualRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.valOp(node, edge, func(res1 interface{}, res2 interface{}) interface{} { return !equals(res1, res2) })
}

/*
Less than runtime
*/
type lessThanRuntime struct {
	*whereItemRuntime
}

/*
lessThanRuntimeInst returns a new runtime component instance.
*/
func lessThanRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &lessThanRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *lessThanRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	ret, err := rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return res1 < res2 })

	if err != nil {

		// Do a simple string ordering

		ret, err = rt.valOp(node, edge, func(res1 interface{}, res2 interface{}) interface{} { return fmt.Sprint(res1) < fmt.Sprint(res2) })
	}

	return ret, err
}

/*
Less than equals runtime
*/
type lessThanEqualsRuntime struct {
	*whereItemRuntime
}

/*
lessThanEqualsRuntimeInst returns a new runtime component instance.
*/
func lessThanEqualsRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &lessThanEqualsRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *lessThanEqualsRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	ret, err := rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return res1 <= res2 })

	if err != nil {

		// Do a simple string ordering

		ret, err = rt.valOp(node, edge, func(res1 interface{}, res2 interface{}) interface{} { return fmt.Sprint(res1) <= fmt.Sprint(res2) })
	}

	return ret, err
}

/*
Greater than runtime
*/
type greaterThanRuntime struct {
	*whereItemRuntime
}

/*
greaterThanRuntimeInst returns a new runtime component instance.
*/
func greaterThanRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &greaterThanRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *greaterThanRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	ret, err := rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return res1 > res2 })

	if err != nil {

		// Do a simple string ordering

		ret, err = rt.valOp(node, edge, func(res1 interface{}, res2 interface{}) interface{} { return fmt.Sprint(res1) > fmt.Sprint(res2) })
	}

	return ret, err
}

/*
Greater than equals runtime
*/
type greaterThanEqualsRuntime struct {
	*whereItemRuntime
}

/*
greaterThanEqualsRuntimeInst returns a new runtime component instance.
*/
func greaterThanEqualsRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &greaterThanEqualsRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *greaterThanEqualsRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	ret, err := rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return res1 >= res2 })

	if err != nil {

		// Do a simple string ordering

		ret, err = rt.valOp(node, edge, func(res1 interface{}, res2 interface{}) interface{} { return fmt.Sprint(res1) >= fmt.Sprint(res2) })
	}

	return ret, err
}

/*
And runtime
*/
type andRuntime struct {
	*whereItemRuntime
}

/*
andRuntimeInst returns a new runtime component instance.
*/
func andRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &andRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *andRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.boolOp(node, edge, func(res1 bool, res2 bool) interface{} { return res1 && res2 },
		func(res1 bool) interface{} {
			if !res1 {
				return false
			}
			return nil
		})
}

/*
Or runtime
*/
type orRuntime struct {
	*whereItemRuntime
}

/*
orRuntimeInst returns a new runtime component instance.
*/
func orRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &orRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *orRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.boolOp(node, edge, func(res1 bool, res2 bool) interface{} { return res1 || res2 },
		func(res1 bool) interface{} {
			if res1 {
				return true
			}
			return nil
		})
}

/*
Not runtime
*/
type notRuntime struct {
	*whereItemRuntime
}

/*
notRuntimeInst returns a new runtime component instance.
*/
func notRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &notRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *notRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.boolOp(node, edge, func(res1 bool, res2 bool) interface{} { return !res1 }, nil)
}

/*
Plus runtime
*/
type plusRuntime struct {
	*whereItemRuntime
}

/*
plusRuntimeInst returns a new runtime component instance.
*/
func plusRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &plusRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *plusRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return res1 + res2 })
}

/*
Minus runtime
*/
type minusRuntime struct {
	*whereItemRuntime
}

/*
minusRuntimeInst returns a new runtime component instance.
*/
func minusRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &minusRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *minusRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return res1 - res2 })
}

/*
Times runtime
*/
type timesRuntime struct {
	*whereItemRuntime
}

/*
timesRuntimeInst returns a new runtime component instance.
*/
func timesRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &timesRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *timesRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return res1 * res2 })
}

/*
Div runtime
*/
type divRuntime struct {
	*whereItemRuntime
}

/*
divRuntimeInst returns a new runtime component instance.
*/
func divRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &divRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *divRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return res1 / res2 })
}

/*
ModInt runtime
*/
type modIntRuntime struct {
	*whereItemRuntime
}

/*
modIntRuntimeInst returns a new runtime component instance.
*/
func modIntRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &modIntRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *modIntRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return int(int(res1) % int(res2)) })
}

/*
DivInt runtime
*/
type divIntRuntime struct {
	*whereItemRuntime
}

/*
divIntRuntimeInst returns a new runtime component instance.
*/
func divIntRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &divIntRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *divIntRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.numOp(node, edge, func(res1 float64, res2 float64) interface{} { return int(int(res1) / int(res2)) })
}

/*
In runtime
*/
type inRuntime struct {
	*whereItemRuntime
}

/*
inRuntimeInst returns a new runtime component instance.
*/
func inRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &inRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *inRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.listOp(node, edge, func(res1 interface{}, res2 []interface{}) interface{} {

		for _, item := range res2 {
			if equals(res1, item) {
				return true
			}
		}

		return false
	})
}

/*
Not in runtime
*/
type notInRuntime struct {
	*whereItemRuntime
}

/*
notInRuntimeInst returns a new runtime component instance.
*/
func notInRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &notInRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *notInRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.listOp(node, edge, func(res1 interface{}, res2 []interface{}) interface{} {

		for _, item := range res2 {
			if equals(res1, item) {
				return false
			}
		}

		return true
	})
}

/*
Like runtime
*/
type likeRuntime struct {
	compiledRegex *regexp.Regexp // Quick lookup of the compiled regex if it is a constant
	*whereItemRuntime
}

/*
likeRuntimeInst returns a new runtime component instance.
*/
func likeRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &likeRuntime{nil, &whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *likeRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {

	// Check for constant regexp

	if valRT, ok := rt.astNode.Children[1].Runtime.(*valueRuntime); ok {
		if !valRT.isNodeAttrValue && !valRT.isEdgeAttrValue {

			// Given regex is a constant and only needs to be compiled once

			val, _ := valRT.CondEval(node, edge)
			valStr := fmt.Sprint(val)
			regexp, err := regexp.Compile(valStr)
			if err != nil {
				return nil, rt.rtp.newRuntimeError(ErrNotARegex,
					fmt.Sprintf("%#v - %s", valStr, err.Error()), rt.astNode.Children[1])
			}

			rt.compiledRegex = regexp
		}
	}

	if rt.compiledRegex == nil {
		return rt.regexOp(node, edge, func(res1 string, res2 *regexp.Regexp) interface{} { return res2.MatchString(res1) })
	}

	return rt.stringOp(node, edge, func(res1 string, res2 string) interface{} { return rt.compiledRegex.MatchString(res1) })
}

/*
Contains runtime
*/
type containsRuntime struct {
	*whereItemRuntime
}

/*
containsRuntimeInst returns a new runtime component instance.
*/
func containsRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &containsRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *containsRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.stringOp(node, edge, func(res1 string, res2 string) interface{} { return strings.Contains(res1, res2) })
}

/*
Contains not runtime
*/
type containsNotRuntime struct {
	*whereItemRuntime
}

/*
containsNotRuntimeInst returns a new runtime component instance.
*/
func containsNotRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &containsNotRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *containsNotRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.stringOp(node, edge, func(res1 string, res2 string) interface{} { return !strings.Contains(res1, res2) })
}

/*
Begins with runtime
*/
type beginsWithRuntime struct {
	*whereItemRuntime
}

/*
beginsWithRuntimeInst returns a new runtime component instance.
*/
func beginsWithRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &beginsWithRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *beginsWithRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.stringOp(node, edge, func(res1 string, res2 string) interface{} { return strings.HasPrefix(res1, res2) })
}

/*
Ends with runtime
*/
type endsWithRuntime struct {
	*whereItemRuntime
}

/*
endsWithRuntimeInst returns a new runtime component instance.
*/
func endsWithRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &endsWithRuntime{&whereItemRuntime{rtp, node}}
}

/*
CondEval evaluates this condition runtime element.
*/
func (rt *endsWithRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.stringOp(node, edge, func(res1 string, res2 string) interface{} { return strings.HasSuffix(res1, res2) })
}
