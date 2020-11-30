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

	"devt.de/krotik/common/lang/graphql/parser"
	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
)

// SelectionSet Runtime
// ====================

/*
Runtime for SelectionSets.
*/
type selectionSetRuntime struct {
	*invalidRuntime
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
selectionSetRuntimeInst returns a new runtime component instance.
*/
func selectionSetRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &selectionSetRuntime{&invalidRuntime{rtp, node}, rtp, node}
}

/*
Eval evaluate this runtime component.
*/
func (rt *selectionSetRuntime) Eval() (map[string]interface{}, error) {
	var err error

	// Build result data

	res := make(map[string]interface{})

	for _, c := range rt.node.Children {

		// Lookup nodes

		if c.Name == parser.NodeField {
			field := c.Runtime.(*fieldRuntime)

			if field.Name() == "__schema" {

				// We have an introspection query - handle this one in a special way

				res[field.Alias()] = field.SelectionSetRuntime().ProcessIntrospection()

			} else if field.SelectionSetRuntime() != nil {

				nodes := field.SelectionSetRuntime().ProcessNodes([]string{field.Alias()},
					field.Name(), field.Arguments(), nil)

				res[field.Alias()] = nodes
			}
		}
	}

	return res, err
}

/*
nodeIterator is an object which can iterate over nodes.
*/
type nodeIterator interface {
	Next() (string, string)
	HasNext() bool
	Error() error
}

/*
nodeKeyIteratorWrapper wraps around a normal node key iterator.
*/
type nodeKeyIteratorWrapper struct {
	kind string
	*graph.NodeKeyIterator
}

func (ni *nodeKeyIteratorWrapper) Next() (string, string) {
	return ni.NodeKeyIterator.Next(), ni.kind
}

/*
traversalIterator contains a traversal result.
*/
type traversalIterator struct {
	index    int
	nodeList []data.Node
}

func (ti *traversalIterator) Next() (string, string) {
	next := ti.nodeList[ti.index]
	ti.index++
	return next.Key(), next.Kind()
}

func (ti *traversalIterator) HasNext() bool {
	return ti.index < len(ti.nodeList)
}

func (ti *traversalIterator) Error() error {
	return nil
}

func (rt *selectionSetRuntime) checkArgs(path []string, args map[string]interface{}) {
	knownArgs := []string{"key", "matches", "traverse", "storeNode",
		"storeEdge", "removeNode", "removeEdge", "ascending", "descending",
		"from", "items", "last"}

	for arg := range args {
		if stringutil.IndexOf(arg, knownArgs) == -1 {
			rt.rtp.handleRuntimeError(fmt.Errorf("Unknown argument: %s", arg),
				path, rt.node)
		}
	}
}

/*
ProcessNodes uses the selection set to lookup/store nodes. Kind is not set during a
traversal.
*/
func (rt *selectionSetRuntime) ProcessNodes(path []string, kind string,
	args map[string]interface{}, it nodeIterator) []map[string]interface{} {

	var from, items, last int
	var ascending, descending string

	var err error

	res := make([]map[string]interface{}, 0)

	// Get only the attributes which were specified

	attrs, aliasMap, traversalMap := rt.GetPlainFieldsAndAliases(path, kind)

	addToRes := func(node data.Node) error {
		var err error

		r := make(map[string]interface{})

		for alias, attr := range aliasMap {

			if err == nil {
				if traversal, ok := traversalMap[alias]; ok {

					nodes, _, err := rt.rtp.gm.TraverseMulti(rt.rtp.part,
						node.Key(), node.Kind(), traversal.spec, false)

					if err == nil {

						data.NodeSort(nodes)

						r[alias] = traversal.selectionSetRuntime.ProcessNodes(
							append(path, traversal.spec), "", traversal.args,
							&traversalIterator{0, nodes})
					}

				} else {
					r[alias] = node.Attr(attr)
				}
			}
		}

		if err == nil {
			res = append(res, r)
		}

		return err
	}

	// Check arguments

	rt.checkArgs(path, args)

	if it == nil {
		err = rt.handleMutationArgs(path, args, kind)
	}

	if err == nil {

		ascending, descending, from, items, last, err = rt.handleOutputArgs(args)

		if err == nil {

			if key, ok := args["key"]; ok && it == nil {
				var node data.Node

				// Lookup a single node

				if node, err = rt.rtp.FetchNode(rt.rtp.part, fmt.Sprint(key), kind); err == nil && node != nil {
					addToRes(node)
				}

			} else {
				matchesRegexMap := make(map[string]*regexp.Regexp)
				matchAttrs := make([]string, 0)

				// Handle matches expression

				matches, matchesOk := args["matches"]
				matchesMap, matchesMapOk := matches.(map[string]interface{})

				if matchesOk {
					if matchesMapOk {
						for k, v := range matchesMap {
							matchAttrs = append(matchAttrs, k)

							if re, rerr := regexp.Compile(fmt.Sprint(v)); rerr == nil {
								matchesRegexMap[k] = re
							} else {
								rt.rtp.handleRuntimeError(fmt.Errorf("Regex %s did not compile: %s", v, rerr.Error()),
									path, rt.node)
							}
						}

					} else {
						rt.rtp.handleRuntimeError(fmt.Errorf("Matches expression is not a map"),
							path, rt.node)
					}
				}

				// Lookup a list of nodes

				if it == nil {
					var kit *graph.NodeKeyIterator
					kit, err = rt.rtp.gm.NodeKeyIterator(rt.rtp.part, kind)
					if kit != nil {
						it = &nodeKeyIteratorWrapper{kind, kit}
					}
				}

				if it != nil && err == nil {

					for err == nil && it.HasNext() {
						var node data.Node

						if err = it.Error(); err == nil {
							nkey, nkind := it.Next()

							if kind == "" {

								// If the kind is not fixed we need to reevaluate the attributes
								// to query for every node

								attrs, aliasMap, traversalMap = rt.GetPlainFieldsAndAliases(path, nkind)
							}

							if node, err = rt.rtp.FetchNodePart(rt.rtp.part, nkey,
								nkind, append(attrs, matchAttrs...)); err == nil && node != nil {

								if matchesOk && !rt.matchNode(node, matchesRegexMap) {
									continue
								}

								err = addToRes(node)
							}
						}
					}
				}
			}

			// Check if the result should be sorted

			if err == nil {

				if _, aok := args["ascending"]; aok {
					dataSort(res, ascending, true)
				} else if _, dok := args["descending"]; dok {
					dataSort(res, descending, false)
				}
			}

			// Check if the result should be truncated

			if last > 0 && last < len(res) {
				res = res[len(res)-last:]
			}

			if from > 0 || items > 0 {
				if from >= len(res) {
					from = 0
				}
				if from+items > len(res) {
					res = res[from:]
				} else {
					res = res[from : from+items]
				}
			}
		}
	}

	rt.rtp.handleRuntimeError(err, path, rt.node)

	return res
}

/*
handleOutputModifyingArgs handles arguments which modify the output presentation.
*/
func (rt *selectionSetRuntime) handleOutputArgs(args map[string]interface{}) (string, string, int, int, int, error) {
	var from, items, last int
	var ascending, descending string
	var err error

	ascendingData, aok := args["ascending"]
	descendingData, dok := args["descending"]

	if aok && dok {
		err = fmt.Errorf("Cannot specify ascending and descending sorting")
	} else if aok {
		ascending = fmt.Sprint(ascendingData)
	} else {
		descending = fmt.Sprint(descendingData)
	}

	if err == nil {
		if lastText, ok := args["last"]; ok {
			last, err = strconv.Atoi(fmt.Sprint(lastText))
		}
	}

	if err == nil {
		if fromText, ok := args["from"]; ok {
			from, err = strconv.Atoi(fmt.Sprint(fromText))
		}
	}

	if err == nil {
		if itemsText, ok := args["items"]; ok {
			items, err = strconv.Atoi(fmt.Sprint(itemsText))
		}
	}

	return ascending, descending, from, items, last, err
}

/*
handleMutationArgs handles node and edge insertion and removal.
*/
func (rt *selectionSetRuntime) handleMutationArgs(path []string, args map[string]interface{}, kind string) error {
	var err error

	if toStore, ok := args["storeNode"]; ok && rt.rtp.CheckWritePermission(path, rt.node) {

		toStoreMap, ok := toStore.(map[string]interface{})

		if ok {
			//  Handle mutations of nodes

			node := data.NewGraphNodeFromMap(toStoreMap)
			if node.Kind() == "" {
				node.SetAttr("kind", kind)
			}
			err = rt.rtp.gm.StoreNode(rt.rtp.part, node)

		} else {

			rt.rtp.handleRuntimeError(fmt.Errorf("Object required for node attributes and values"),
				path, rt.node)
		}
	}

	if toRemove, ok := args["removeNode"]; ok && rt.rtp.CheckWritePermission(path, rt.node) {

		toRemoveMap, ok := toRemove.(map[string]interface{})

		if ok {
			//  Handle removal of nodes

			node := data.NewGraphNodeFromMap(toRemoveMap)
			if node.Kind() == "" {
				node.SetAttr("kind", kind)
			}

			if node.Key() == "" {
				var it *graph.NodeKeyIterator

				if it, err = rt.rtp.gm.NodeKeyIterator(rt.rtp.part, node.Kind()); err == nil {
					var keys []string

					for it.HasNext() && err == nil {
						keys = append(keys, it.Next())
						err = it.Error()
					}

					if err == nil {
						for _, key := range keys {
							if err == nil {
								_, err = rt.rtp.gm.RemoveNode(rt.rtp.part, key, node.Kind())
							}
						}
					}
				}

			} else {
				_, err = rt.rtp.gm.RemoveNode(rt.rtp.part, node.Key(), node.Kind())
			}

		} else {

			rt.rtp.handleRuntimeError(fmt.Errorf("Object required for node key and kind"),
				path, rt.node)
		}
	}

	if toStore, ok := args["storeEdge"]; err == nil && ok && rt.rtp.CheckWritePermission(path, rt.node) {

		toStoreMap, ok := toStore.(map[string]interface{})

		if ok {
			//  Handle mutations of edges

			node := data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(toStoreMap))
			err = rt.rtp.gm.StoreEdge(rt.rtp.part, node)

		} else {

			rt.rtp.handleRuntimeError(fmt.Errorf("Object required for edge attributes and values"),
				path, rt.node)
		}
	}

	if toRemove, ok := args["removeEdge"]; err == nil && ok && rt.rtp.CheckWritePermission(path, rt.node) {

		toRemoveMap, ok := toRemove.(map[string]interface{})

		if ok {
			//  Handle mutations of edges

			node := data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(toRemoveMap))
			_, err = rt.rtp.gm.RemoveEdge(rt.rtp.part, node.Key(), node.Kind())

		} else {

			rt.rtp.handleRuntimeError(fmt.Errorf("Object required for edge key and kind"),
				path, rt.node)
		}
	}

	return err
}

/*
matchNode matches a given node against a given node template. Returns true if
the template matches, false otherwise.
*/
func (rt *selectionSetRuntime) matchNode(node data.Node, nodeTemplate map[string]*regexp.Regexp) bool {
	nodeData := node.Data()

	for k, v := range nodeTemplate {

		// Check if the match query should be negated

		negate := false
		if strings.HasPrefix(k, "not_") {
			k = k[4:]
			negate = true
		}

		mapAttr, ok := nodeData[k]
		if !ok {
			return false // Attribute does not exist
		}

		if negate {
			if v.MatchString(fmt.Sprint(mapAttr)) {
				return false // Attribute is the same
			}
		} else {
			if !v.MatchString(fmt.Sprint(mapAttr)) {
				return false // Attribute is not the same
			}
		}
	}

	return true
}

/*
traversal captures all required data for a traversal during node lookup.
*/
type traversal struct {
	spec                string
	args                map[string]interface{}
	selectionSetRuntime *selectionSetRuntime
}

/*
fragmentRuntime is the common interface for all fragment runtimes.
*/
type fragmentRuntime interface {
	TypeCondition() string
	SelectionSet() *parser.ASTNode
}

/*
GetPlainFieldsAndAliases returns all fields as a list of node attributes, a map of
aliases to names and a map from aliases to traversals.
*/
func (rt *selectionSetRuntime) GetPlainFieldsAndAliases(path []string, kind string) (
	[]string, map[string]string, map[string]*traversal) {

	errMultiFields := make([]string, 0)
	resList := []string{"key", "kind"}
	resMap := make(map[string]string)
	traversalMap := make(map[string]*traversal)

	fieldList := append(rt.node.Children[:0:0], rt.node.Children...) // Copy into new slice

	for i := 0; i < len(fieldList); i++ {
		var lastChild *parser.ASTNode

		c := fieldList[i]

		if len(c.Children) > 0 {
			lastChild = c.Children[len(c.Children)-1]
		}

		// Check for skip and include directive

		if rt.skipField(path, c) {
			continue
		}

		if c.Name == parser.NodeField {

			// Handle simple fields

			field := c.Runtime.(*fieldRuntime)

			if _, ok := resMap[field.Alias()]; ok {

				// Alias was used before

				if stringutil.IndexOf(field.Alias(), errMultiFields) == -1 {
					errMultiFields = append(errMultiFields, field.Alias())
				}

				continue
			}

			// Map alias to name and process the field

			resMap[field.Alias()] = field.Name()

			if lastChild.Name == parser.NodeSelectionSet {
				args := field.Arguments()

				// Handle traversals

				if spec, ok := args["traverse"]; ok {

					traversalMap[field.Alias()] = &traversal{
						spec:                fmt.Sprint(spec),
						args:                args,
						selectionSetRuntime: field.SelectionSetRuntime(),
					}

				} else {
					rt.rtp.handleRuntimeError(fmt.Errorf(
						"Traversal argument is missing"), path, c)
				}

			} else if stringutil.IndexOf(field.Name(), resList) == -1 {

				// Handle normal attribute lookup

				resList = append(resList, field.Name())
			}

		} else if c.Name == parser.NodeFragmentSpread || c.Name == parser.NodeInlineFragment {
			var fd fragmentRuntime

			if c.Name == parser.NodeFragmentSpread {

				// Lookup fragment spreads

				fd = rt.rtp.fragments[c.Token.Val]

			} else {

				// Construct inline fragments

				fd = c.Runtime.(*inlineFragmentDefinitionRuntime)
			}

			if fd.TypeCondition() != kind {

				// Type condition was not met - just skip the fragment

				continue
			}

			ss := fd.SelectionSet()
			fieldList = append(fieldList, ss.Children...)
		}
	}

	if len(errMultiFields) > 0 {
		for _, name := range errMultiFields {
			rt.rtp.handleRuntimeError(fmt.Errorf(
				"Field identifier %s used multiple times", name),
				path, rt.node)
		}
	}

	return resList, resMap, traversalMap
}

/*
skipField checks if a given field has a skip or include directive and returns
if the directive excludes the field.
*/
func (rt *selectionSetRuntime) skipField(path []string, node *parser.ASTNode) bool {

	for _, c := range node.Children {
		if c.Name == parser.NodeDirectives {

			for _, directive := range c.Children {
				rt := directive.Runtime.(*argumentExpressionRuntime)
				name := rt.Name()
				args := rt.Arguments()

				if name == "skip" || name == "include" {
					if cond, ok := args["if"]; ok {

						if name == "skip" {
							skip, _ := strconv.ParseBool(fmt.Sprint(cond))
							return skip
						}

						include, _ := strconv.ParseBool(fmt.Sprint(cond))
						return !include
					}

					rt.rtp.handleRuntimeError(fmt.Errorf(
						"Directive %s is missing the 'if' argument", name), path, c)
				}
			}
		}
	}

	return false
}

// ArgumentExpression Runtime
// ==========================

/*
Runtime for expressions with arguments.
*/
type argumentExpressionRuntime struct {
	*invalidRuntime
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
argumentExpressionRuntimeInst returns a new runtime component instance.
*/
func argumentExpressionRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &argumentExpressionRuntime{&invalidRuntime{rtp, node}, rtp, node}
}

/*
Name returns the name of this field.
*/
func (rt *argumentExpressionRuntime) Name() string {
	if rt.node.Children[0].Name == parser.NodeAlias {
		return rt.node.Children[1].Token.Val
	}
	return rt.node.Children[0].Token.Val
}

/*
Arguments returns all arguments of the field as a map.
*/
func (rt *argumentExpressionRuntime) Arguments() map[string]interface{} {
	res := make(map[string]interface{})
	for _, c := range rt.node.Children {

		if c.Name == parser.NodeArguments {

			for _, a := range c.Children {
				res[a.Children[0].Token.Val] = a.Children[1].Runtime.(*valueRuntime).Value()
			}
		}
	}

	return res
}

// Field Runtime
// =============

/*
Runtime for Fields.
*/
type fieldRuntime struct {
	*argumentExpressionRuntime
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
fieldRuntimeInst returns a new runtime component instance.
*/
func fieldRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &fieldRuntime{&argumentExpressionRuntime{&invalidRuntime{rtp, node},
		rtp, node}, rtp, node}
}

/*
Alias returns the alias of this field.
*/
func (rt *fieldRuntime) Alias() string {
	return rt.node.Children[0].Token.Val
}

/*
SelectionSetRuntime returns the SelectionSet runtime of this field.
*/
func (rt *fieldRuntime) SelectionSetRuntime() *selectionSetRuntime {
	res, _ := rt.node.Children[len(rt.node.Children)-1].Runtime.(*selectionSetRuntime)
	return res
}
