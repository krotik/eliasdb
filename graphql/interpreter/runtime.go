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

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/lang/graphql/parser"
	"devt.de/krotik/eliasdb/graph"
)

// Runtime definition
// ==================

/*
Instance function for runtime components
*/
type runtimeInst func(*GraphQLRuntimeProvider, *parser.ASTNode) parser.Runtime

/*
Runtime map for runtime components
*/
var runtimeProviderMap = map[string]runtimeInst{
	parser.NodeEOF:                  invalidRuntimeInst,
	parser.NodeDocument:             documentRuntimeInst,
	parser.NodeExecutableDefinition: executableDefinitionRuntimeInst,
	parser.NodeFragmentDefinition:   fragmentDefinitionRuntimeInst,
	parser.NodeInlineFragment:       inlineFragmentDefinitionRuntimeInst,
	parser.NodeOperationDefinition:  operationDefinitionRuntimeInst,
	parser.NodeSelectionSet:         selectionSetRuntimeInst,
	parser.NodeField:                fieldRuntimeInst,
	parser.NodeDirective:            argumentExpressionRuntimeInst,

	parser.NodeObjectValue:  valueRuntimeInst,
	parser.NodeValue:        valueRuntimeInst,
	parser.NodeDefaultValue: valueRuntimeInst,
	parser.NodeEnumValue:    valueRuntimeInst,
	parser.NodeListValue:    valueRuntimeInst,
	parser.NodeVariable:     valueRuntimeInst,
}

// General runtime provider
// ========================

/*
QueryType is a know GraphQL query type
*/
type QueryType string

/*
All known query types
*/
const (
	QueryTypeQuery        QueryType = "query"
	QueryTypeMutation               = "mutation"
	QueryTypeSubscription           = "subscription"
)

/*
GraphQLRuntimeProvider defines the main interpreter
datastructure and all functions for general evaluation.
*/
type GraphQLRuntimeProvider struct {
	Name           string                 // Name to identify the input
	QueryType      QueryType              // Query type (query, mutation, subscription)
	OperationName  string                 // Name of operation to execute
	VariableValues map[string]interface{} // Values of variables
	ErrorKeys      []string               // List of error hashes (used for deduplication)
	Errors         []*RuntimeError        // List of errors
	ErrorPaths     [][]string             // List of error paths

	part                string                      // Graph partition to query
	gm                  *graph.Manager              // GraphManager to operate on
	callbackHandler     SubscriptionCallbackHandler // Subscription callback handler for updates
	subscriptionHandler *subscriptionHandler        // Subscription handler forwarding event is the callback object

	readOnly  bool                                  // Flag if only read operations are allowed
	operation *parser.ASTNode                       // Operation to execute
	fragments map[string]*fragmentDefinitionRuntime // Fragment definitions
}

/*
NewGraphQLRuntimeProvider creates a new GraphQLRuntimeProvider object.
*/
func NewGraphQLRuntimeProvider(name string, part string, gm *graph.Manager,
	op string, vars map[string]interface{}, callbackHandler SubscriptionCallbackHandler,
	readOnly bool) *GraphQLRuntimeProvider {

	return &GraphQLRuntimeProvider{name, "", op, vars, []string{}, []*RuntimeError{},
		[][]string{}, part, gm, callbackHandler, nil, readOnly, nil,
		make(map[string]*fragmentDefinitionRuntime)}
}

/*
CheckWritePermission checks if the current query is allowed to modify data.
Returns true if data can be modified.
*/
func (rtp *GraphQLRuntimeProvider) CheckWritePermission(path []string, node *parser.ASTNode) bool {

	if rtp.readOnly {
		rtp.handleRuntimeError(fmt.Errorf("Can only perform read operations"),
			path, node)
		return false
	}

	if rtp.QueryType != QueryTypeMutation {
		rtp.handleRuntimeError(fmt.Errorf("Operation must be a mutation to modify data"),
			path, node)
		return false
	}

	return true
}

/*
Initialise data structures.
*/
func (rtp *GraphQLRuntimeProvider) init() error {
	rtp.QueryType = ""
	rtp.operation = nil

	return nil
}

/*
Runtime returns a runtime component for a given ASTNode.
*/
func (rtp *GraphQLRuntimeProvider) Runtime(node *parser.ASTNode) parser.Runtime {
	if pinst, ok := runtimeProviderMap[node.Name]; ok {
		return pinst(rtp, node)
	}
	return invalidRuntimeInst(rtp, node)
}

/*
TraverseAST traverses the AST starting with a given root and executes a given
visitor function on each node. An accumulator is given to track state. A path
is given to track selection sets.
*/
func (rtp *GraphQLRuntimeProvider) TraverseAST(root *parser.ASTNode,
	visitor func(*parser.ASTNode)) {

	visitor(root)

	for _, child := range root.Children {
		rtp.TraverseAST(child, visitor)
	}
}

// Document Runtime
// ================

type documentRuntime struct {
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
documentRuntimeInst creates a new document runtime instance.
*/
func documentRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &documentRuntime{rtp, node}
}

/*
 Validate and reset this runtime component and all its child components.
*/
func (rt *documentRuntime) Validate() error {

	err := rt.rtp.init()

	for _, c := range rt.node.Children {
		if err == nil {
			err = c.Runtime.Validate()
		}
	}

	if rt.rtp.operation == nil {

		// We didn't find an operation to execute

		if rt.rtp.OperationName == "" {
			err = rt.rtp.newFatalRuntimeError(ErrMissingOperation,
				"No executable expression found", rt.node)
		} else {
			err = rt.rtp.newFatalRuntimeError(ErrMissingOperation,
				fmt.Sprintf("Operation %s not found", rt.rtp.OperationName), rt.node)
		}
	}

	if err == nil && rt.rtp.QueryType == "" {
		rt.rtp.QueryType = QueryTypeQuery
	}

	if err == nil {

		// Check variables - types are not checked

		ort := rt.rtp.operation.Runtime.(*operationDefinitionRuntime)

		declared, defaultValues, _ := ort.DeclaredVariables()

		// Build up variable values

		vals := rt.rtp.VariableValues
		rt.rtp.VariableValues = make(map[string]interface{})

		for _, name := range declared {
			val, ok := vals[name]
			if ok {
				rt.rtp.VariableValues[name] = val
			} else {
				rt.rtp.VariableValues[name] = defaultValues[name]
			}
		}
	}

	if err == nil {

		// Collect fragment definitions

		rt.rtp.TraverseAST(rt.node, func(n *parser.ASTNode) {

			if err == nil && n.Name == parser.NodeFragmentDefinition {
				fr := n.Runtime.(*fragmentDefinitionRuntime)

				if _, ok := rt.rtp.fragments[fr.Name()]; ok {
					err = rt.rtp.newFatalRuntimeError(ErrAmbiguousDefinition,
						fmt.Sprintf("Fragment %s defined multiple times",
							fr.Name()), rt.node)
				}

				if err == nil {
					rt.rtp.fragments[fr.Name()] = fr
				}
			}
		})

		if err == nil {

			// Validate that all fragment spreads can be resolved

			rt.rtp.TraverseAST(rt.node, func(n *parser.ASTNode) {

				if err == nil && n.Name == parser.NodeFragmentSpread {
					name := n.Token.Val

					if _, ok := rt.rtp.fragments[name]; !ok {
						err = rt.rtp.newFatalRuntimeError(ErrInvalidConstruct,
							fmt.Sprintf("Fragment %s is not defined",
								name), rt.node)
					}

				}
			})
		}
	}

	return err
}

/*
Eval evaluate this runtime component.
*/
func (rt *documentRuntime) Eval() (map[string]interface{}, error) {
	var err error

	// First validate the query and reset the runtime provider datastructures

	if rt.rtp.QueryType == "" {
		if err = rt.Validate(); err != nil {
			return nil, err
		}
	}

	// Validate must have found the query type and the operation to execute

	errorutil.AssertTrue(rt.rtp.QueryType != "", "Unknown querytype")
	errorutil.AssertTrue(rt.rtp.operation != nil, "Unknown operation")

	if rt.rtp.QueryType == QueryTypeSubscription && rt.rtp.callbackHandler != nil {

		rt.rtp.InitSubscription(rt)
	}

	return rt.rtp.operation.Runtime.Eval()
}

// ExecutableDefinition Runtime
// ============================

type executableDefinitionRuntime struct {
	*invalidRuntime
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
executableDefinitionRuntimeInst creates a new document runtime instance.
*/
func executableDefinitionRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &executableDefinitionRuntime{&invalidRuntime{rtp, node}, rtp, node}
}

/*
 Validate and reset this runtime component and all its child components.
*/
func (rt *executableDefinitionRuntime) Validate() error {

	if rt.rtp.operation == nil {

		// Choose an operation to execute

		if rt.node.Children[0].Name == parser.NodeOperationDefinition {

			if rt.rtp.OperationName == "" {

				// No operation name defined - take the first available operation

				rt.rtp.operation = rt.node.Children[0]

				// Check the operation type

				if rt.node.Children[0].Children[0].Name == parser.NodeOperationType {

					if rt.node.Children[0].Children[0].Token.Val == "mutation" {
						rt.rtp.QueryType = QueryTypeMutation
					} else if rt.node.Children[0].Children[0].Token.Val == "subscription" {
						rt.rtp.QueryType = QueryTypeSubscription
					}
				}

			} else {

				// If an operation name is defined we must not have a query shorthand

				if rt.node.Children[0].Children[0].Name == parser.NodeOperationType {

					name := rt.node.Children[0].Children[1].Token.Val

					if rt.rtp.OperationName == name {

						// We found the operation to execture

						if rt.node.Children[0].Children[0].Name == parser.NodeOperationType {

							// See what type it is

							if rt.node.Children[0].Children[0].Token.Val == "mutation" {
								rt.rtp.QueryType = QueryTypeMutation
							} else if rt.node.Children[0].Children[0].Token.Val == "subscription" {
								rt.rtp.QueryType = QueryTypeSubscription
							}
						}

						rt.rtp.operation = rt.node.Children[0]
					}
				}
			}
		}
	}

	return nil
}

// OperationDefinition Runtime
// ============================

type operationDefinitionRuntime struct {
	*invalidRuntime
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
operationDefinitionRuntimeInst creates a new operation definition runtime instance.
*/
func operationDefinitionRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &operationDefinitionRuntime{&invalidRuntime{rtp, node}, rtp, node}
}

/*
Eval evaluate this runtime component.
*/
func (rt *operationDefinitionRuntime) Eval() (map[string]interface{}, error) {
	res := make(map[string]interface{})

	// Execute the selection set

	data, err := rt.node.Children[len(rt.node.Children)-1].Runtime.Eval()

	res["data"] = data

	// Collect errors

	resErr := make([]map[string]interface{}, 0)

	for i, rterr := range rt.rtp.Errors {
		resErr = append(resErr, map[string]interface{}{
			"message": rterr.Detail,
			"locations": []map[string]interface{}{
				{
					"line":   rterr.Line,
					"column": rterr.Pos,
				},
			},
			"path": rt.rtp.ErrorPaths[i],
		})
	}

	if len(resErr) > 0 {

		// Only add errors if there are any (@spec 7.1.2)

		res["errors"] = resErr
	}

	return res, err
}

/*
DeclaredVariables returns all declared variables as list and their default
values (if defined) and their type as maps.
*/
func (rt *operationDefinitionRuntime) DeclaredVariables() ([]string, map[string]interface{}, map[string]string) {
	declared := make([]string, 0)
	defValues := make(map[string]interface{})
	types := make(map[string]string)

	for _, c := range rt.node.Children {

		if c.Name == parser.NodeVariableDefinitions {

			for _, vardef := range c.Children {
				name := vardef.Children[0].Token.Val
				declared = append(declared, name)

				if len(vardef.Children) > 2 {
					defValues[name] = vardef.Children[2].Runtime.(*valueRuntime).Value()
				}

				types[name] = vardef.Children[1].Token.Val
			}
		}
	}

	return declared, defValues, types
}

// FragmentDefinition Runtime
// ==========================

type fragmentDefinitionRuntime struct {
	*invalidRuntime
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
fragmentDefinitionRuntimeInst creates a new fragment definition runtime instance.
*/
func fragmentDefinitionRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &fragmentDefinitionRuntime{&invalidRuntime{rtp, node}, rtp, node}
}

/*
Name returns the name of the fragment definition.
*/
func (rt *fragmentDefinitionRuntime) Name() string {
	return rt.node.Children[0].Token.Val
}

/*
TypeCondition returns the type condition of the fragment definition.
*/
func (rt *fragmentDefinitionRuntime) TypeCondition() string {
	return rt.node.Children[1].Token.Val
}

/*
SelectionSet returns the selection set of the fragment definition.
*/
func (rt *fragmentDefinitionRuntime) SelectionSet() *parser.ASTNode {
	return rt.node.Children[len(rt.node.Children)-1]
}

// InlineFragmentDefinition Runtime
// ================================

type inlineFragmentDefinitionRuntime struct {
	*invalidRuntime
	rtp  *GraphQLRuntimeProvider
	node *parser.ASTNode
}

/*
fragmentDefinitionRuntimeInst creates a new inline fragment definition runtime instance.
*/
func inlineFragmentDefinitionRuntimeInst(rtp *GraphQLRuntimeProvider, node *parser.ASTNode) parser.Runtime {
	return &inlineFragmentDefinitionRuntime{&invalidRuntime{rtp, node}, rtp, node}
}

/*
TypeCondition returns the type condition of the inline fragment definition.
*/
func (rt *inlineFragmentDefinitionRuntime) TypeCondition() string {
	return rt.node.Children[0].Token.Val
}

/*
SelectionSet returns the selection set of the inline fragment definition.
*/
func (rt *inlineFragmentDefinitionRuntime) SelectionSet() *parser.ASTNode {
	return rt.node.Children[len(rt.node.Children)-1]
}
