/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dbfunc

import (
	"fmt"

	"devt.de/krotik/ecal/interpreter"
	"devt.de/krotik/ecal/parser"
	"devt.de/krotik/ecal/util"
	"devt.de/krotik/eliasdb/graph"
)

/*
RaiseGraphEventHandledFunc returns the special graph.ErrEventHandled error which a sink,
handling graph events, can return to notify the GraphManager that no further
action is necessary.
*/
type RaiseGraphEventHandledFunc struct {
}

/*
Run executes the ECAL function.
*/
func (f *RaiseGraphEventHandledFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	return nil, graph.ErrEventHandled
}

/*
DocString returns a descriptive string.
*/
func (f *RaiseGraphEventHandledFunc) DocString() (string, error) {
	return "When handling a graph event, notify the GraphManager of EliasDB that no further action is necessary.", nil
}

/*
ErrWebEventHandled is a special error to signal that a web request was handled.
*/
var ErrWebEventHandled = fmt.Errorf("Web event handled")

/*
RaiseWebEventHandledFunc returns a special error which a sink can return to notify
the web API that a web request was handled.
*/
type RaiseWebEventHandledFunc struct {
}

/*
Run executes the ECAL function.
*/
func (f *RaiseWebEventHandledFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	if arglen := len(args); arglen != 1 {
		return nil, fmt.Errorf("Function requires 1 parameter: request response object")
	}

	res := args[0]

	resMap, ok := res.(map[interface{}]interface{})

	if !ok {
		return nil, fmt.Errorf("Request response object should be a map")
	}

	if _, ok := resMap["status"]; !ok {
		resMap["status"] = 200
	}
	if _, ok := resMap["headers"]; !ok {
		resMap["header"] = map[interface{}]interface{}{
			"Content-Type":           "application/json; charset=utf-8",
			"X-Content-Type-Options": "nosniff",
		}
	}
	if _, ok := resMap["body"]; !ok {
		resMap["body"] = map[interface{}]interface{}{}
	}

	erp := is["erp"].(*interpreter.ECALRuntimeProvider)
	node := is["astnode"].(*parser.ASTNode)

	return nil, &util.RuntimeErrorWithDetail{
		RuntimeError: erp.NewRuntimeError(ErrWebEventHandled, "", node).(*util.RuntimeError),
		Environment:  vs,
		Data:         res,
	}
}

/*
DocString returns a descriptive string.
*/
func (f *RaiseWebEventHandledFunc) DocString() (string, error) {
	return "When handling a web event, notify the web API of EliasDB that the web request was handled.", nil
}
