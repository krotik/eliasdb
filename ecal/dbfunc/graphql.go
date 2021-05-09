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

	"devt.de/krotik/ecal/parser"
	"devt.de/krotik/ecal/scope"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graphql"
)

/*
GraphQLFunc runs a GraphQL query.
*/
type GraphQLFunc struct {
	GM *graph.Manager
}

/*
Run executes the ECAL function.
*/
func (f *GraphQLFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	var err error
	var ret interface{}

	if arglen := len(args); arglen < 2 {
		err = fmt.Errorf("Function requires at least 2 parameters: partition and query with optionally a map of variables and an operation name")
	}

	if err == nil {
		var res, varMap map[string]interface{}

		part := fmt.Sprint(args[0])
		query := fmt.Sprint(args[1])
		opname := ""

		if err == nil && len(args) > 2 {
			varECALMap, ok := args[2].(map[interface{}]interface{})

			if !ok {
				err = fmt.Errorf("Third parameter must be a map")
			} else {
				varMap = make(map[string]interface{})
				for k, v := range varECALMap {
					varMap[fmt.Sprint(k)] = v
				}
			}
		}

		if err == nil && len(args) > 3 {
			opname = fmt.Sprint(args[3])
		}

		if err == nil {
			res, err = graphql.RunQuery("db.query", part, map[string]interface{}{
				"operationName": opname,
				"query":         query,
				"variables":     varMap,
			}, f.GM, nil, false)

			if err == nil {
				ret = scope.ConvertJSONToECALObject(res)
			}
		}
	}

	return ret, err
}

/*
DocString returns a descriptive string.
*/
func (f *GraphQLFunc) DocString() (string, error) {
	return "Run a GraphQL query.", nil
}
