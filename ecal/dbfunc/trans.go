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
	"strconv"

	"devt.de/krotik/ecal/parser"
	"devt.de/krotik/eliasdb/graph"
)

/*
NewTransFunc creates a new transaction for EliasDB.
*/
type NewTransFunc struct {
	GM *graph.Manager
}

/*
Run executes the ECAL function.
*/
func (f *NewTransFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	var err error

	if len(args) != 0 {
		err = fmt.Errorf("Function does not require any parameters")
	}

	return graph.NewConcurrentGraphTrans(f.GM), err
}

/*
DocString returns a descriptive string.
*/
func (f *NewTransFunc) DocString() (string, error) {
	return "Creates a new transaction for EliasDB.", nil
}

/*
NewRollingTransFunc creates a new rolling transaction for EliasDB.
A rolling transaction commits after n entries.
*/
type NewRollingTransFunc struct {
	GM *graph.Manager
}

/*
Run executes the ECAL function.
*/
func (f *NewRollingTransFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	var err error
	var trans graph.Trans

	if arglen := len(args); arglen != 1 {
		err = fmt.Errorf(
			"Function requires the rolling threshold (number of operations before rolling)")
	}

	if err == nil {
		var i int

		if i, err = strconv.Atoi(fmt.Sprint(args[0])); err != nil {
			err = fmt.Errorf("Rolling threshold must be a number not: %v", args[0])
		} else {
			trans = graph.NewRollingTrans(graph.NewConcurrentGraphTrans(f.GM),
				i, f.GM, graph.NewConcurrentGraphTrans)
		}
	}

	return trans, err
}

/*
DocString returns a descriptive string.
*/
func (f *NewRollingTransFunc) DocString() (string, error) {
	return "Creates a new rolling transaction for EliasDB. A rolling transaction commits after n entries.", nil
}

/*
CommitTransFunc commits an existing transaction for EliasDB.
*/
type CommitTransFunc struct {
	GM *graph.Manager
}

/*
Run executes the ECAL function.
*/
func (f *CommitTransFunc) Run(instanceID string, vs parser.Scope, is map[string]interface{}, tid uint64, args []interface{}) (interface{}, error) {
	var err error

	if arglen := len(args); arglen != 1 {
		err = fmt.Errorf(
			"Function requires the transaction to commit as parameter")
	}

	if err == nil {
		trans, ok := args[0].(graph.Trans)

		// Check parameters

		if !ok {
			err = fmt.Errorf("Parameter must be a transaction")
		} else {
			err = trans.Commit()
		}
	}

	return nil, err
}

/*
DocString returns a descriptive string.
*/
func (f *CommitTransFunc) DocString() (string, error) {
	return "Commits an existing transaction for EliasDB.", nil
}
