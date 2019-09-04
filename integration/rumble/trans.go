/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package rumble

import (
	"fmt"
	"strconv"

	"devt.de/krotik/common/defs/rumble"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/graph"
)

// Function: newTrans
// ==================

/*
NewTransFunc creates a new transaction for EliasDB.
*/
type NewTransFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *NewTransFunc) Name() string {
	return "db.newTrans"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *NewTransFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 0 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function newTrans does not require any parameters")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *NewTransFunc) Execute(argsVal []interface{}, vars rumble.Variables,
	rt rumble.Runtime) (interface{}, rumble.RuntimeError) {

	return graph.NewConcurrentGraphTrans(api.GM), nil
}

// Function: newRollingTrans
// =========================

/*
NewRollingTransFunc creates a new rolling transaction for EliasDB.
A rolling transaction commits after n entries.
*/
type NewRollingTransFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *NewRollingTransFunc) Name() string {
	return "db.newRollingTrans"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *NewRollingTransFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 1 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function newRollingTrans requires the rolling threshold (number of operations before rolling)")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *NewRollingTransFunc) Execute(argsVal []interface{}, vars rumble.Variables,
	rt rumble.Runtime) (interface{}, rumble.RuntimeError) {

	i, err := strconv.Atoi(fmt.Sprint(argsVal[0]))

	if err != nil {
		return nil, rt.NewRuntimeError(rumble.ErrNotANumber,
			fmt.Sprintf("Rolling threshold must be a number not: %v", argsVal[0]))
	}

	trans := graph.NewRollingTrans(graph.NewConcurrentGraphTrans(api.GM),
		i, api.GM, graph.NewConcurrentGraphTrans)

	return trans, nil
}

// Function: commitTrans
// =====================

/*
CommitTransFunc commits an existing transaction for EliasDB.
*/
type CommitTransFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *CommitTransFunc) Name() string {
	return "db.commitTrans"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *CommitTransFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 1 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function commitTrans	 requires the transaction to commit as parameter")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *CommitTransFunc) Execute(argsVal []interface{}, vars rumble.Variables,
	rt rumble.Runtime) (interface{}, rumble.RuntimeError) {

	var err rumble.RuntimeError

	trans, ok := argsVal[0].(graph.Trans)

	// Check parameters

	if !ok {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Parameter must be a transaction")
	}

	if err == nil {
		if err = trans.Commit(); err != nil {
			err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
				fmt.Sprintf("Cannot store node: %v", err.Error()))
		}
	}

	return nil, err
}
