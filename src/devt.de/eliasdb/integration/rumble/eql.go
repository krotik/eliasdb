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

	"devt.de/common/defs/rumble"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/eql"
)

// Function: query
// ===============

/*
QueryFunc runs an EQL query.
*/
type QueryFunc struct {
}

/*
Name returns the name of the function.
*/
func (f *QueryFunc) Name() string {
	return "db.query"
}

/*
Validate is called for parameter validation and to reset the function state.
*/
func (f *QueryFunc) Validate(argsNum int, rt rumble.Runtime) rumble.RuntimeError {
	var err rumble.RuntimeError

	if argsNum != 2 {
		err = rt.NewRuntimeError(rumble.ErrInvalidConstruct,
			"Function query requires 2 parameters: partition and a query string")
	}

	return err
}

/*
Execute executes the rumble function.
*/
func (f *QueryFunc) Execute(argsVal []interface{}, vars rumble.Variables,
	rt rumble.Runtime) (interface{}, rumble.RuntimeError) {

	part := fmt.Sprint(argsVal[0])
	query := fmt.Sprint(argsVal[1])

	res, err := eql.RunQuery("db.query", part, query, api.GM)

	if err != nil {

		// Wrap error message in RuntimeError

		return nil, rt.NewRuntimeError(rumble.ErrInvalidState,
			fmt.Sprintf(err.Error()))
	}

	// Convert result to rumble data structure

	labels := res.Header().Labels()
	cols := make([]interface{}, len(labels))
	for i, v := range labels {
		cols[i] = v
	}

	rrows := res.Rows()
	rows := make([]interface{}, len(rrows))
	for i, v := range rrows {
		rows[i] = v
	}

	return map[interface{}]interface{}{
		"cols": cols,
		"rows": rows,
	}, err
}
