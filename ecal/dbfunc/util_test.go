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
	"testing"

	"devt.de/krotik/ecal/interpreter"
	"devt.de/krotik/ecal/parser"
	"devt.de/krotik/ecal/util"
	"devt.de/krotik/eliasdb/graph"
)

func TestRaiseGraphEventHandled(t *testing.T) {

	f := &RaiseGraphEventHandledFunc{}

	if _, err := f.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Run("", nil, nil, 0, []interface{}{}); err != graph.ErrEventHandled {
		t.Error("Unexpected result:", err)
		return
	}
}

func TestRaiseWebEventHandled(t *testing.T) {

	f := &RaiseWebEventHandledFunc{}

	if _, err := f.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := f.Run("", nil, nil, 0, []interface{}{}); err == nil ||
		err.Error() != "Function requires 1 parameter: request response object" {
		t.Error(err)
		return
	}

	if _, err := f.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Request response object should be a map" {
		t.Error(err)
		return
	}

	astnode, _ := parser.ASTFromJSONObject(map[string]interface{}{
		"name": "foo",
	})

	_, err := f.Run("", nil, map[string]interface{}{
		"erp":     interpreter.NewECALRuntimeProvider("", nil, nil),
		"astnode": astnode,
	}, 0, []interface{}{map[interface{}]interface{}{}})

	if err.(*util.RuntimeErrorWithDetail).Type != ErrWebEventHandled {
		t.Error("Unexpected result:", err)
		return
	}
}
