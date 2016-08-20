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
	"testing"

	"devt.de/eliasdb/eql/parser"
)

func TestHelperRuntime(t *testing.T) {
	gm, _ := simpleGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, &testNodeInfo{&defaultNodeInfo{gm}})

	// Test simple value runtime

	ast, err := parser.ParseWithRuntime("test", "get mynode", rt)
	if err != nil {
		t.Error(err)
		return
	}

	if val, _ := ast.Children[0].Runtime.Eval(); val != "mynode" {
		t.Error("Unexpected eval result:", val)
		return
	}

	if err := ast.Children[0].Runtime.Validate(); err != err {
		t.Error(err)
		return
	}

	// Test not implemented runtime

	irt := invalidRuntimeInst(rt.eqlRuntimeProvider, ast.Children[0])

	if err := irt.Validate(); err.Error() != "EQL error in test: Invalid construct (value) (Line:1 Pos:5)" {
		t.Error("Unexpected validate result:", err)
		return
	}

	if _, err := irt.Eval(); err.Error() != "EQL error in test: Invalid construct (value) (Line:1 Pos:5)" {
		t.Error("Unexpected validate result:", err)
		return
	}
}
