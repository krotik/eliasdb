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

import "testing"

func TestFunctions(t *testing.T) {
	gm, _ := songGraphGroups()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if _, err := getResult("get group traverse ::: end show key, @count(2, :::Author)", `
Labels: Group Key, Count
Format: auto, auto
Data: 1:n:key, 2:func:count()
Best, 1
Best, 1
Best, 1
Best, 1
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author show name, @count(1, :::Song) AS mycount format xxx", `
Labels: Author Name, mycount
Format: auto, xxx
Data: 1:n:name, 1:func:count()
Hans, 1
John, 4
Mike, 4
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	// Test parsing and runtime error

	if _, err := getResult("get group show key, @unknownfunction(:::Author)", "", rt, true); err.Error() !=
		"EQL error in test: Invalid construct (Unknown function: unknownfunction) (Line:1 Pos:21)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get group show key, @count(:::Author)", "", rt, true); err.Error() !=
		"EQL error in test: Invalid construct (Count function requires 2 parameters: data index, traversal spec) (Line:1 Pos:21)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get group show key, @count(1, ::Author)", "", rt, true); err.Error() !=
		"GraphError: Invalid data (Invalid spec: ::Author)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get group show key, @count(99, ::Author)", "", rt, true); err.Error() !=
		"EQL error in test: Invalid column data spec (Data index out of range: 99) (Line:1 Pos:21)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author where @count(:::Song) > 3 show name, @count(1, :::Song)", `
Labels: Author Name, Count
Format: auto, auto
Data: 1:n:name, 1:func:count()
John, 4
Mike, 4
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	// Test parsing and runtime error

	if _, err := getResult("get Author where @unknownfunction() > 3", "", rt, true); err.Error() !=
		"EQL error in test: Invalid construct (Unknown function: unknownfunction) (Line:1 Pos:18)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author where @count() > 3", "", rt, true); err.Error() !=
		"EQL error in test: Invalid construct (Count function requires 1 parameter: traversal spec) (Line:1 Pos:18)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author where @count(::Song) > 3", "", rt, true); err.Error() !=
		"GraphError: Invalid data (Invalid spec: ::Song)" {
		t.Error(err)
		return
	}
}
