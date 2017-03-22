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

func TestDateFunctions(t *testing.T) {
	gm, _ := dateGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if _, err := getResult("get datetest", `
Labels: Datetest Key, Rfc3339 Value, Naive Value, Datetest Name, Unix
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:RFC3339_value, 1:n:naive_value, 1:n:name, 1:n:unix
000, 2012-10-09T19:00:55Z, 2012-10-09, date1, 1349809255
001, 2012-10-12T19:00:55+02:00, 2012-10-12, date2, 1350061255
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	// Test default conversion to RFC3339

	if _, err := getResult("get datetest where @parseDate(RFC3339_value) = unix", `
Labels: Datetest Key, Rfc3339 Value, Naive Value, Datetest Name, Unix
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:RFC3339_value, 1:n:naive_value, 1:n:name, 1:n:unix
000, 2012-10-09T19:00:55Z, 2012-10-09, date1, 1349809255
001, 2012-10-12T19:00:55+02:00, 2012-10-12, date2, 1350061255
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	// The format string '2006-01-02' interprets <year>-<month>-<day>
	// The string 2012-10-12 is interpreted as 2012-10-12 00:00:00 +0000 UTC

	if _, err := getResult("get datetest where @parseDate(naive_value, '2006-01-02') = 1350000000", `
Labels: Datetest Key, Rfc3339 Value, Naive Value, Datetest Name, Unix
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:RFC3339_value, 1:n:naive_value, 1:n:name, 1:n:unix
001, 2012-10-12T19:00:55+02:00, 2012-10-12, date2, 1350061255
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}
}

func TestFunctionErrors(t *testing.T) {
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
		"EQL error in test: Invalid construct (Count function requires 2 parameters: traversal step, traversal spec) (Line:1 Pos:21)" {
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

	if _, err := getResult("get Author show @objget(1)", "", rt, true); err.Error() !=
		"EQL error in test: Invalid construct (Objget function requires 3 parameters: traversal step, attribute name, path to value) (Line:1 Pos:17)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author where @parseDate()", "", rt, true); err.Error() !=
		"EQL error in test: Invalid construct (parseDate function requires 1 parameter: date string) (Line:1 Pos:18)" {
		t.Error(err)
		return
	}
}
