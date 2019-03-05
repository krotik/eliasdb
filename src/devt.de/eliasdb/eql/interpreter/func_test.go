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

	if _, err := getResult("get datetest where @parseDate(naive_value, '2006-01-02') > @parseDate('2012-10-11', '2006-01-02')", `
Labels: Datetest Key, Rfc3339 Value, Naive Value, Datetest Name, Unix
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:RFC3339_value, 1:n:naive_value, 1:n:name, 1:n:unix
001, 2012-10-12T19:00:55+02:00, 2012-10-12, date2, 1350061255
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}
}

func TestCountFunctions(t *testing.T) {
	gm, _ := songGraphGroups()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))
	rt2 := NewLookupRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if _, err := getResult("get Author traverse :::Song end show name, 2:n:name", `
Labels: Author Name, Name
Format: auto, auto
Data: 1:n:name, 2:n:name
Hans, MyOnlySong3
John, Aria1
John, Aria2
John, Aria3
John, Aria4
Mike, DeadSong2
Mike, FightSong4
Mike, LoveSong3
Mike, StrangeSong1
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

	if _, err := getResult("get Author traverse :::Song where (name beginswith 'A') or name beginswith 'L' end show key, name, 2:n:name", `
Labels: Author Key, Author Name, Name
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:name
000, John, Aria1
000, John, Aria2
000, John, Aria3
000, John, Aria4
123, Mike, LoveSong3
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	// Make sure for the source of the count in row 2 (John, 4) is:
	// q:lookup Author "000" traverse :::Song where name beginswith A or name beginswith L end show 2:n:key, 2:n:kind, 2:n:name

	if res, err := getResult("get Author show name, @count(1, :::Song, r\"(name beginswith A) or name beginswith 'L' TRAVERSE ::: END\") AS mycount format xxx", `
Labels: Author Name, mycount
Format: auto, xxx
Data: 1:n:name, 1:func:count()
Hans, 0
John, 4
Mike, 1
`[1:], rt, true); err != nil || res.RowSource(1)[1] != `q:lookup Author "000" traverse :::Song where name beginswith A or name beginswith L end show 2:n:key, 2:n:kind, 2:n:name` {
		t.Error(res.RowSource(1)[1], err)
		return
	}

	// Make sure the source query has the expected result (the source nodes for the count of 4)

	if _, err := getResult(`lookup Author "000" traverse :::Song where name beginswith A or name beginswith L end show 2:n:key, 2:n:kind, 2:n:name`, `
Labels: Key, Kind, Name
Format: auto, auto, auto
Data: 2:n:key, 2:n:kind, 2:n:name
Aria1, Song, Aria1
Aria2, Song, Aria2
Aria3, Song, Aria3
Aria4, Song, Aria4
`[1:], rt2, true); err != nil {
		t.Error(err)
		return
	}

	// Use the count feature in the where clause - get all authors who have only one song beginning with M or L

	if _, err := getResult("get Author where @count(:::Song, \"(name beginswith 'M') or name beginswith 'L'\") = 1 show key, name, @count(1, :::Song, \"(name beginswith 'M') or name beginswith 'L'\")", `
Labels: Author Key, Author Name, Count
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:func:count()
123, Mike, 1
456, Hans, 1
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}
}

func TestFunctionErrors(t *testing.T) {
	gm, _ := songGraphGroups()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	// Test parsing and runtime error

	if _, err := getResult("get group show key, @unknownfunction(:::Author)", "", rt, true); err.Error() !=
		"EQL error in test: Invalid construct (Unknown function: unknownfunction) (Line:1 Pos:21)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get group show key, @count(:::Author)", "", rt, true); err.Error() !=
		"EQL error in test: Invalid construct (Count function requires 2 or 3 parameters: traversal step, traversal spec, condition clause) (Line:1 Pos:21)" {
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
		"EQL error in test: Invalid construct (Count function requires 1 or 2 parameters: traversal spec, condition clause) (Line:1 Pos:18)" {
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

	if _, err := getResult("get Author where @count(:::Song, \"name\")", "", rt, true); err == nil || err.Error() !=
		"EQL error in test: Invalid construct (Could not evaluate condition clause in count function) (Line:1 Pos:18)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author where @count(:::Song, \"name =\")", "", rt, true); err == nil || err.Error() !=
		"EQL error in test: Invalid construct (Invalid condition clause in count function: Parse error in count condition: Unexpected end) (Line:1 Pos:18)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author where @count(:::Song, \"show\") = 1", "", rt, true); err == nil || err.Error() !=
		"EQL error in test: Invalid construct (Invalid condition clause in count function: EQL error in test: Invalid construct (show) (Line:1 Pos:13)) (Line:1 Pos:18)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author show @count(1, :::Song, \"name\")", "", rt, true); err == nil || err.Error() !=
		"EQL error in test: Invalid construct (Could not evaluate condition clause in count function) (Line:1 Pos:17)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author show @count(1, :::Song, \"name =\")", "", rt, true); err == nil || err.Error() !=
		"EQL error in test: Invalid construct (Invalid condition clause in count function: Parse error in count condition: Unexpected end) (Line:1 Pos:17)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author show @count(1, :::Song, \"show\")", "", rt, true); err == nil || err.Error() !=
		"EQL error in test: Invalid construct (show) (Line:1 Pos:13)" {
		t.Error(err)
		return
	}
}
