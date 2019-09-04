/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package parser

import (
	"fmt"
	"testing"
)

func TestSimpleExpressionPrinting(t *testing.T) {

	input := "a + b * 5 /2-1"
	expectedOutput := `
minus
  plus
    value: "a"
    div
      times
        value: "b"
        value: "5"
      value: "2"
  value: "1"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"a + b * 5 / 2 - 1"); err != nil {
		t.Error(err)
		return
	}

	input = "(a + 1) * 5 / (6 - 2)"
	expectedOutput = `
div
  times
    plus
      value: "a"
      value: "1"
    value: "5"
  minus
    value: "6"
    value: "2"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"(a + 1) * 5 / (6 - 2)"); err != nil {
		t.Error(err)
		return
	}

	input = "a + (1 * 5) / 6 - 2"
	expectedOutput = `
minus
  plus
    value: "a"
    div
      times
        value: "1"
        value: "5"
      value: "6"
  value: "2"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"a + 1 * 5 / 6 - 2"); err != nil {
		t.Error(err)
		return
	}

	input = "a + 1 * [1,2,[1,2],3]"
	expectedOutput = `
plus
  value: "a"
  times
    value: "1"
    list
      value: "1"
      value: "2"
      list
        value: "1"
        value: "2"
      value: "3"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"a + 1 * [1, 2, [1, 2], 3]"); err != nil {
		t.Error(err)
		return
	}

	input = "not (a + 1) * 5 and tRue or not 1 - 5 != !test"
	expectedOutput = `
or
  and
    not
      times
        plus
          value: "a"
          value: "1"
        value: "5"
    true
  not
    !=
      minus
        value: "1"
        value: "5"
      value: "!test"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"not (a + 1) * 5 and true or not 1 - 5 != \"!test\""); err != nil {
		t.Error(err)
		return
	}

	input = "a and (b or c)"
	expectedOutput = `
and
  value: "a"
  or
    value: "b"
    value: "c"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"a and (b or c)"); err != nil {
		t.Error(err)
		return
	}
}

func TestQueryPrinting(t *testing.T) {

	input := `
GeT Song FROM group test`
	expectedOutput := `
get
  value: "Song"
  from
    group
      value: "test"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"get Song from group test"); err != nil {
		t.Error(err)
		return
	}

	input = `
GeT Song`
	expectedOutput = `
get
  value: "Song"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"get Song"); err != nil {
		t.Error(err)
		return
	}

	input = `
GeT Song where foo in bar and bar notin foo or xx = ""`
	expectedOutput = `
get
  value: "Song"
  where
    or
      and
        in
          value: "foo"
          value: "bar"
        notin
          value: "bar"
          value: "foo"
      =
        value: "xx"
        value: ""
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		"get Song where foo in bar and bar notin foo or xx = \"\""); err != nil {
		t.Error(err)
		return
	}

	input = `
lOOkup Song "a","b","c"`
	expectedOutput = `
lookup
  value: "Song"
  value: "a"
  value: "b"
  value: "c"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		`lookup Song "a", "b", "c"`); err != nil {
		t.Error(err)
		return
	}

	input = `
lOOkup Song "a","b","c", "blaД" primary Song
FROM group test
show a, b`
	expectedOutput = `
lookup
  value: "Song"
  value: "a"
  value: "b"
  value: "c"
  value: "blaД"
  primary
    value: "Song"
  from
    group
      value: "test"
  show
    showterm: "a"
    showterm: "b"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		`lookup Song "a", "b", "c", "blaД" primary Song from group test
show
  a,
  b`); err != nil {
		t.Error(err)
		return
	}

	input = `
GeT bla FROM group test where attr:Name != val:Node1 AND b = 1 + -1 - 1 where True`
	expectedOutput = `
get
  value: "bla"
  from
    group
      value: "test"
  where
    and
      !=
        value: "attr:Name"
        value: "val:Node1"
      =
        value: "b"
        minus
          plus
            value: "1"
            minus
              value: "1"
          value: "1"
  where
    true
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		`get bla from group test where attr:Name != val:Node1 and b = 1 + -1 - 1 where true`); err != nil {
		t.Error(err)
		return
	}

	input = `
GeT bla TraverSE :::bla where true or false TraverSE test:::xxx where false end TraverSE :::ttt where true END END where n.ab = 1`
	expectedOutput = `
get
  value: "bla"
  traverse
    value: ":::bla"
    where
      or
        true
        false
    traverse
      value: "test:::xxx"
      where
        false
    traverse
      value: ":::ttt"
      where
        true
  where
    =
      value: "n.ab"
      value: "1"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput, `
get bla 
  traverse :::bla where true or false 
    traverse test:::xxx where false
    end 
    traverse :::ttt where true
    end
  end where n.ab = 1`[1:]); err != nil {
		t.Error(err)
		return
	}

	input = `
GeT Song where @a() or @count("File:File:StoredData:Data") > 1 and @boolfunc1(123,"test", aaa)`
	expectedOutput = `
get
  value: "Song"
  where
    or
      func
        value: "a"
      and
        >
          func
            value: "count"
            value: "File:File:"...
          value: "1"
        func
          value: "boolfunc1"
          value: "123"
          value: "test"
          value: "aaa"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput,
		`get Song where @a() or @count(File:File:StoredData:Data) > 1 and @boolfunc1(123, test, aaa)`); err != nil {
		t.Error(err)
		return
	}
}

func TestShowPrinting(t *testing.T) {

	input := `
get song where true primary 1:song show name, state`
	expectedOutput := `
get
  value: "song"
  where
    true
  primary
    value: "1:song"
  show
    showterm: "name"
    showterm: "state"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput, `
get song where true primary 1:song
show
  name,
  state`[1:]); err != nil {
		t.Error(err)
		return
	}

	input = `
get song where true primary 1:song show name, state, @test(12, r"34") AS Bla FORMAT x, key`
	expectedOutput = `
get
  value: "song"
  where
    true
  primary
    value: "1:song"
  show
    showterm: "name"
    showterm: "state"
    showterm
      func
        value: "test"
        value: "12"
        value: "34"
      as
        value: "Bla"
      format
        value: "x"
    showterm: "key"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput, `
get song where true primary 1:song
show
  name,
  state,
  @test(12, 34) as Bla format x,
  key`[1:]); err != nil {
		t.Error(err)
		return
	}

	input = `
get song where true primary 1:song show @test(12, r"34") format x`
	expectedOutput = `
get
  value: "song"
  where
    true
  primary
    value: "1:song"
  show
    showterm
      func
        value: "test"
        value: "12"
        value: "34"
      format
        value: "x"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput, `
get song where true primary 1:song
show
  @test(12, 34) format x`[1:]); err != nil {
		t.Error(err)
		return
	}

	input = `
get song where true primary 1:song show 
Song:title AS r'Title (mytitle)',
r'Song!2:t title' AS "Title test" FORMAT text:bla_bla_blub:dudududu,
x:kind`
	expectedOutput = `
get
  value: "song"
  where
    true
  primary
    value: "1:song"
  show
    showterm: "Song:title"
      as
        value: "Title (myt"...
    showterm: "Song!2:t t"...
      as
        value: "Title test"
      format
        value: "text:bla_b"...
    showterm: "x:kind"
`[1:]

	if err := testPrettyPrinting(input, expectedOutput, `
get song where true primary 1:song
show
  Song:title as "Title (mytitle)",
  "Song!2:t title" as "Title test" format text:bla_bla_blub:dudududu,
  x:kind`[1:]); err != nil {
		t.Error(err)
		return
	}

	input = `
get song where true // 'div' show bla wIth orderinG(ASCending aa,Descending bb), FILTERING(ISNOTNULL test2,UNIQUE test3, uniquecount test3), nulltraversal(true)`
	expectedOutput = `
get
  value: "song"
  where
    divint
      true
      value: "div"
  show
    showterm: "bla"
  with
    ordering
      asc
        value: "aa"
      desc
        value: "bb"
    filtering
      isnotnull
        value: "test2"
      unique
        value: "test3"
      uniquecount
        value: "test3"
    nulltraversal
      true
`[1:]

	if err := testPrettyPrinting(input, expectedOutput, `
get song where true // div
show
  bla 
with
  ordering(ascending aa, descending bb),
  filtering(isnotnull test2, unique test3, uniquecount test3),
  nulltraversal(true)`[1:]); err != nil {
		t.Error(err)
		return
	}
}

func TestSpecialCases(t *testing.T) {

	// Test error reporting of an illegal AST node

	input := "get test"

	astres, err := ParseWithRuntime("mytest", input, &TestRuntimeProvider{})
	if err != nil {
		t.Error(err)
		return
	}

	// Create an illegal node

	astres.Children[0].Name = "foobar"

	_, err = PrettyPrint(astres)

	if err.Error() != "Could not find template for foobar (tempkey: foobar)" {
		t.Error("Unexpected result:", err)
		return
	}

	// Test if a value contains a double quote

	input = `get test where a = 'test "'`

	astres, err = ParseWithRuntime("mytest", input, &TestRuntimeProvider{})
	if err != nil {
		t.Error(err)
		return
	}

	ppres, err := PrettyPrint(astres)

	if ppres != `get test where a = 'test "'` {
		t.Error("Unexpected result:", ppres)
		return
	}

	// Test if value contains double and single quote

	input = `get test where a = "test 1: \" 2: ' "`

	astres, err = ParseWithRuntime("mytest", input, nil)
	if err != nil {
		t.Error(err)
		return
	}

	ppres, err = PrettyPrint(astres)

	if ppres != `get test where a = "test 1: \" 2: ' "` {
		t.Error("Unexpected result:", ppres)
		return
	}
}

func testPrettyPrinting(input, astOutput, ppOutput string) error {

	astres, err := ParseWithRuntime("mytest", input, &TestRuntimeProvider{})
	if err != nil || fmt.Sprint(astres) != astOutput {
		return fmt.Errorf("Unexpected parser output:\n%v expected was:\n%v Error: %v", astres, astOutput, err)
	}

	ppres, err := PrettyPrint(astres)
	if err != nil || ppres != ppOutput {
		return fmt.Errorf("Unexpected result: %v %v", ppres, err)
	}

	// Make sure the pretty printed result is valid and gets the same parse tree

	astres2, err := ParseWithRuntime("mytest", ppres, &TestRuntimeProvider{})
	if err != nil || fmt.Sprint(astres2) != astOutput {
		return fmt.Errorf("Unexpected parser output from pretty print string:\n%v expected was:\n%v Error: %v", astres2, astOutput, err)
	}

	return nil
}
