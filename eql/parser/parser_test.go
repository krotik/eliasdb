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
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

/*
Test RuntimeProvider provides runtime components for a parse tree.
*/
type TestRuntimeProvider struct {
}

/*
Runtime returns a runtime component for a given ASTNode.
*/
func (trp *TestRuntimeProvider) Runtime(node *ASTNode) Runtime {
	return &TestRuntime{}
}

/*
Test Runtime provides the runtime for an ASTNode.
*/
type TestRuntime struct {
}

/*
Validate this runtime component and all its child components.
*/
func (tr *TestRuntime) Validate() error {
	return nil
}

/*
Eval evaluate this runtime component.
*/
func (tr *TestRuntime) Eval() (interface{}, error) {
	return nil, nil
}

func TestSimpleExpressionParsing(t *testing.T) {

	// Test error output

	input := `"bl\*a"`
	if _, err := Parse("mytest", input); err.Error() !=
		"Parse error in mytest: Lexical error (invalid syntax while parsing escape sequences) (Line:1 Pos:1)" {
		t.Error(err)
		return
	}

	// Test incomplete expression

	input = `a *`
	if _, err := Parse("mytest", input); err.Error() !=
		"Parse error in mytest: Unexpected end" {
		t.Error(err)
		return
	}

	// Test prefix operator

	input = ` + a - -5`
	expectedOutput := `
minus
  plus
    value: "a"
  minus
    value: "5"
`[1:]

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	// Test simple arithmetics

	input = "a + b * 5 /2"
	expectedOutput = `
plus
  value: "a"
  div
    times
      value: "b"
      value: "5"
    value: "2"
`[1:]

	if res, err := ParseWithRuntime("mytest", input, &TestRuntimeProvider{}); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	// Test brackets

	input = "a + 1 * (5 + 6)"
	expectedOutput = `
plus
  value: "a"
  times
    value: "1"
    plus
      value: "5"
      value: "6"
`[1:]

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	// Test logical operators

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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}
}

func TestQueryParsing(t *testing.T) {

	// Test get expressions

	input := `
GeT Song FROM group test`
	expectedOutput := `
get
  value: "Song"
  from
    group
      value: "test"
`[1:]

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	// Test lookup expressions

	input = `
lOOkup Song "a","b","c", "blaД"
FROM group test`
	expectedOutput = `
lookup
  value: "Song"
  value: "a"
  value: "b"
  value: "c"
  value: "blaД"
  from
    group
      value: "test"
`[1:]

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	// Test where clause

	input = `
GeT bla FROM group test where x = 1 AND b = -1 where True`
	expectedOutput = `
get
  value: "bla"
  from
    group
      value: "test"
  where
    and
      =
        value: "x"
        value: "1"
      =
        value: "b"
        minus
          value: "1"
  where
    true
`[1:]

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	input = `
GeT bla where nest.nint = 1 AND b = -1`
	expectedOutput = `
get
  value: "bla"
  where
    and
      =
        value: "nest.nint"
        value: "1"
      =
        value: "b"
        minus
          value: "1"
`[1:]

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	// Test traverse clause

	input = `
GeT bla TraverSE :::bla where true or false TraverSE test:::xxx where false TraverSE :::ttt where true END END END where 1 = 1`
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
      value: "1"
      value: "1"
`[1:]

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	// Test functions

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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}
}

func TestShowParsing(t *testing.T) {

	// Test simple show expression

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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
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

	if res, err := Parse("mytest", input); err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}
}

func TestParserErrorCases(t *testing.T) {

	if res, err := ParseWithRuntime("mytest", "", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected end" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET r\"aa", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Lexical error (Invalid node kind 'r\"aa' - can only contain [a-zA-Z0-9_]) (Line:1 Pos:5)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "= GET", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Term cannot start an expression (=) (Line:1 Pos:1)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "get a where 1 (", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Term can only start an expression (() (Line:1 Pos:15)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "get a where (=", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Term cannot start an expression (=) (Line:1 Pos:14)" {
		t.Error("Unexpected result", res, err)
		return
	}

	// Test "Get" parsing with invalid lexer output

	res, err := testParserRun([]LexToken{
		{TokenGET, 1, "", 1, 1},
		{TokenGET, 1, "", 1, 1},
		{TokenEOF, 1, "", 1, 1},
	})
	if err.Error() != "Parse error in special test: Unexpected term (Line:1 Pos:1)" {
		t.Error("Unexpected result", res, err)
		return
	}

	res, err = testParserRun([]LexToken{
		{TokenLOOKUP, 1, "", 1, 1},
		{TokenGET, 1, "", 1, 1},
		{TokenEOF, 1, "", 1, 1},
	})
	if err.Error() != "Parse error in special test: Unexpected term (Line:1 Pos:1)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "lookup x", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected end" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "lookup x '123', GET", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected term (GET) (Line:1 Pos:17)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "lookup x '123' GET", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected end" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x FROM GeT", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected term (GeT) (Line:1 Pos:12)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x traverse GeT", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected term (GeT) (Line:1 Pos:16)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x traverse ::: GeT", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected end" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x where @where(", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected term (where) (Line:1 Pos:14)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x where @xxx)", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected term ()) (Line:1 Pos:17)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x where @xxx(12", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected end (Line:1 Pos:18)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x where @xxx(abc,", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected end" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x show a AS get", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected term (get) (Line:1 Pos:17)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x show x, a FORMAT get", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected term (get) (Line:1 Pos:24)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "GET x show @bla(1,", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected end" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "get a with ordering)", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected term ()) (Line:1 Pos:20)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "get a with ordering(=", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Term cannot start an expression (=) (Line:1 Pos:21)" {
		t.Error("Unexpected result", res, err)
		return
	}

	if res, err := ParseWithRuntime("mytest", "get a where [1,2", &TestRuntimeProvider{}); err.Error() !=
		"Parse error in mytest: Unexpected end" {
		t.Error("Unexpected result", res, err)
		return
	}

	var TokenUnknown LexTokenID = -5

	res, err = testParserRun([]LexToken{
		{TokenUnknown, 1, "", 1, 1},
		{TokenEOF, 1, "", 1, 1},
	})
	if err.Error() != "Parse error in special test: Unknown term (id:-5 (\"\")) (Line:1 Pos:1)" {
		t.Error("Unexpected result", res, err)
		return
	}

	res, err = testParserRun([]LexToken{
		{TokenVALUE, 1, "", 1, 1},
		{TokenMINUS, 1, "", 1, 1},
		{TokenUnknown, 1, "", 1, 1},
		{TokenEOF, 1, "", 1, 1},
	})
	if err.Error() != "Parse error in special test: Unknown term (id:-5 (\"\")) (Line:1 Pos:1)" {
		t.Error("Unexpected result", res, err)
		return
	}
}

func TestAstPlainRepresentation(t *testing.T) {

	input := `
get song where true // 'div' show bla wIth orderinG(ASCending aa,Descending bb), FILTERING(ISNOTNULL test2,UNIQUE test3, uniquecount test3), nulltraversal(true)`
	expectedOutput := `
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

	res, err := Parse("mytest", input)

	if err != nil || fmt.Sprint(res) != expectedOutput {
		t.Error("Unexpected parser output:\n", res, "expected was:\n", expectedOutput, "Error:", err)
		return
	}

	plainres := res.Plain()

	jsonplainres, err := json.Marshal(plainres)
	if err != nil {
		t.Error(err)
		return
	}

	out := bytes.Buffer{}
	err = json.Indent(&out, []byte(jsonplainres), "", "  ")
	if err != nil {
		t.Error(err)
		return
	}

	if out.String() != `
{
  "children": [
    {
      "name": "value",
      "value": "song"
    },
    {
      "children": [
        {
          "children": [
            {
              "name": "true",
              "value": "true"
            },
            {
              "name": "value",
              "value": "div"
            }
          ],
          "name": "divint",
          "value": "//"
        }
      ],
      "name": "where",
      "value": "where"
    },
    {
      "children": [
        {
          "name": "showterm",
          "value": "bla"
        }
      ],
      "name": "show",
      "value": "show"
    },
    {
      "children": [
        {
          "children": [
            {
              "children": [
                {
                  "name": "value",
                  "value": "aa"
                }
              ],
              "name": "asc",
              "value": "ASCending"
            },
            {
              "children": [
                {
                  "name": "value",
                  "value": "bb"
                }
              ],
              "name": "desc",
              "value": "Descending"
            }
          ],
          "name": "ordering",
          "value": "orderinG"
        },
        {
          "children": [
            {
              "children": [
                {
                  "name": "value",
                  "value": "test2"
                }
              ],
              "name": "isnotnull",
              "value": "ISNOTNULL"
            },
            {
              "children": [
                {
                  "name": "value",
                  "value": "test3"
                }
              ],
              "name": "unique",
              "value": "UNIQUE"
            },
            {
              "children": [
                {
                  "name": "value",
                  "value": "test3"
                }
              ],
              "name": "uniquecount",
              "value": "uniquecount"
            }
          ],
          "name": "filtering",
          "value": "FILTERING"
        },
        {
          "children": [
            {
              "name": "true",
              "value": "true"
            }
          ],
          "name": "nulltraversal",
          "value": "nulltraversal"
        }
      ],
      "name": "with",
      "value": "wIth"
    }
  ],
  "name": "get",
  "value": "get"
}`[1:] {
		t.Error("Unexpected result: ", out.String())
		return
	}

	// Now convert the plain ast back into a normal AST and pretty print the result

	astfromplain, err := ASTFromPlain(plainres)
	if err != nil {
		t.Error(err)
		return
	}

	//  Check that the generated AST is equal to the expected output

	if fmt.Sprint(astfromplain) != expectedOutput {
		t.Error("Unexpected output:", astfromplain)
		return
	}

	ppquery, err := PrettyPrint(astfromplain)
	if err != nil {
		t.Error(err)
		return
	}

	if ppquery != `
get song where true // div
show
  bla 
with
  ordering(ascending aa, descending bb),
  filtering(isnotnull test2, unique test3, uniquecount test3),
  nulltraversal(true)`[1:] {
		t.Error("Unexpected output:", ppquery)
		return
	}

	// Test parsing from JSON (this will produce a []interface{} for children)

	data := make(map[string]interface{})
	json.NewDecoder(bytes.NewBufferString(`{
		"name"     : "get",
		"value"    : "get",
		"children" : [{
			"name"     : "value",
			"value"    : "bla"
		}]
	}`)).Decode(&data)

	astfromplain, err = ASTFromPlain(data)
	if err != nil {
		t.Error(err)
		return
	}

	if fmt.Sprint(astfromplain) != `
get
  value: "bla"
`[1:] {
		t.Error("Unexpected result:", astfromplain)
		return
	}

	// Test error message

	if _, err := ASTFromPlain(map[string]interface{}{
		"name": "bla",
	}); err.Error() != "Found plain ast node without a value: map[name:bla]" {
		t.Error("Unexpected error:", err)
		return
	}

	if _, err := ASTFromPlain(map[string]interface{}{
		"name":  "bla",
		"value": "",
		"children": []map[string]interface{}{{
			"fame": "bla",
		}},
	}); err.Error() != "Found plain ast node without a name: map[fame:bla]" {
		t.Error("Unexpected error:", err)
		return
	}
}

/*
Special function to test lexer runs which might be prevented by the actual lexer.
*/
func testParserRun(tokens []LexToken) (*ASTNode, error) {

	// Create channel which is filled with the given lex tokens

	tokenChan := make(chan LexToken)
	run := func() {
		for _, item := range tokens {
			tokenChan <- item
		}
	}
	go run()

	// Create parser which processes the given tokens

	p := &parser{"special test", nil, tokenChan, nil}

	node, err := p.next()

	if err != nil {
		return nil, err
	}

	p.node = node

	return p.run(0)
}
