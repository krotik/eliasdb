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
	"errors"
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/eql/parser"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func TestDataQueries(t *testing.T) {
	gm := dataNodes()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if err := runSearch("get mynode", `
Labels: Mynode Key, Mynode Name, Nested, Nested.Nest1.Nest2.Atom1, Type
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:nested, 1:n:nested.nest1.nest2.atom1, 1:n:type
000, Node0, <not set>, <not set>, type1
123, Node1, <not set>, 1.46, type1
456, Node2, map[nest1:map[nest2:map[atom1:1.45]]], <not set>, type2
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where attr:nested.nest1.nest2.atom1 != null", `
Labels: Mynode Key, Mynode Name, Nested, Nested.Nest1.Nest2.Atom1, Type
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:nested, 1:n:nested.nest1.nest2.atom1, 1:n:type
123, Node1, <not set>, 1.46, type1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where nested.nest1.nest2.atom1 = 1.45", `
Labels: Mynode Key, Mynode Name, Nested, Nested.Nest1.Nest2.Atom1, Type
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:nested, 1:n:nested.nest1.nest2.atom1, 1:n:type
456, Node2, map[nest1:map[nest2:map[atom1:1.45]]], <not set>, type2
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where nested.nest1.nest2.atom = 1.45", `
Labels: Mynode Key, Mynode Name, Nested, Nested.Nest1.Nest2.Atom1, Type
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:nested, 1:n:nested.nest1.nest2.atom1, 1:n:type
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	// Test nested show clause

	if err := runSearch("get mynode where nested.nest1.nest2.atom1 = 1.45 show @objget(1, nested, nest1)", `
Labels: Nested.nest1
Format: auto
Data: 1:func:objget()
map[nest2:map[atom1:1.45]]
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where nested.nest1.nest2.atom1 = 1.45 show @objget(1, nested, nest1.nest2.atom1)", `
Labels: Nested.nest1.nest2.atom1
Format: auto
Data: 1:func:objget()
1.45
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode show @objget(1, key, nest1.nest2.atom1)", `
Labels: Key.nest1.nest2.atom1
Format: auto
Data: 1:func:objget()
000
123
456
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where nested.nest1.nest2.atom1 = 1.45 show name", `
Labels: Mynode Name
Format: auto
Data: 1:n:name
Node2
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where nested.nest1.nest2.atom1 = 1.45 show key, @objget(1, key, nest1.nest2.atom1)", `
Labels: Mynode Key, Key.nest1.nest2.atom1
Format: auto, auto
Data: 1:n:key, 1:func:objget()
456, 456
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

}

func TestWhere(t *testing.T) {
	gm, _ := simpleGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if err := runSearch("get mynode", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
000, Node0
123, Node1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	// Test simple where clause

	if err := runSearch("get mynode where Name = Node1", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
123, Node1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where 1", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
000, Node0
123, Node1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where 0", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where true", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
000, Node0
123, Node1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where false", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where null", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where 'a'", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
000, Node0
123, Node1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ''", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where Name = Node1", rt); err != nil {
		t.Error(err)
	}

	if err := runSearch("get mynode where attr:Name != val:Node1", `
Labels: Mynode Key, Name
Format: auto, auto
Data: 1:n:key, 1:n:Name
000, Node0
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where Name != Node1", rt); err != nil {
		t.Error(err)
	}

	if err := runSearch("get mynode where eattr:Name != Node1", "", rt); err.Error() !=
		"EQL error in test: Invalid where clause (No edge data available at this level) (Line:1 Pos:18)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode traverse :::mynewnode where r'Nam \xe2\x8c\x98 e' = Node4", `
Labels: Mynode Key, Name, Mynewnode Key, Na Me, Nam ⌘ E, Name
Format: auto, auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:Name, 2:n:key, 2:n:Na me, 2:n:Nam ⌘ e, 2:n:Name
123, Node1, xxx ⌘, <not set>, Node4, <not set>
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode traverse :::mynewnode where eattr:name = Edge1abc99 end", `
Labels: Mynode Key, Name, Mynewnode Key, Na Me, Nam ⌘ E, Name
Format: auto, auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:Name, 2:n:key, 2:n:Na me, 2:n:Nam ⌘ e, 2:n:Name
123, Node1, xxx ⌘, <not set>, Node4, <not set>
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	gm, _ = simpleList()
	rt = NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if err := runSearch("get mynode", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking < 2.2", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
123, Node1, 2.1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where name > 'Node0'", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where name < Node1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where name <= Node1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where key > 055", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where key >= 023test", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking <= 2.1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
123, Node1, 2.1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking > 2", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking >= 2.1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking >= 2.1 and ranking < 3", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where Name + Node1", rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking +  x", "", rt); err.Error() !=
		"EQL error in test: Value of operand is not a number (x) (Line:1 Pos:29)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where name + ranking", "", rt); err.Error() !=
		"EQL error in test: Value of operand is not a number (name=Node1) (Line:1 Pos:18)" {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where 1 and 2", rt); err != nil {
		t.Error(err)
	}

	if err := runSearch("get mynode where ranking = 02.100 or ranking = 3.5", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where not ranking = 02.100", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking = 10.7 % 4.1 + 0.1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking = 8.7 // 4.1 + 0.1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking = (3 * 4 - 10 + 0.1) / 2.1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where (3 * 4 - 10 + 0.1) - 2.1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where (3 * 4 - 10 + 0.1) - 2", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where [1,2] in [1,2,[1,2],3]", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking in [1,2,[1,2],3]", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where 0 in 2", rt); err != nil {
		t.Error(err)
	}

	if err := runSearch("get mynode where [1,2] in ranking", "", rt); err.Error() !=
		"EQL error in test: Value of operand is not a list (ranking=2.1) (Line:1 Pos:27)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where [1,2] in 1", "", rt); err.Error() !=
		"EQL error in test: Value of operand is not a list (1) (Line:1 Pos:27)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where ranking notin [1,2.1,3]", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where name contains 1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where 0 contains 2", rt); err != nil {
		t.Error(err)
	}

	if err := runSearch("get mynode where name containsnot 1", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where 0 containsnot 2", rt); err != nil {
		t.Error(err)
	}

	if err := runSearch("get mynode where name beginswith Nod", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where 0 beginswith 2", rt); err != nil {
		t.Error(err)
	}

	if err := runSearch("get mynode where name endswith de0", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where 0 endswith 2", rt); err != nil {
		t.Error(err)
	}

	// Test regex

	if err := runSearch("get mynode where name like 'Node?'", `
Labels: Mynode Key, Mynode Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
000, Node0, 1
123, Node1, 2.1
456, Node1, 3.5
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where 0 like 2", rt); err != nil {
		t.Error(err)
	}

	if err := runSearch("get mynode where name like '[1'", "", rt); err.Error() !=
		"EQL error in test: Value of operand is not a valid regex (\"[1\" - error parsing regexp: missing closing ]: `[1`) (Line:1 Pos:28)" {
		t.Error(err)
		return
	}

	gm, _ = regexList()
	rt = NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if err := runSearch("get mynode where name like regex", "", rt); err.Error() !=
		"EQL error in test: Value of operand is not a valid regex (\"[1\" - error parsing regexp: missing closing ]: `[1`) (Line:1 Pos:28)" {
		t.Error(err)
		return
	}

	if err := runSearch("get mynode where name = node0 and name like regex", `
Labels: Mynode Key, Mynode Name, Ranking, Regex
Format: auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking, 1:n:regex
000, node0, 1, ^[a-z]+[0-9]$
`[1:], rt); err != nil {
		t.Error(err)
		return
	}

	if err := testSimpleOperationErrors("get mynode where name like regex", rt); err != nil {
		t.Error(err)
	}
}

func TestWhereErrors(t *testing.T) {
	gm, _ := simpleGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	ast, err := parser.ParseWithRuntime("test", "get mynode where Name = Node1", rt)
	if err != nil {
		t.Error(err)
		return
	}

	val := ast.Children[1]

	// Test that normal eval of whereRuntime returns an error

	if _, err := val.Runtime.Eval(); err.Error() != "EQL error in test: Invalid construct (where) (Line:1 Pos:12)" {
		t.Error("Unexpected error return:", err)
	}

	val = ast.Children[1].Children[0]

	// Test that normal validate and eval of whereItemRuntime return errors

	if _, err := val.Runtime.Eval(); err.Error() != "EQL error in test: Invalid construct (=) (Line:1 Pos:23)" {
		t.Error("Unexpected error return:", err)
	}

	if err := val.Runtime.Validate(); err.Error() != "EQL error in test: Invalid construct (=) (Line:1 Pos:23)" {
		t.Error("Unexpected error return:", err)
	}

	// Test where validation error

	ast, err = parser.ParseWithRuntime("test", "get mynode show x where 1 = 2", rt)
	if _, err := ast.Runtime.Eval(); err.Error() !=
		"EQL error in test: Invalid construct (condition must be before show clause and traversals) (Line:1 Pos:19)" {
		t.Error(err)
		return
	}

	ast, err = parser.ParseWithRuntime("test", "get mynode where 1 = 2", rt)
	if err != nil {
		t.Error(err)
		return
	}

	vnode := ast.Children[1].Children[0].Children[0]
	tr := testRuntimeInst(rt.eqlRuntimeProvider, vnode)
	tr.retValidate = errors.New("TestError0")
	vnode.Runtime = tr

	err = ast.Runtime.Validate()
	if err == nil || err.Error() != "TestError0" {
		t.Error(err)
		return
	}

	// Test traversal where validation and eval error

	ast, err = parser.ParseWithRuntime("test", "get mynode traverse ::: where 1 = 2", rt)
	if err != nil {
		t.Error(err)
		return
	}

	vnode = ast.Children[1].Children[1].Children[0].Children[0]
	tr = testRuntimeInst(rt.eqlRuntimeProvider, vnode)
	tr.retCondEvalErr = errors.New("TestError1")
	vnode.Runtime = tr

	_, err = ast.Runtime.Eval()
	if err == nil || err.Error() != "TestError1" {
		t.Error(err)
		return
	}

	tr.retValidate = errors.New("TestError2")

	err = ast.Runtime.Validate()
	if err == nil || err.Error() != "TestError2" {
		t.Error(err)
		return
	}
}

func testSimpleOperationErrors(query string, rt *GetRuntimeProvider) error {
	ast, err := parser.ParseWithRuntime("test", query, rt)
	if err != nil {
		return err
	}

	insertTestRuntime := func(child int) error {

		val := ast.Children[1].Children[0].Children[child]
		tr := testRuntimeInst(rt.eqlRuntimeProvider, val)
		tr.retCondEvalErr = errors.New("TestError")
		oldRuntime := val.Runtime
		val.Runtime = tr

		_, err = ast.Runtime.Eval()
		if err == nil || err.Error() != "TestError" {
			return errors.New(fmt.Sprint("Unexpected error return:", err))
		}

		val.Runtime = oldRuntime

		return nil
	}

	if err := insertTestRuntime(0); err != nil {
		return err
	}

	return insertTestRuntime(1)
}

/*
TestRuntime for values
*/
type testRuntime struct {
	rtp            *eqlRuntimeProvider
	node           *parser.ASTNode
	retValidate    error
	retEvalVal     interface{}
	retEvalErr     error
	retCondEvalVal interface{}
	retCondEvalErr error
}

/*
invalidRuntimeInst returns a new runtime component instance.
*/
func testRuntimeInst(rtp *eqlRuntimeProvider, node *parser.ASTNode) *testRuntime {
	return &testRuntime{rtp, node, nil, nil, nil, nil, nil}
}

/*
Validate this node and all its child nodes.
*/
func (rt *testRuntime) Validate() error {
	return rt.retValidate
}

/*
Eval evaluate this runtime component.
*/
func (rt *testRuntime) Eval() (interface{}, error) {
	return rt.retEvalVal, rt.retCondEvalErr
}

/*
Evaluate the value as a condition component.
*/
func (rt *testRuntime) CondEval(node data.Node, edge data.Edge) (interface{}, error) {
	return rt.retCondEvalVal, rt.retCondEvalErr
}

func simpleList() (*graph.Manager, *graphstorage.MemoryGraphStorage) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "mynode")
	node0.SetAttr("name", "Node0")
	node0.SetAttr("ranking", 1)
	gm.StoreNode("main", node0)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("name", "Node1")
	node1.SetAttr("ranking", 2.1)
	gm.StoreNode("main", node1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynode")
	node2.SetAttr("name", "Node1")
	node2.SetAttr("ranking", 3.5)
	gm.StoreNode("main", node2)

	return gm, mgs.(*graphstorage.MemoryGraphStorage)
}

func regexList() (*graph.Manager, *graphstorage.MemoryGraphStorage) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "mynode")
	node0.SetAttr("name", "node0")
	node0.SetAttr("ranking", 1)
	node0.SetAttr("regex", "^[a-z]+[0-9]$")
	gm.StoreNode("main", node0)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "mynode")
	node2.SetAttr("name", "Node1")
	node2.SetAttr("regex", "[1")
	node2.SetAttr("ranking", 3.5)
	gm.StoreNode("main", node2)

	return gm, mgs.(*graphstorage.MemoryGraphStorage)
}
