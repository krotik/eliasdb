/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
Package eql contains the main API for EQL.

Example EQL query:

GET Person where name = "Marvin"
*/
package eql

import (
	"strings"

	"devt.de/eliasdb/eql/interpreter"
	"devt.de/eliasdb/eql/parser"
	"devt.de/eliasdb/graph"
)

/*
GroupNodeKind is a special node kind representing groups
*/
const GroupNodeKind = interpreter.GroupNodeKind

/*
RunQuery runs a search query against a given graph database.
*/
func RunQuery(name string, part string, query string, gm *graph.Manager) (SearchResult, error) {
	return RunQueryWithNodeInfo(name, part, query, gm, interpreter.NewDefaultNodeInfo(gm))
}

/*
RunQueryWithNodeInfo runs a search query against a given graph database. Using
a given NodeInfo object to retrieve rendering information.
*/
func RunQueryWithNodeInfo(name string, part string, query string, gm *graph.Manager, ni interpreter.NodeInfo) (SearchResult, error) {
	var rtp parser.RuntimeProvider

	word := strings.ToLower(parser.FirstWord(query))

	if word == "get" {
		rtp = interpreter.NewGetRuntimeProvider(name, part, gm, ni)
	} else if word == "lookup" {
		rtp = interpreter.NewLookupRuntimeProvider(name, part, gm, ni)
	} else {
		return nil, &interpreter.RuntimeError{
			Source: name,
			Type:   interpreter.ErrInvalidConstruct,
			Detail: "Unknown query type: " + word,
			Node:   nil,
			Line:   1,
			Pos:    1,
		}
	}

	ast, err := parser.ParseWithRuntime(name, query, rtp)
	if err != nil {
		return nil, err
	}

	res, err := ast.Runtime.Eval()
	if err != nil {
		return nil, err
	}

	return &queryResult{res.(*interpreter.SearchResult)}, nil
}

/*
ParseQuery parses a search query and return its Abstract Syntax Tree.
*/
func ParseQuery(name string, query string) (*parser.ASTNode, error) {
	ast, err := parser.Parse(name, query)
	if err != nil {
		return nil, err
	}

	return ast, nil
}

/*
queryResult datastructure to hide implementation details.
*/
type queryResult struct {
	*interpreter.SearchResult
}

/*
Header returns a data structure describing the result header.
*/
func (qr *queryResult) Header() SearchResultHeader {
	return qr.SearchResult.Header()
}
