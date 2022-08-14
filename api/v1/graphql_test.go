/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package v1

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/krotik/common/errorutil"
)

func TestGraphQLQuery(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraphQLQuery

	query := url.QueryEscape(`{
  Song(key : "Aria1") {
	key
  }
}`)
	_, _, res := sendTestRequest(queryURL+"main?query="+query, "GET", nil)

	if res != `
{
  "data": {
    "Song": [
      {
        "key": "Aria1"
      }
    ]
  }
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	query = url.QueryEscape(`query foo($bar : String) {
  Song(key : $bar) {
	key
  }
}`)
	variables := url.QueryEscape(`{ "bar" : "Aria1" }`)
	_, _, res = sendTestRequest(queryURL+"main?operationName=foo&query="+query+"&variables="+variables, "GET", nil)

	if res != `
{
  "data": {
    "Song": [
      {
        "key": "Aria1"
      }
    ]
  }
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}
}

func TestGraphQLQueryErrors(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraphQLQuery

	query := url.QueryEscape(`{`)
	_, _, res := sendTestRequest(queryURL+"main?query="+query, "GET", nil)

	if res != "Parse error in Main query: Unexpected end (Line:1 Pos:1)" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"?query="+query, "GET", nil)

	if res != "Need a partition" {
		t.Error("Unexpected response:", res)
		return
	}
	_, _, res = sendTestRequest(queryURL+"main?ry="+query, "GET", nil)

	if res != "Need a query parameter" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main?query="+query+"&variables=123", "GET", nil)

	if res != "Could not decode variables: json: cannot unmarshal number into Go value of type map[string]interface {}" {
		t.Error("Unexpected response:", res)
		return
	}
}

func TestGraphQL(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraphQL

	q, err := json.Marshal(map[string]interface{}{
		"partition": "main",
		"query": `{
  Song(key : "Aria1") {
	key
  }
}`,
	})
	errorutil.AssertOk(err)
	_, _, res := sendTestRequest(queryURL+"main", "POST", q)

	if res != `
{
  "data": {
    "Song": [
      {
        "key": "Aria1"
      }
    ]
  }
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}
}

func TestGraphQLErrors(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraphQL

	q, err := json.Marshal(map[string]interface{}{
		"operationName": nil,
		"variables":     nil,
		"query":         "{",
	})
	errorutil.AssertOk(err)
	_, _, res := sendTestRequest(queryURL+"main", "POST", q)

	if res != "Parse error in Main query: Unexpected end (Line:1 Pos:1)" {
		t.Error("Unexpected response:", res)
		return
	}

	q, err = json.Marshal(map[string]interface{}{
		"operationName": nil,
		"variables":     nil,
		"query":         "{",
	})
	errorutil.AssertOk(err)
	_, _, res = sendTestRequest(queryURL, "POST", q)

	if res != "Need a partition" {
		t.Error("Unexpected response:", res)
		return
	}

	q, err = json.Marshal(map[string]interface{}{
		"partition":     "main",
		"operationName": nil,
		"variables":     nil,
	})
	errorutil.AssertOk(err)
	_, _, res = sendTestRequest(queryURL, "POST", q)

	if res != "Mandatory field 'query' missing from query object" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL, "POST", []byte("{"))

	if res != "Could not decode request body: unexpected EOF" {
		t.Error("Unexpected response:", res)
		return
	}
}

func TestGraphQLParsing(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraphQL

	q, err := json.Marshal(map[string]interface{}{
		"query-to-ast": `{
  Song
}`,
	})
	errorutil.AssertOk(err)
	_, _, res := sendTestRequest(queryURL+"main", "POST", q)

	if res != `
{
  "result-ast": {
    "children": [
      {
        "children": [
          {
            "children": [
              {
                "children": [
                  {
                    "children": [
                      {
                        "name": "Name",
                        "value": "Song"
                      }
                    ],
                    "name": "Field"
                  }
                ],
                "name": "SelectionSet"
              }
            ],
            "name": "OperationDefinition"
          }
        ],
        "name": "ExecutableDefinition"
      }
    ],
    "name": "Document"
  }
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main", "POST", []byte(`{"ast-to-query": {
  "children": [
    {
      "children": [
        {
          "children": [
            {
              "children": [
                {
                  "children": [
                    {
                      "name": "Name",
                      "value": "Song"
                    }
                  ],
                  "name": "Field"
                }
              ],
              "name": "SelectionSet"
            }
          ],
          "name": "OperationDefinition"
        }
      ],
      "name": "ExecutableDefinition"
    }
  ],
  "name": "Document"
}}`))

	if res != `
{
  "result-query": "{\n  Song\n}"
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}
}

func TestGraphQLParsingErrors(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraphQL

	q, err := json.Marshal(map[string]interface{}{
		"query-to-ast": `{{
  Song
}`,
	})
	errorutil.AssertOk(err)
	_, _, res := sendTestRequest(queryURL+"main", "POST", q)

	if res != "Parse error in request: Name expected ({) (Line:1 Pos:2)" {
		t.Error("Unexpected response:", res)
		return
	}

	q, err = json.Marshal(map[string]interface{}{
		"ast-to-query": `aaa`,
	})
	errorutil.AssertOk(err)
	_, _, res = sendTestRequest(queryURL+"main", "POST", q)

	if res != "Plain AST object expected as 'ast-to-query' value" {
		t.Error("Unexpected response:", res)
		return
	}

	q, err = json.Marshal(map[string]interface{}{
		"ast-to-query": map[string]interface{}{
			"foo": `Document`,
		},
	})
	errorutil.AssertOk(err)
	_, _, res = sendTestRequest(queryURL+"main", "POST", q)

	if res != "Found plain ast node without a name: map[foo:Document]" {
		t.Error("Unexpected response:", res)
		return
	}

	q, err = json.Marshal(map[string]interface{}{
		"ast-to-query": map[string]interface{}{
			"name": `foo`,
		},
	})
	errorutil.AssertOk(err)
	_, _, res = sendTestRequest(queryURL+"main", "POST", q)

	if res != "Could not find template for foo (tempkey: foo)" {
		t.Error("Unexpected response:", res)
		return
	}
}
