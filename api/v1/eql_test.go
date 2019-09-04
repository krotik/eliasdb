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
	"bytes"
	"encoding/json"
	"testing"
)

func TestEql(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointEql

	// Test error messages

	_, _, res := sendTestRequest(queryURL, "POST", nil)

	if res != "Could not decode request body: EOF" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main/n", "POST", []byte(`
{
    "foo" : "bar"
}
`[1:]))

	if res != "Need either a query or an ast parameter" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main/n", "POST", []byte(`
{
    "query" : "get =bla where foo = 'bar'"
}
`[1:]))

	if res != "Parse error in request: Lexical error (Invalid node kind '=bla' - can only contain [a-zA-Z0-9_]) (Line:1 Pos:5)" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main/n", "POST", []byte(`
{
    "ast" : "foobar"
}
`[1:]))

	if res != "Plain AST object expected as 'ast' value" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main/n", "POST", []byte(`
{
    "ast" : {
		"foo" : "bar"
	}
}
`[1:]))

	if res != "Found plain ast node without a name: map[foo:bar]" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main/n", "POST", []byte(`
{
    "ast" : {
		"name" : "foo",
		"value" : "bar"
	}
}
`[1:]))

	if res != "Could not find template for foo (tempkey: foo)" {
		t.Error("Unexpected response:", res)
		return
	}

	// Test parsing and pretty printing

	_, _, res = sendTestRequest(queryURL+"main/n", "POST", []byte(`
{
  "query": "get bla where foo = bar \nwith\n  ordering(ascending name)"
}
`[1:]))

	if res != `
{
  "ast": {
    "children": [
      {
        "name": "value",
        "value": "bla"
      },
      {
        "children": [
          {
            "children": [
              {
                "name": "value",
                "value": "foo"
              },
              {
                "name": "value",
                "value": "bar"
              }
            ],
            "name": "=",
            "value": "="
          }
        ],
        "name": "where",
        "value": "where"
      },
      {
        "children": [
          {
            "children": [
              {
                "children": [
                  {
                    "name": "value",
                    "value": "name"
                  }
                ],
                "name": "asc",
                "value": "ascending"
              }
            ],
            "name": "ordering",
            "value": "ordering"
          }
        ],
        "name": "with",
        "value": "with"
      }
    ],
    "name": "get",
    "value": "get"
  }
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	var astInput map[string]interface{}
	var astText bytes.Buffer

	json.NewDecoder(bytes.NewBufferString(res)).Decode(&astInput)

	json.NewEncoder(&astText).Encode(astInput)

	_, _, res = sendTestRequest(queryURL, "POST", astText.Bytes())

	if res != `
{
  "query": "get bla where foo = bar \nwith\n  ordering(ascending name)"
}`[1:] {
		t.Error("Unexpected result:", res)
		return
	}
}

func TestEqlSpecial(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointEql

	// And and or AST nodes might have more than 2 children

	res := `
{
  "ast": {
    "children": [
      {
        "name": "value",
        "value": "bla"
      },
      {
        "children": [
          {
            "name": "and",
            "value": "and",
            "children": [
	          {
	            "children": [
	              {
	                "name": "value",
	                "value": "foo1"
	              },
	              {
	                "name": "value",
	                "value": "bar1"
	              }
	            ],
	            "name": "=",
	            "value": "="
	          }, {
	            "children": [
	              {
	                "name": "value",
	                "value": "foo2"
	              },
	              {
	                "name": "value",
	                "value": "bar2"
	              }
	            ],
	            "name": "=",
	            "value": "="
	          }, {
	            "children": [
	              {
	                "name": "value",
	                "value": "foo3"
	              },
	              {
	                "name": "value",
	                "value": "bar3"
	              }
	            ],
	            "name": "=",
	            "value": "="
	          }
			]
		  }
        ],
        "name": "where",
        "value": "where"
      }
    ],
    "name": "get",
    "value": "get"
  }
}`[1:]

	var astInput map[string]interface{}
	var astText bytes.Buffer

	err := json.NewDecoder(bytes.NewBufferString(res)).Decode(&astInput)
	if err != nil {
		t.Error(err)
		return
	}

	json.NewEncoder(&astText).Encode(astInput)

	_, _, res = sendTestRequest(queryURL, "POST", astText.Bytes())

	if res != `
{
  "query": "get bla where foo1 = bar1 and foo2 = bar2 and foo3 = bar3"
}`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	res = `
{
  "ast": {
    "children": [
      {
        "name": "value",
        "value": "bla"
      },
      {
        "children": [
          {
            "name": "or",
            "value": "or",
            "children": [
	          {
	            "children": [
	              {
	                "name": "value",
	                "value": "foo1"
	              },
	              {
	                "name": "value",
	                "value": "bar1"
	              }
	            ],
	            "name": "=",
	            "value": "="
	          }, {
	            "children": [
	              {
	                "name": "value",
	                "value": "foo2"
	              },
	              {
	                "name": "value",
	                "value": "bar2"
	              }
	            ],
	            "name": "=",
	            "value": "="
	          }, {
	            "children": [
	              {
	                "name": "value",
	                "value": "foo3"
	              },
	              {
	                "name": "value",
	                "value": "bar3"
	              }
	            ],
	            "name": "=",
	            "value": "="
	          }
			]
		  }
        ],
        "name": "where",
        "value": "where"
      }
    ],
    "name": "get",
    "value": "get"
  }
}`[1:]

	err = json.NewDecoder(bytes.NewBufferString(res)).Decode(&astInput)
	if err != nil {
		t.Error(err)
		return
	}

	astText = bytes.Buffer{}

	json.NewEncoder(&astText).Encode(astInput)

	_, _, res = sendTestRequest(queryURL, "POST", astText.Bytes())

	if res != `
{
  "query": "get bla where foo1 = bar1 or foo2 = bar2 or foo3 = bar3"
}`[1:] {
		t.Error("Unexpected result:", res)
		return
	}
}
