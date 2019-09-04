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
	"fmt"
	"testing"

	"devt.de/krotik/common/lang/graphql/parser"
)

func TestIntrospection(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types {
      ...FullType
    }
    directives {
      name
      description
      locations
      args {
        ...InputValue
      }
    }
  }
}

fragment FullType on __Type {
  kind
  name
  description
  fields(includeDeprecated: true) {
    name
    description
    args {
      ...InputValue
    }
    type {
      ...TypeRef
    }
    isDeprecated
    deprecationReason
  }
  inputFields {
    ...InputValue
  }
  interfaces {
    ...TypeRef
  }
  enumValues(includeDeprecated: true) {
    name
    description
    isDeprecated
    deprecationReason
  }
  possibleTypes {
    ...TypeRef
  }
}

fragment InputValue on __InputValue {
  name
  description
  type { ...TypeRef }
  defaultValue
}

fragment TypeRef on __Type {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
    }
  }
}
`}

	res, err := runQuery("test", "main", query, gm, nil, false)

	data := res["data"].(map[string]interface{})
	schema := data["__schema"].(map[string]interface{})

	if _, ok := schema["types"]; !ok || err != nil {
		t.Error("Unexpected result:", schema, err)
		return
	}

	// Create runtime provider

	rtp := NewGraphQLRuntimeProvider("test", "main", gm,
		fmt.Sprint(query["operationName"]), make(map[string]interface{}), nil, true)

	// Parse the query and annotate the AST with runtime components

	ast, err := parser.ParseWithRuntime("test", fmt.Sprint(query["query"]), rtp)

	if err != nil {
		t.Error("Unexpected result", err)
	}

	err = ast.Runtime.Validate()

	if err != nil {
		t.Error("Unexpected result", err)
	}

	// Evaluate the query

	sr := ast.Children[0].Children[0].Children[2].Children[0].Children[1].Runtime.(*selectionSetRuntime)

	full := formatData(sr.ProcessFullIntrospection())
	filtered := formatData(sr.ProcessIntrospection())

	if full != filtered {

		// This needs thorough investigation - no point in outputting these
		// large datastructures during failure

		t.Error("Full and filtered introspection are different")
		return
	}

	// Now try out a reduced version

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
	   query IntrospectionQuery {
	     __schema {
	       queryType { name }
	       mutationType { name }
	       subscriptionType { name }
	       directives {
	         name
	         description
	         locations
	         args {
	           ...InputValue
	           ...InputValue @skip(if: true)
				... {
					name
				}
	         }
			name1: name1
	       }
	     }
	   }

	   fragment InputValue on __InputValue {
	     name
	     description
	     type { ...TypeRef }
	     defaultValue
	   }

	   fragment TypeRef on __Type {
	     kind
	     name
	   }
	   `}

	res, err = runQuery("test", "main", query, gm, nil, false)

	if formatData(res) != `{
  "data": {
    "__schema": {
      "directives": [
        {
          "args": [
            {
              "defaultValue": null,
              "description": "Skipped when true.",
              "name": "if",
              "type": {
                "kind": "NON_NULL",
                "name": null
              }
            }
          ],
          "description": "Directs the executor to skip this field or fragment when the `+"`if`"+` argument is true.",
          "locations": [
            "FIELD",
            "FRAGMENT_SPREAD",
            "INLINE_FRAGMENT"
          ],
          "name": "skip",
          "name1": null
        },
        {
          "args": [
            {
              "defaultValue": null,
              "description": "Included when true.",
              "name": "if",
              "type": {
                "kind": "NON_NULL",
                "name": null
              }
            }
          ],
          "description": "Directs the executor to include this field or fragment only when the `+"`if`"+` argument is true.",
          "locations": [
            "FIELD",
            "FRAGMENT_SPREAD",
            "INLINE_FRAGMENT"
          ],
          "name": "include",
          "name1": null
        }
      ],
      "mutationType": {
        "name": "Mutation"
      },
      "queryType": {
        "name": "Query"
      },
      "subscriptionType": {
        "name": "Subscription"
      }
    }
  }
}` {
		t.Error("Unexpected result:", formatData(res), err)
		return
	}
}
