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
	"fmt"
	"net/http"

	"github.com/krotik/common/lang/graphql/parser"
	"github.com/krotik/common/stringutil"
	"github.com/krotik/eliasdb/api"
	"github.com/krotik/eliasdb/graphql"
)

/*
EndpointGraphQL is the GraphQL endpoint URL (rooted). Handles everything under graphql/...
*/
const EndpointGraphQL = api.APIRoot + APIv1 + "/graphql/"

/*
GraphQLEndpointInst creates a new endpoint handler.
*/
func GraphQLEndpointInst() api.RestEndpointHandler {
	return &graphQLEndpoint{}
}

/*
Handler object for GraphQL operations.
*/
type graphQLEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandlePOST handles GraphQL queries.
*/
func (e *graphQLEndpoint) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {
	var err error
	var res map[string]interface{}

	dec := json.NewDecoder(r.Body)
	data := make(map[string]interface{})

	if err := dec.Decode(&data); err != nil {
		http.Error(w, "Could not decode request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	toAST, ok1 := data["query-to-ast"]
	toQuery, ok2 := data["ast-to-query"]
	if ok1 || ok2 {

		res := make(map[string]interface{})

		if ok1 {
			resast, err := parser.Parse("request", fmt.Sprint(toAST))

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			res["result-ast"] = resast.Plain()
		}

		if ok2 {
			astmap, ok := toQuery.(map[string]interface{})

			if !ok {
				http.Error(w, "Plain AST object expected as 'ast-to-query' value", http.StatusBadRequest)
				return
			}

			// Try to create a proper AST from plain AST

			astnode, err := parser.ASTFromPlain(astmap)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			// Now pretty print the AST

			ppres, err := parser.PrettyPrint(astnode)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			res["result-query"] = ppres
		}

		w.Header().Set("content-type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(res)

		return

	} else {
		partData, ok := data["partition"]
		if !ok && len(resources) > 0 {
			partData = resources[0]
			ok = true
		}
		if !ok || partData == "" {
			http.Error(w, "Need a partition", http.StatusBadRequest)
			return
		}

		part := fmt.Sprint(partData)

		if _, ok := data["variables"]; !ok {
			data["variables"] = nil
		}

		if _, ok := data["operationName"]; !ok {
			data["operationName"] = nil
		}

		res, err = graphql.RunQuery(stringutil.CreateDisplayString(part)+" query",
			part, data, api.GM, nil, false)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(res)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (e *graphQLEndpoint) SwaggerDefs(s map[string]interface{}) {

	graphqlRequestParam := map[string]interface{}{
		"name":        "graphql_request",
		"in":          "body",
		"description": "GraphQL request",
		"required":    true,
		"schema": map[string]interface{}{
			"$ref": "#/definitions/GraphQLRequest",
		},
	}

	s["paths"].(map[string]interface{})["/v1/graphql/{partition}"] = map[string]interface{}{
		"post": map[string]interface{}{
			"summary":     "GraphQL interface.",
			"description": "The GraphQL interface can be used to query and modify data.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": []map[string]interface{}{
				{
					"name":        "partition",
					"in":          "path",
					"description": "Partition to query.",
					"required":    true,
					"type":        "string",
				},
				graphqlRequestParam,
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "The operation was successful.",
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
	}

	s["definitions"].(map[string]interface{})["GraphQLRequest"] = map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"operationName": map[string]interface{}{
				"description": "GraphQL query operation name.",
				"type":        "string",
			},
			"query": map[string]interface{}{
				"description": "GraphQL query.",
				"type":        "string",
			},
			"variables": map[string]interface{}{
				"description": "GraphQL query variable values.",
				"type":        "object",
			},
		},
	}

	s["paths"].(map[string]interface{})["/v1/graphql"] = map[string]interface{}{
		"post": map[string]interface{}{
			"summary":     "GraphQL parser and pretty printer endpoint.",
			"description": "The GraphQL endpoint without specifying a partition should be used to parse a given GraphQL query into an Abstract Syntax Tree or pretty print a given Abstract Syntax Tree into a GraphQL query.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": []map[string]interface{}{
				{
					"name":        "data",
					"in":          "body",
					"description": "Query or AST which should be converted.",
					"required":    true,
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"query-to-ast": map[string]interface{}{
								"description": "Query which should be parsed.",
								"type":        "string",
							},
							"ast-to-query": map[string]interface{}{
								"description": "AST which should be pretty printed.",
								"type":        "object",
							},
						},
					},
				},
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "The operation was successful.",
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"result-ast": map[string]interface{}{
								"description": "The resulting AST if a query was parsed.",
								"type":        "object",
							},
							"result-query": map[string]interface{}{
								"description": "The pretty printed query if an AST was given.",
								"type":        "string",
							},
						},
					},
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
	}
}
