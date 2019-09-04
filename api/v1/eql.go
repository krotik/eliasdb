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

	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/eql"
	"devt.de/krotik/eliasdb/eql/parser"
)

/*
EndpointEql is the eql endpoint URL (rooted). Handles everything under eql/...
*/
const EndpointEql = api.APIRoot + APIv1 + "/eql/"

/*
EqlEndpointInst creates a new endpoint handler.
*/
func EqlEndpointInst() api.RestEndpointHandler {
	return &eqlEndpoint{}
}

/*
Handler object for eql operations.
*/
type eqlEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandlePOST handles REST calls to transform EQL queries.
*/
func (e *eqlEndpoint) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {

	dec := json.NewDecoder(r.Body)
	data := make(map[string]interface{})

	if err := dec.Decode(&data); err != nil {
		http.Error(w, "Could not decode request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Handle query and ast requests

	query, ok1 := data["query"]
	ast, ok2 := data["ast"]

	if ok1 || ok2 {

		res := make(map[string]interface{})

		if ok1 {
			resast, err := eql.ParseQuery("request", fmt.Sprint(query))

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			res["ast"] = resast.Plain()
		}

		if ok2 {

			astmap, ok := ast.(map[string]interface{})

			if !ok {
				http.Error(w, "Plain AST object expected as 'ast' value", http.StatusBadRequest)
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

			res["query"] = ppres
		}

		w.Header().Set("content-type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(res)

		return
	}

	http.Error(w, "Need either a query or an ast parameter", http.StatusBadRequest)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (e *eqlEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/v1/eql"] = map[string]interface{}{
		"post": map[string]interface{}{
			"summary":     "EQL parser and pretty printer endpoint.",
			"description": "The eql endpoint should be used to parse a given EQL query into an Abstract Syntax Tree or pretty print a given Abstract Syntax Tree into an EQL query.",
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
							"query": map[string]interface{}{
								"description": "Query which should be parsed.",
								"type":        "string",
							},
							"ast": map[string]interface{}{
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
							"ast": map[string]interface{}{
								"description": "The resulting AST if a query was parsed.",
								"type":        "object",
							},
							"query": map[string]interface{}{
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
