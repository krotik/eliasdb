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
	"net/http"

	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/graphql"
)

/*
EndpointGraphQLQuery is a query-only GraphQL endpoint URL (rooted). Handles
everything under graphql-query/...
*/
const EndpointGraphQLQuery = api.APIRoot + APIv1 + "/graphql-query/"

/*
GraphQLQueryEndpointInst creates a new endpoint handler.
*/
func GraphQLQueryEndpointInst() api.RestEndpointHandler {
	return &graphQLQueryEndpoint{}
}

/*
Handler object for GraphQL operations.
*/
type graphQLQueryEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles GraphQL queries.
*/
func (e *graphQLQueryEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {

	gqlquery := map[string]interface{}{
		"variables":     nil,
		"operationName": nil,
	}

	partition := r.URL.Query().Get("partition")
	if partition == "" && len(resources) > 0 {
		partition = resources[0]
	}

	if partition == "" {
		http.Error(w, "Need a partition", http.StatusBadRequest)
		return
	}

	query := r.URL.Query().Get("query")
	if query == "" {
		http.Error(w, "Need a query parameter", http.StatusBadRequest)
		return
	}
	gqlquery["query"] = query

	if operationName := r.URL.Query().Get("operationName"); operationName != "" {
		gqlquery["operationName"] = operationName
	}

	if variables := r.URL.Query().Get("variables"); variables != "" {
		varData := make(map[string]interface{})

		if err := json.Unmarshal([]byte(variables), &varData); err != nil {
			http.Error(w, "Could not decode variables: "+err.Error(), http.StatusBadRequest)
			return
		}

		gqlquery["variables"] = varData
	}

	res, err := graphql.RunQuery(stringutil.CreateDisplayString(partition)+" query",
		partition, gqlquery, api.GM, nil, true)

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
func (e *graphQLQueryEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/v1/graphql-query/{partition}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "GraphQL interface which only executes non-modifying queries.",
			"description": "The GraphQL interface can be used to query data.",
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
				{
					"name":        "operationName",
					"in":          "query",
					"description": "GraphQL query operation name.",
					"required":    false,
					"type":        "string",
				},
				{
					"name":        "query",
					"in":          "query",
					"description": "GraphQL query.",
					"required":    true,
					"type":        "string",
				},
				{
					"name":        "variables",
					"in":          "query",
					"description": "GraphQL query variable values.",
					"required":    false,
					"type":        "string",
				},
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
}
