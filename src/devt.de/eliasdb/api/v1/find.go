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
	"strings"

	"devt.de/common/stringutil"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
)

/*
EndpointFindQuery is the find endpoint URL (rooted). Handles everything under find/...
*/
const EndpointFindQuery = api.APIRoot + APIv1 + "/find/"

/*
FindEndpointInst creates a new endpoint handler.
*/
func FindEndpointInst() api.RestEndpointHandler {
	return &findEndpoint{}
}

/*
Handler object for find queries.
*/
type findEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles a search query REST call.
*/
func (ie *findEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	var err error

	ret := make(map[string]map[string][]interface{})

	// Check what is queried

	text := r.URL.Query().Get("text")
	value := r.URL.Query().Get("value")

	if text == "" && value == "" {
		http.Error(w, "Query string for text (word or phrase) or value (exact match) is required", http.StatusBadRequest)
		return
	}

	lookup := stringutil.IsTrueValue(r.URL.Query().Get("lookup"))
	part := r.URL.Query().Get("part")

	parts := api.GM.Partitions()
	kinds := api.GM.NodeKinds()

	if part != "" && stringutil.IndexOf(part, parts) == -1 {
		err = fmt.Errorf("Partition %s does not exist", part)
	}

	if err == nil {

		// Go through all partitions

		for _, p := range parts {

			if strings.HasPrefix(p, "_") || part != "" && part != p {

				// Ignore partitions which start with an _ character or if they
				// are not searched for.

				continue
			}

			partitionData := make(map[string][]interface{})
			ret[p] = partitionData

			// Go through all known node kinds

			for _, k := range kinds {
				var iq graph.IndexQuery
				var nodes []interface{}

				nodeMap := make(map[string]interface{})

				// NodeIndexQuery may return nil nil if the node kind does not exist
				// in a partition

				if iq, err = api.GM.NodeIndexQuery(p, k); err == nil && iq != nil {

					// Go through all known attributes of the node kind

					for _, attr := range api.GM.NodeAttrs(k) {
						var keys []string

						// Run the lookup on all attributes

						if text != "" {
							keys, err = iq.LookupPhrase(attr, text)
						} else {
							keys, err = iq.LookupValue(attr, value)
						}

						// Lookup all nodes

						for _, key := range keys {
							var node data.Node

							if _, ok := nodeMap[key]; !ok && err == nil {

								if lookup {
									if node, err = api.GM.FetchNode(p, key, k); node != nil {
										nodeMap[key] = node.Data()
									}
								} else {
									nodeMap[key] = map[string]interface{}{
										data.NodeKey:  key,
										data.NodeKind: k,
									}
								}
							}
						}
					}
				}

				for _, n := range nodeMap {
					nodes = append(nodes, n)
				}

				if nodes != nil {
					partitionData[k] = nodes
				}
			}
		}
	}

	// Check if there was an error

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write data

	w.Header().Set("content-type", "application/json; charset=utf-8")

	e := json.NewEncoder(w)
	e.Encode(ret)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (ie *findEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/v1/find"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Run index searches on the EliasDB datastore.",
			"description": "The find endpoint should be used to run simple index searches for either a value or a phrase.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": []map[string]interface{}{
				map[string]interface{}{
					"name":        "text",
					"in":          "query",
					"description": "A word or phrase to search for.",
					"required":    false,
					"type":        "string",
				},
				map[string]interface{}{
					"name":        "value",
					"in":          "query",
					"description": "A node value to search for.",
					"required":    false,
					"type":        "string",
				},
				map[string]interface{}{
					"name":        "lookup",
					"in":          "query",
					"description": "Flag if a complete node lookup should be done (otherwise only key and kind are returned).",
					"required":    false,
					"type":        "boolean",
				},
				map[string]interface{}{
					"name":        "part",
					"in":          "query",
					"description": "Limit the search to a partition (without the option all partitions are searched).",
					"required":    false,
					"type":        "string",
				},
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "An object of search results.",
					"schema": map[string]interface{}{
						"type":        "object",
						"description": "Object of results per partition.",
						"properties": map[string]interface{}{
							"partition": map[string]interface{}{
								"type":        "object",
								"description": "Object of results per kind.",
								"properties": map[string]interface{}{
									"kind": map[string]interface{}{
										"description": "List of found nodes.",
										"type":        "array",
										"items": map[string]interface{}{
											"description": "Found node.",
											"type":        "object",
										},
									},
								},
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

	// Add generic error object to definition

	s["definitions"].(map[string]interface{})["Error"] = map[string]interface{}{
		"description": "A human readable error mesage.",
		"type":        "string",
	}
}
