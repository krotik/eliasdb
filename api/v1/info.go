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
)

/*
EndpointInfoQuery is the info endpoint URL (rooted). Handles everything under info/...
*/
const EndpointInfoQuery = api.APIRoot + APIv1 + "/info/"

/*
InfoEndpointInst creates a new endpoint handler.
*/
func InfoEndpointInst() api.RestEndpointHandler {
	return &infoEndpoint{}
}

/*
Handler object for info queries.
*/
type infoEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles a info query REST call.
*/
func (ie *infoEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {

	data := make(map[string]interface{})

	if len(resources) > 0 {

		if resources[0] == "kind" {

			// Kind info is requested

			if len(resources) == 1 {
				http.Error(w, "Missing node kind", http.StatusBadRequest)
				return
			}

			na := api.GM.NodeAttrs(resources[1])
			ea := api.GM.EdgeAttrs(resources[1])

			if len(na) == 0 && len(ea) == 0 {
				http.Error(w, fmt.Sprint("Unknown node kind ", resources[1]), http.StatusBadRequest)
				return
			}

			data["node_attrs"] = na
			data["node_edges"] = api.GM.NodeEdges(resources[1])
			data["edge_attrs"] = ea
		}

	} else {

		// Get general information

		data["partitions"] = api.GM.Partitions()

		nks := api.GM.NodeKinds()
		data["node_kinds"] = nks

		ncs := make(map[string]uint64)
		for _, nk := range nks {
			ncs[nk] = api.GM.NodeCount(nk)
		}

		data["node_counts"] = ncs

		eks := api.GM.EdgeKinds()
		data["edge_kinds"] = eks

		ecs := make(map[string]uint64)
		for _, ek := range eks {
			ecs[ek] = api.GM.EdgeCount(ek)
		}

		data["edge_counts"] = ecs
	}

	// Write data

	w.Header().Set("content-type", "application/json; charset=utf-8")

	ret := json.NewEncoder(w)
	ret.Encode(data)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (ie *infoEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/v1/info"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return general datastore information.",
			"description": "The info endpoint returns general database information such as known node kinds, known attributes, etc.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "A key-value map.",
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

	s["paths"].(map[string]interface{})["/v1/info/kind/{kind}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return information on a given node or edge kind.",
			"description": "The info kind endpoint returns information on a given node kind such as known attributes and edges.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": []map[string]interface{}{
				{
					"name":        "kind",
					"in":          "path",
					"description": "Node or edge kind to be queried.",
					"required":    true,
					"type":        "string",
				},
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "A key-value map.",
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
