/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package api

import (
	"encoding/json"
	"net/http"
)

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (a *aboutEndpoint) SwaggerDefs(s map[string]interface{}) {

	// Add query paths

	s["paths"].(map[string]interface{})["/about"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return information about the REST API provider.",
			"description": "Returns available API versions, product name and product version.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "About info object",
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"api_versions": map[string]interface{}{
								"description": "List of available API versions.",
								"type":        "array",
								"items": map[string]interface{}{
									"description": "Available API version.",
									"type":        "string",
								},
							},
							"product": map[string]interface{}{
								"description": "Product name of the REST API provider.",
								"type":        "string",
							},
							"version": map[string]interface{}{
								"description": "Version of the REST API provider.",
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

	// Add generic error object to definition

	s["definitions"].(map[string]interface{})["Error"] = map[string]interface{}{
		"description": "A human readable error mesage.",
		"type":        "string",
	}
}

/*
EndpointSwagger is the swagger endpoint URL (rooted). Handles swagger.json/
*/
const EndpointSwagger = APIRoot + "/swagger.json/"

/*
SwaggerEndpointInst creates a new endpoint handler.
*/
func SwaggerEndpointInst() RestEndpointHandler {
	return &swaggerEndpoint{}
}

/*
Handler object for swagger operations.
*/
type swaggerEndpoint struct {
	*DefaultEndpointHandler
}

/*
HandleGET returns the swagger definition of the REST API.
*/
func (a *swaggerEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {

	// Add general sections

	data := map[string]interface{}{
		"swagger":     "2.0",
		"host":        APIHost,
		"schemes":     APISchemes,
		"basePath":    APIRoot,
		"produces":    []string{"application/json"},
		"paths":       map[string]interface{}{},
		"definitions": map[string]interface{}{},
	}

	// Go through all registered components and let them add their definitions

	a.SwaggerDefs(data)

	for _, inst := range registered {
		inst().SwaggerDefs(data)
	}

	// Write data

	w.Header().Set("content-type", "application/json; charset=utf-8")

	ret := json.NewEncoder(w)
	ret.Encode(data)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (a *swaggerEndpoint) SwaggerDefs(s map[string]interface{}) {

	// Add general application information

	s["info"] = map[string]interface{}{
		"title":       "EliasDB API",
		"description": "Query and modify the EliasDB datastore.",
		"version":     APIVersion,
	}
}
