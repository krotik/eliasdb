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
RESTful API for EliasDB. This REST API provides an interface to EliasDB.
It allows querying and modifying of the datastore. The API responds to GET,
POST, PUT and DELETE requests in JSON if the request was successful
(Return code 200 OK) and plain text in all other cases.

Version 1 API root: /db/v1

API endpoints:

/db/v1/graph - Access to the graph
/db/v1/query - Run a search query
*/
package api

import (
	"net/http"
	"strings"

	"devt.de/eliasdb/graph"
)

/*
API root directory for the REST API
*/
const API_ROOT = "/db"

/*
Supported schemes by the API
*/
var API_SCHEMES = []string{"https"}

/*
Host definition for REST API
*/
var API_HOST = "localhost:9090"

/*
RestEndpointInst models a factory function for REST endpoint handlers.
*/
type RestEndpointInst func() RestEndpointHandler

/*
RestEndpointHandler models a REST endpoint handler.
*/
type RestEndpointHandler interface {

	/*
		HandleGET handles a GET request.
	*/
	HandleGET(w http.ResponseWriter, r *http.Request, resources []string)

	/*
		HandlePOST handles a POST request.
	*/
	HandlePOST(w http.ResponseWriter, r *http.Request, resources []string)

	/*
		HandlePUT handles a PUT request.
	*/
	HandlePUT(w http.ResponseWriter, r *http.Request, resources []string)

	/*
		HandleDELETE handles a DELETE request.
	*/
	HandleDELETE(w http.ResponseWriter, r *http.Request, resources []string)

	/*
		SwaggerDefs is used to describe the endpoint in swagger.
	*/
	SwaggerDefs(s map[string]interface{})
}

/*
GraphManager instance which should be used by the REST API.
*/
var GM *graph.GraphManager

/*
Map of all registered endpoint handlers.
*/
var registered = map[string]RestEndpointInst{}

/*
HandleFunc to use for registering handlers
*/
var HandleFunc func(pattern string, handler func(http.ResponseWriter, *http.Request)) = http.HandleFunc

/*
Register all given REST endpoint handlers.
*/
func RegisterRestEndpoints(endpointInsts map[string]RestEndpointInst) {

	for url, endpointInst := range endpointInsts {
		registered[url] = endpointInst

		HandleFunc(url, func() func(w http.ResponseWriter, r *http.Request) {

			var handlerURL = url
			var handlerInst = endpointInst

			return func(w http.ResponseWriter, r *http.Request) {

				// Create a new handler instance

				handler := handlerInst()

				// Handle request in appropriate method

				res := strings.TrimSpace(r.URL.Path[len(handlerURL):])

				if len(res) > 0 && res[len(res)-1] == '/' {
					res = res[:len(res)-1]
				}

				var resources []string

				if res != "" {
					resources = strings.Split(res, "/")
				}

				switch r.Method {
				case "GET":
					handler.HandleGET(w, r, resources)

				case "POST":
					handler.HandlePOST(w, r, resources)

				case "PUT":
					handler.HandlePUT(w, r, resources)

				case "DELETE":
					handler.HandleDELETE(w, r, resources)

				default:
					http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
				}
			}
		}())
	}
}

/*
Default endpoint handler.
*/
type DefaultEndpointHandler struct {
}

func (de *DefaultEndpointHandler) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

func (de *DefaultEndpointHandler) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

func (de *DefaultEndpointHandler) HandlePUT(w http.ResponseWriter, r *http.Request, resources []string) {
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

func (de *DefaultEndpointHandler) HandleDELETE(w http.ResponseWriter, r *http.Request, resources []string) {
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}
