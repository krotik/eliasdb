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
	"net/http"
	"strings"

	"devt.de/common/datautil"
	"devt.de/eliasdb/cluster"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/graphstorage"
)

/*
APIVersion is the version of the REST API
*/
const APIVersion = "1.0.0"

/*
APIRoot is the root directory for the REST API
*/
const APIRoot = "/db"

/*
APISchemes is a list of supported protocol schemes
*/
var APISchemes = []string{"https"}

/*
APIHost is the host definition for the REST API
*/
var APIHost = "localhost:9090"

/*
GeneralEndpointMap contains general endpoints which should always be available
*/
var GeneralEndpointMap = map[string]RestEndpointInst{
	EndpointAbout:   AboutEndpointInst,
	EndpointSwagger: SwaggerEndpointInst,
}

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
GM is the GraphManager instance which should be used by the REST API.
*/
var GM *graph.Manager

/*
GS is the GraphStorage instance which should be used by the REST API.
*/
var GS graphstorage.Storage

/*
DD is the DistributedStorage instance which should be used by the REST API.
(Only available if clustering is enabled.)
*/
var DD *cluster.DistributedStorage

/*
DDLog is a ringbuffer containing cluster related logs.
(Only available if clustering is enabled.)
*/
var DDLog *datautil.RingBuffer

/*
Map of all registered endpoint handlers.
*/
var registered = map[string]RestEndpointInst{}

/*
HandleFunc to use for registering handlers

Should be of type: func(pattern string, handler func(http.ResponseWriter, *http.Request))
*/
var HandleFunc = http.HandleFunc

/*
RegisterRestEndpoints registers all given REST endpoint handlers.
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
DefaultEndpointHandler represents the default endpoint handler.
*/
type DefaultEndpointHandler struct {
}

/*
HandleGET is a method stub returning an error.
*/
func (de *DefaultEndpointHandler) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

/*
HandlePOST is a method stub returning an error.
*/
func (de *DefaultEndpointHandler) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

/*
HandlePUT is a method stub returning an error.
*/
func (de *DefaultEndpointHandler) HandlePUT(w http.ResponseWriter, r *http.Request, resources []string) {
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

/*
HandleDELETE is a method stub returning an error.
*/
func (de *DefaultEndpointHandler) HandleDELETE(w http.ResponseWriter, r *http.Request, resources []string) {
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}
