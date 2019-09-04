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
EndpointClusterQuery is the cluster endpoint URL (rooted). Handles everything under cluster/...
*/
const EndpointClusterQuery = api.APIRoot + APIv1 + "/cluster/"

/*
ClusterEndpointInst creates a new endpoint handler.
*/
func ClusterEndpointInst() api.RestEndpointHandler {
	return &clusterEndpoint{}
}

/*
Handler object for cluster queries.
*/
type clusterEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles a cluster query REST call.
*/
func (ce *clusterEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	var data interface{}

	// Check clustering is enabled

	if api.DD == nil || api.DDLog == nil {
		http.Error(w, "Clustering is not enabled on this instance", http.StatusServiceUnavailable)
		return
	}

	if len(resources) == 1 && resources[0] == "log" {

		// Cluster logs are requested

		data = api.DDLog.StringSlice()

	} else if len(resources) == 1 && resources[0] == "memberinfos" {

		// Cluster member infos are requested

		data = api.DD.MemberManager.MemberInfoCluster()

	} else {

		// By default the cluster state is returned

		data = api.DD.MemberManager.StateInfo().Map()
	}

	// Write data

	w.Header().Set("content-type", "application/json; charset=utf-8")

	ret := json.NewEncoder(w)
	ret.Encode(data)
}

/*
HandlePUT handles a REST call to join/eject/ping members of the cluster.
*/
func (ce *clusterEndpoint) HandlePUT(w http.ResponseWriter, r *http.Request, resources []string) {

	// Check parameters

	if !checkResources(w, resources, 1, 1, "Need a command either: join or eject") {
		return
	}

	dec := json.NewDecoder(r.Body)

	args := make(map[string]string)

	if err := dec.Decode(&args); err != nil {
		http.Error(w, "Could not decode arguments: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Function to check arguments

	getArg := func(arg string) (string, bool) {
		v, ok := args[arg]
		if !ok {
			http.Error(w, fmt.Sprintf("Required argument %v missing in body arguments", arg), http.StatusBadRequest)
		}
		return v, ok
	}

	if resources[0] == "join" {

		// Get required args

		name, ok := getArg("name")
		if ok {

			rpc, ok := getArg("netaddr")
			if ok {

				err := api.DD.MemberManager.JoinCluster(name, rpc)
				if err != nil {
					http.Error(w, "Could not join the cluster: "+err.Error(), http.StatusForbidden)
				}
			}
		}

	} else if resources[0] == "eject" {

		// Get required args

		name, ok := getArg("name")
		if ok {

			err := api.DD.MemberManager.EjectMember(name)
			if err != nil {
				http.Error(w, "Could not eject "+name+" from cluster: "+err.Error(), http.StatusForbidden)
			}
		}

	} else if resources[0] == "ping" {

		// Get required args

		name, ok := getArg("name")
		if ok {

			rpc, ok := getArg("netaddr")
			if ok {

				res, err := api.DD.MemberManager.Client.SendPing(name, rpc)

				if err != nil {
					http.Error(w, "Ping returned an error: "+err.Error(), http.StatusForbidden)
				} else {

					w.Header().Set("content-type", "application/json; charset=utf-8")

					ret := json.NewEncoder(w)
					ret.Encode(res)
				}
			}
		}

	} else {
		http.Error(w, "Unknown command: "+resources[0], http.StatusBadRequest)
	}
}

/*
HandleDELETE handles a cluster delete REST call.
*/
func (ce *clusterEndpoint) HandleDELETE(w http.ResponseWriter, r *http.Request, resources []string) {

	// Check clustering is enabled

	if api.DD == nil || api.DDLog == nil {
		http.Error(w, "Clustering is not enabled on this instance", http.StatusServiceUnavailable)
		return
	}

	if len(resources) == 1 && resources[0] == "log" {

		// Cluster log should be reset

		api.DDLog.Reset()

		return
	}

	http.Error(w, "Request had no effect", http.StatusBadRequest)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (ce *clusterEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/v1/cluster"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return cluster specific information.",
			"description": "The cluster endpoint returns the cluster state info which contains cluster members and their state.",
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

	s["paths"].(map[string]interface{})["/v1/cluster/{command}"] = map[string]interface{}{
		"put": map[string]interface{}{
			"summary":     "Commands can be given to the cluster by using PUT requests.",
			"description": "The cluster can be controlled via this command endpoint on any member.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": []map[string]interface{}{
				{
					"name":        "command",
					"in":          "path",
					"description": "Valid commands are: ping, join and eject.",
					"required":    true,
					"type":        "string",
				},
				{
					"name":        "args",
					"in":          "body",
					"description": "Arguments for a command",
					"required":    true,
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"name": map[string]interface{}{
								"description": "Name of a cluster member (ping/join=member to contact, eject=member to eject).",
								"type":        "string",
							},
							"netaddr": map[string]interface{}{
								"description": "Network address of a member e.g. localhost:9030 (ping/join=member address to contact)",
								"type":        "string",
							},
						},
					},
				},
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Only the ping command returns its result. All other positive responses are empty.",
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

	s["paths"].(map[string]interface{})["/v1/cluster/memberinfos"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return static member info of every known cluster member.",
			"description": "The memberinfos returns the static member info of every known cluster member. If a member is not reachable its info contains a single key-value pair with the key error and an error message as value.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "A map of memberinfos (keys are member names).",
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

	s["paths"].(map[string]interface{})["/v1/cluster/log"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return latest cluster related log messages.",
			"description": "The cluster log endpoint returns the latest cluster related log messages from a memory ring buffer.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "A list of log messages.",
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
		"delete": map[string]interface{}{
			"summary":     "Reset the cluster log.",
			"description": "A delete call to the log endpoint resets the cluster related log and clears the ring buffer in memory.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Cluster related log was reset.",
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
