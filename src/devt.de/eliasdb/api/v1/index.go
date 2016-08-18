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
REST endpoint to handle index queries.

/index

The index query endpoint should be used to run index search queries against partitions.
Index queries look for words or phrases on all nodes of a given node kind.

A request to phrase finds all nodes/edges where an attribute contains a certain phrase.
A request url which runs a new phrase search should be of the following form:

/index/<partition>/n/<node kind>?phrase=<phrase>&attr=<attribute>
/index/<partition>/e/<edge kind>?phrase=<phrase>&attr=<attribute>

The return data is a list of node keys:

[ <node key1>, <node key2>, ... ]

A request to word finds all nodes/edges where an attribute contains a certain word.
A request url which runs a new word search should be of the following form:

/index/<partition>/n/<node kind>?word=<word>&attr=<attribute>
/index/<partition>/e/<edge kind>?word=<word>&attr=<attribute>

The return data is a map which maps node key to a list of word positions:

{
	<node key> : [ <pos1>, <pos2>, ... ],
	...
}

A request to value finds all nodes/edges where an attribute has a certain value.
A request url which runs a new value search should be of the following form:

/index/<partition>/n/<node kind>?value=<value>&attr=<attribute>
/index/<partition>/e/<edge kind>?value=<value>&attr=<attribute>

The return data is a list of node keys:

[ <node key1>, <node key2>, ... ]
*/
package v1

import (
	"encoding/json"
	"net/http"

	"devt.de/eliasdb/api"
	"devt.de/eliasdb/graph"
)

/*
Query endpoint definition (rooted). Handles everything under index/...
*/
const ENDPOINT_INDEX_QUERY = api.API_ROOT + API_VERSION_V1 + "/index/"

/*
IndexEndpointInst creates a new endpoint handler.
*/
func IndexEndpointInst() api.RestEndpointHandler {
	return &indexEndpoint{}
}

/*
Handler object for search queries.
*/
type indexEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles a search query REST call.
*/
func (eq *indexEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {

	var err error

	// Check parameters

	if !checkResources(w, resources, 3, 3, "Need a partition, entity type (n or e) and a kind") {
		return
	}

	if resources[1] != "n" && resources[1] != "e" {
		http.Error(w, "Entity type must be n (nodes) or e (edges)", http.StatusBadRequest)
		return
	}

	// Check what is queried

	attr := r.URL.Query().Get("attr")
	if attr == "" {
		http.Error(w, "Query string for attr (attribute) is required", http.StatusBadRequest)
		return
	}

	phrase := r.URL.Query().Get("phrase")
	word := r.URL.Query().Get("word")
	value := r.URL.Query().Get("value")

	// Get the index query object

	var iq graph.IndexQuery

	if resources[1] == "n" {
		iq, err = api.GM.NodeIndexQuery(resources[0], resources[2])
	} else {
		iq, err = api.GM.EdgeIndexQuery(resources[0], resources[2])
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if iq == nil {
		http.Error(w, "Unknown partition or node kind", http.StatusBadRequest)
		return
	}

	// Do the lookup

	var data interface{}

	switch {
	case phrase != "":
		data, err = iq.LookupPhrase(attr, phrase)
		if len(data.([]string)) == 0 {
			data = []string{}
		}
	case word != "":
		data, err = iq.LookupWord(attr, word)
		if len(data.(map[string][]uint64)) == 0 {
			data = map[string][]uint64{}
		}
	case value != "":
		data, err = iq.LookupValue(attr, value)
		if len(data.([]string)) == 0 {
			data = []string{}
		}
	default:
		http.Error(w, "Query string for either phrase, word or value is required", http.StatusBadRequest)
		return
	}

	// Check if there was an error

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write data

	w.Header().Set("content-type", "application/json; charset=utf-8")

	ret := json.NewEncoder(w)
	ret.Encode(data)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (ge *indexEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/v1/index/{partition}/{entity_type}/{kind}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Run index searches on the EliasDB datastore.",
			"description": "The query endpoint should be used to run index searches for either a word, phrase or a whole value. All queries must specify a kind and an node/edge attribute.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": []map[string]interface{}{
				map[string]interface{}{
					"name":        "partition",
					"in":          "path",
					"description": "Partition to query.",
					"required":    true,
					"type":        "string",
				},
				map[string]interface{}{
					"name": "entity_type",
					"in":   "path",
					"description": "Datastore entity type which should selected. " +
						"Either n for nodes or e for edges.",
					"required": true,
					"type":     "string",
				},
				map[string]interface{}{
					"name":        "kind",
					"in":          "path",
					"description": "Node or edge kind to be queried.",
					"required":    true,
					"type":        "string",
				},
				map[string]interface{}{
					"name":        "attr",
					"in":          "query",
					"description": "Attribute which should contain the word, phrase or value.",
					"required":    true,
					"type":        "string",
				},
				map[string]interface{}{
					"name":        "word",
					"in":          "query",
					"description": "Word to search for in word queries.",
					"required":    false,
					"type":        "string",
				},
				map[string]interface{}{
					"name":        "phrase",
					"in":          "query",
					"description": "Phrase to search for in phrase queries.",
					"required":    false,
					"type":        "string",
				},
				map[string]interface{}{
					"name":        "value",
					"in":          "query",
					"description": "Value (node/edge attribute value) to search for in value queries.",
					"required":    false,
					"type":        "string",
				},
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "A list of keys or when doing a word search a map with node/edge key to word positions.",
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
