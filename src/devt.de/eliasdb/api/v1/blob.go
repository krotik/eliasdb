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
Package v1 contains EliasDB REST API Version 1.

Binary Blob control endpoint

/blob

The blob endpoint can be used to store and retrieve binary data to/from automatically
allocated storage locatons.

A new binary blob can be stored by sending a POST request. The body should
be the binary data to store. The response should have the following structure:

	{
		id : <Id of the stored binary blob>
	}

/blob/<id>

GET requests can be used to retrieve a binary blobs with a specific id. Binary blobs
can be updated by sending a PUT request and removed by sending a DELETE request.


Cluster control endpoint

/cluster

The cluster endpoint returns cluster state specific information. A GET request
returns the cluster state info as a key-value map:

	{
	    <stateinfo key> : <info value>,
	    ...
	}

/cluster/join

An instance can join an existing cluster by sending a PUT request to the join
endpoint. The body should have the following datastructure:

	{
		name    : <Name of an existing cluster member>,
		netaddr : <Network address of an existing cluster member>
	}

/cluster/eject

A cluster member can eject another cluster member or itself by sending a PUT
request to the eject endpoint. The body should have the following datastructure:

	{
		name    : <Name the cluster member to eject>,
	}

/cluster/ping

An instance can ping another instance (provided the secret is correct). Cluster
membership is not required for this command. The body should have the following datastructure:

	{
		name    : <Name of an existing instance>,
		netaddr : <Network address of an existing instance>
	}

/cluster/memberinfos

The memberinfos endpoint returns the static member info of every known cluster
member. If a member is not reachable its info contains a single key-value pair with
the key error and an error message as value. A GET request returns the member
info of every member as a key-value map:

	{
	    <memberinfo key> : <memberinfo value>,
	    ...
	}

/cluster/log

Returns the latest cluster related log messages. A DELETE call will clear
the current log.


EQL parser endpoint

/eql

The EQL endpoint provides direct access to the EQL parser. It can be used
to parse a given EQL query into an Abstract Syntax Tree or pretty print a
given Abstract Syntax Tree into an EQL query.

A query can be parsed into an Abstract Syntax Tree by sending a POST request. The
body should have the following format:

	{
		query : <Query to parse>
	}

Returns a JSON structure or an error message.

	{
		ast : <AST of the given query>
	}

An Abstract Syntax Tree can be pretty printed into a query by sending a POST request.
The body should have the following format:

	{
		ast : <AST to pretty print>
	}

Returns a JSON structure or an error message.

	{
		query : <Pretty printed query>
	}


Graph request endpoint

/graph

The graph endpoint is the main entry point to send and request graph data.

Data can be send by using POST and PUT requests. POST will store
data in the datastore and always overwrite any existing data. PUT requests on
nodes will only update the given attributes. PUT requests on edges are handled
equally to POST requests. Data can be deleted using DELETE requests. The data
structure for DELETE requests requires only the key and kind attributes.

A PUT, POST or DELETE request should be send to one of the following
endpoints:

/graph/<partition>

A graph with the following datastructure:

	{
		nodes : [ { <attr> : <value> }, ... ],
		edges : [ { <attr> : <value> }, ... ]
	}

/graph/<partition>/n

A list of nodes:

	[ { <attr> : <value> }, ... ]

/graph/<partition>/e

A list of edges:

	[ { <attr> : <value> }, ... ]

GET requests can be used to query single or a series of nodes. The endpoints
support the limit and offset parameters for lists:

	limit  - How many list items to return
	offset - Offset in the dataset (0 to <total count>-1)

The total number of entries is returned in the X-Total-Count header when
a list is returned.

/graph/<partition>/n/<node kind>/[node key]/[traversal spec]

/graph/<partition>/e/<edge kind>/<edge key>

The return data is a list of objects unless a specific node / edge or a traversal
from a specific node is requested. Each object in the list models a node or edge.

	[{
	    key : <value>,
		...
	}]

If a specifc object is requested then the return data is a single object.

	{
	    key : <value>,
	    ...
	}

Traversals return two lists containing traversed nodes and edges. The traversal
endpoint does NOT support limit and offset parameters. Also the X-Total-Count
header is not set.

	[
	    [ <traversed nodes> ], [ <traversed edges> ]
	]


Index query endpoint

/index

The index query endpoint should be used to run index search queries against
partitions. Index queries look for words or phrases on all nodes of a given
node kind.

A phrase query finds all nodes/edges where an attribute contains a
certain phrase. A request url which runs a new phrase search should be of the
following form:

/index/<partition>/n/<node kind>?phrase=<phrase>&attr=<attribute>

/index/<partition>/e/<edge kind>?phrase=<phrase>&attr=<attribute>

The return data is a list of node keys:

	[ <node key1>, <node key2>, ... ]

A word query finds all nodes/edges where an attribute contains a certain word.
A request url which runs a new word search should be of the following form:

/index/<partition>/n/<node kind>?word=<word>&attr=<attribute>

/index/<partition>/e/<edge kind>?word=<word>&attr=<attribute>

The return data is a map which maps node key to a list of word positions:

	{
	    key : [ <pos1>, <pos2>, ... ],
	    ...
	}

A value search finds all nodes/edges where an attribute has a certain value.
A request url which runs a new value search should be of the following form:

/index/<partition>/n/<node kind>?value=<value>&attr=<attribute>

/index/<partition>/e/<edge kind>?value=<value>&attr=<attribute>

The return data is a list of node keys:

	[ <node key1>, <node key2>, ... ]


General database information endpoint

/info

The info endpoint returns general database information such as known
node kinds, known attributes, etc ..

The return data is a key-value map:

	{
	    <info name> : <info value>,
	    ...
	}

/info/kind/<kind>

The node kind info endpoint returns general information about a known node or
edge kind such as known attributes or known edges.


Query endpoint

/query

The query endpoint should be used to run EQL search queries against partitions.
The return value is always a list (even if there is only a single entry).

A query result gets an ID and is stored in a cache. The id is returned in the
X-Cache-Id header. Subsequent requests for the same result can use the id
instead of a query.

The endpoint supports the optional limit and offset parameter:

	limit  - How many list items to return
	offset - Offset in the dataset

The total number of entries in the result is returned in the X-Total-Count header.
A request url which runs a new query should be of the following form:

/query/<partition>?q=<query>

/query/<partition>?rid=<result id>

The return data is a result object:

	{
	    header  : {
	        labels       : All column labels of the search result.
	        format       : All column format definitions of the search result.
	        data         : The data which is displayed in each column of the search result.
	                       (e.g. 1:n:name - Name of starting nodes,
	                             3:e:key  - Key of edge traversed in the second traversal)
	        primary_kind : The primary kind of the search result.
	    },
	    rows    : [ [ <col1>, <col2>, ... ] ],
	    sources : [ [ <src col1>, <src col2>, ... ] ],
	}
*/
package v1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"devt.de/eliasdb/api"
	"devt.de/eliasdb/storage"
)

/*
StorageSuffixBlob is the suffix for binary blob storage
*/
const StorageSuffixBlob = ".blob"

/*
EndpointBlob is the blob endpoint URL (rooted). Handles everything under blob/...
*/
const EndpointBlob = api.APIRoot + APIv1 + "/blob/"

/*
BlobEndpointInst creates a new endpoint handler.
*/
func BlobEndpointInst() api.RestEndpointHandler {
	return &blobEndpoint{}
}

/*
Handler object for blob operations.
*/
type blobEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles REST calls to retrieve binary data.
*/
func (be *blobEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	var res interface{}
	var ret []byte

	// Check parameters

	if !checkResources(w, resources, 2, 2, "Need a partition and a specific data id") {
		return
	}

	loc, err := strconv.ParseUint(resources[1], 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprint("Could not decode data id: ", err.Error()),
			http.StatusBadRequest)
		return
	}

	sm := api.GS.StorageManager(resources[0]+StorageSuffixBlob, false)

	if sm != nil {

		res, err = sm.FetchCached(loc)

		if err == storage.ErrNotInCache {
			err = sm.Fetch(loc, &ret)
		} else if err == nil && res != nil {
			ret = res.([]byte)
		}
	}

	// Write data

	w.Header().Set("content-type", "application/octet-stream")
	w.Write(ret)
}

/*
HandlePOST handles a REST call to store new binary data.
*/
func (be *blobEndpoint) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {
	var buf bytes.Buffer

	// Check parameters

	if !checkResources(w, resources, 1, 1, "Need a partition") {
		return
	}

	sm := api.GS.StorageManager(resources[0]+StorageSuffixBlob, true)

	// Use a memory buffer to read send data

	buf.ReadFrom(r.Body)

	loc, err := sm.Insert(buf.Bytes())

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sm.Flush()

	// Write data

	w.Header().Set("content-type", "application/json; charset=utf-8")

	ret := json.NewEncoder(w)
	ret.Encode(map[string]interface{}{
		"id": loc,
	})
}

/*
HandlePUT handles a REST call to update existing binary data.
*/
func (be *blobEndpoint) HandlePUT(w http.ResponseWriter, r *http.Request, resources []string) {
	var buf bytes.Buffer

	// Check parameters

	if !checkResources(w, resources, 2, 2, "Need a partition and a specific data id") {
		return
	}

	loc, err := strconv.ParseUint(resources[1], 10, 64)

	if err != nil {
		http.Error(w, fmt.Sprint("Could not decode data id: ", err.Error()), http.StatusBadRequest)
		return
	}

	sm := api.GS.StorageManager(resources[0]+StorageSuffixBlob, false)

	if sm != nil {

		// Use a memory buffer to read send data

		buf.ReadFrom(r.Body)

		if err := sm.Update(loc, buf.Bytes()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		sm.Flush()
	}
}

/*
HandleDELETE handles a REST call to remove existing binary data.
*/
func (be *blobEndpoint) HandleDELETE(w http.ResponseWriter, r *http.Request, resources []string) {

	// Check parameters

	if !checkResources(w, resources, 2, 2, "Need a partition and a specific data id") {
		return
	}

	loc, err := strconv.ParseUint(resources[1], 10, 64)

	if err != nil {
		http.Error(w, fmt.Sprint("Could not decode data id: ", err.Error()), http.StatusBadRequest)
		return
	}

	sm := api.GS.StorageManager(resources[0]+StorageSuffixBlob, false)

	if sm != nil {

		if err := sm.Free(loc); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		sm.Flush()
	}
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (be *blobEndpoint) SwaggerDefs(s map[string]interface{}) {

	idParams := []map[string]interface{}{
		map[string]interface{}{
			"name":        "id",
			"in":          "path",
			"description": "Id of the binary blob.",
			"required":    true,
			"type":        "string",
		},
	}

	partitionParams := []map[string]interface{}{
		map[string]interface{}{
			"name":        "partition",
			"in":          "path",
			"description": "Partition to select.",
			"required":    true,
			"type":        "string",
		},
	}

	binaryData := []map[string]interface{}{
		map[string]interface{}{
			"name":        "data",
			"in":          "body",
			"description": "The data to store.",
			"required":    true,
			"schema": map[string]interface{}{
				"description": "A blob of binary data.",
			},
		},
	}

	s["paths"].(map[string]interface{})["/v1/blob/{partition}"] = map[string]interface{}{
		"post": map[string]interface{}{
			"summary":     "Create a binary blob of data.",
			"description": "The blob endpoint can be used to store binary data. Its location will be automatically allocated.",
			"consumes": []string{
				"application/octet-stream",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(binaryData, partitionParams...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "The operation was successful.",
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"id": map[string]interface{}{
								"description": "The data id which can be used to lookup the data.",
								"type":        "number",
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

	s["paths"].(map[string]interface{})["/v1/blob/{partition}/{id}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Retrieve a binary blob of data.",
			"description": "The blob endpoint can be used to retrieve binary data from a specific location.",
			"produces": []string{
				"text/plain",
				"application/octet-stream",
			},
			"parameters": append(idParams, partitionParams...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "The requested binary blob.",
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
		"put": map[string]interface{}{
			"summary":     "Update a binary blob of data.",
			"description": "The blob endpoint can be used to update binary data at a specific location.",
			"produces": []string{
				"text/plain",
			},
			"parameters": append(idParams, partitionParams...),
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
		"delete": map[string]interface{}{
			"summary":     "Remove a binary blob of data.",
			"description": "The blob endpoint can be used to remove binary data from a specific location.",
			"produces": []string{
				"text/plain",
			},
			"parameters": append(idParams, partitionParams...),
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

	// Add generic error object to definition

	s["definitions"].(map[string]interface{})["Error"] = map[string]interface{}{
		"description": "A human readable error mesage.",
		"type":        "string",
	}
}
