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
	"sort"
	"strconv"

	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
EndpointGraph is the graph endpoint URL (rooted). Handles everything under graph/...
*/
const EndpointGraph = api.APIRoot + APIv1 + "/graph/"

/*
GraphEndpointInst creates a new endpoint handler.
*/
func GraphEndpointInst() api.RestEndpointHandler {
	return &graphEndpoint{}
}

/*
Handler object for graph operations.
*/
type graphEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles REST calls to retrieve data from the graph database.
*/
func (ge *graphEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {

	// Check parameters

	if !checkResources(w, resources, 3, 5, "Need a partition, entity type (n or e) and a kind; optional key and traversal spec") {
		return
	}

	if resources[1] != "n" && resources[1] != "e" {
		http.Error(w, "Entity type must be n (nodes) or e (edges)", http.StatusBadRequest)
		return
	}

	if len(resources) == 3 {

		// Iterate over a list of nodes

		if resources[1] == "n" {

			// Get limit parameter; -1 if not set

			limit, ok := queryParamPosNum(w, r, "limit")
			if !ok {
				return
			}

			// Get offset parameter; -1 if not set

			offset, ok := queryParamPosNum(w, r, "offset")
			if !ok {
				return
			}

			it, err := api.GM.NodeKeyIterator(resources[0], resources[2])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else if it == nil {
				http.Error(w, "Unknown partition or node kind", http.StatusBadRequest)
				return
			}

			i := 0

			if offset != -1 {

				for i = 0; i < offset; i++ {
					if !it.HasNext() {
						http.Error(w, "Offset exceeds available nodes", http.StatusInternalServerError)
						return
					}

					if it.Next(); it.LastError != nil {
						http.Error(w, it.LastError.Error(), http.StatusInternalServerError)
						return
					}
				}

			} else {

				offset = 0
			}

			var data []interface{}

			if limit == -1 {
				data = make([]interface{}, 0)
			} else {
				data = make([]interface{}, 0, limit)
			}

			for i = offset; it.HasNext(); i++ {

				// Break out if the limit was reached

				if limit != -1 && i > offset+limit-1 {
					break
				}

				key := it.Next()

				if it.LastError != nil {
					http.Error(w, it.LastError.Error(), http.StatusInternalServerError)
					return
				}

				node, err := api.GM.FetchNode(resources[0], key, resources[2])

				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				data = append(data, node.Data())
			}

			// Set total count header

			w.Header().Add(HTTPHeaderTotalCount, strconv.FormatUint(api.GM.NodeCount(resources[2]), 10))

			// Write data

			w.Header().Set("content-type", "application/json; charset=utf-8")

			ret := json.NewEncoder(w)
			ret.Encode(data)

		} else {
			http.Error(w, "Entity type must be n (nodes) when requesting all items", http.StatusBadRequest)
			return
		}

	} else if len(resources) == 4 {

		// Fetch a specific node or relationship

		var data map[string]interface{}

		if resources[1] == "n" {

			node, err := api.GM.FetchNode(resources[0], resources[3], resources[2])

			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else if node == nil {
				http.Error(w, "Unknown partition or node kind", http.StatusBadRequest)
				return
			}

			data = node.Data()

		} else {

			edge, err := api.GM.FetchEdge(resources[0], resources[3], resources[2])

			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else if edge == nil {
				http.Error(w, "Unknown partition or edge kind", http.StatusBadRequest)
				return
			}

			data = edge.Data()
		}

		// Write data

		w.Header().Set("content-type", "application/json; charset=utf-8")

		ret := json.NewEncoder(w)
		ret.Encode(data)

	} else {

		if resources[1] == "n" {

			node, err := api.GM.FetchNodePart(resources[0], resources[3], resources[2], []string{"key", "kind"})

			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else if node == nil {
				http.Error(w, "Unknown partition or node kind", http.StatusBadRequest)
				return
			}

			nodes, edges, err := api.GM.TraverseMulti(resources[0], resources[3],
				resources[2], resources[4], true)

			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			data := make([][]map[string]interface{}, 2)

			dataNodes := make([]map[string]interface{}, 0, len(nodes))
			dataEdges := make([]map[string]interface{}, 0, len(edges))

			if nodes != nil && edges != nil {
				for i, n := range nodes {
					e := edges[i]

					dataNodes = append(dataNodes, n.Data())
					dataEdges = append(dataEdges, e.Data())
				}
			}

			data[0] = dataNodes
			data[1] = dataEdges

			// Sort the result

			sort.Stable(&traversalResultComparator{data})

			// Write data

			w.Header().Set("content-type", "application/json; charset=utf-8")

			ret := json.NewEncoder(w)
			ret.Encode(data)

		} else {
			http.Error(w, "Entity type must be n (nodes) when requesting traversal results", http.StatusBadRequest)
			return
		}
	}
}

/*
HandlePUT handles a REST call to insert new elements into the graph or update
existing elements. Nodes are updated if they already exist. Edges are replaced
if they already exist.
*/
func (ge *graphEndpoint) HandlePUT(w http.ResponseWriter, r *http.Request, resources []string) {
	ge.handleGraphRequest(w, r, resources,
		func(trans graph.Trans, part string, node data.Node) error {
			return trans.UpdateNode(part, node)
		},
		func(trans graph.Trans, part string, edge data.Edge) error {
			return trans.StoreEdge(part, edge)
		})
}

/*
HandlePOST handles a REST call to insert new elements into the graph or update
existing elements. Nodes and edges are replaced if they already exist.
*/
func (ge *graphEndpoint) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {
	ge.handleGraphRequest(w, r, resources,
		func(trans graph.Trans, part string, node data.Node) error {
			return trans.StoreNode(part, node)
		},
		func(trans graph.Trans, part string, edge data.Edge) error {
			return trans.StoreEdge(part, edge)
		})
}

/*
HandleDELETE handles a REST call to delete elements from the graph.
*/
func (ge *graphEndpoint) HandleDELETE(w http.ResponseWriter, r *http.Request, resources []string) {
	ge.handleGraphRequest(w, r, resources,
		func(trans graph.Trans, part string, node data.Node) error {
			return trans.RemoveNode(part, node.Key(), node.Kind())
		},
		func(trans graph.Trans, part string, edge data.Edge) error {
			return trans.RemoveEdge(part, edge.Key(), edge.Kind())
		})
}

/*
handleGraphRequest handles a graph query REST call.
*/
func (ge *graphEndpoint) handleGraphRequest(w http.ResponseWriter, r *http.Request, resources []string,
	transFuncNode func(trans graph.Trans, part string, node data.Node) error,
	transFuncEdge func(trans graph.Trans, part string, edge data.Edge) error) {

	var nDataList []map[string]interface{}
	var eDataList []map[string]interface{}

	// Check parameters

	if !checkResources(w, resources, 1, 2, "Need a partition; optional entity type (n or e)") {
		return
	}

	dec := json.NewDecoder(r.Body)

	if len(resources) == 1 {

		// No explicit type given - expecting a graph

		gdata := make(map[string][]map[string]interface{})

		if err := dec.Decode(&gdata); err != nil {
			http.Error(w, "Could not decode request body as object with list of nodes and/or edges: "+err.Error(), http.StatusBadRequest)
			return
		}

		nDataList = gdata["nodes"]
		eDataList = gdata["edges"]

	} else if resources[1] == "n" {

		nDataList = make([]map[string]interface{}, 1)

		if err := dec.Decode(&nDataList); err != nil {
			http.Error(w, "Could not decode request body as list of nodes: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else if resources[1] == "e" {

		eDataList = make([]map[string]interface{}, 1)

		if err := dec.Decode(&eDataList); err != nil {
			http.Error(w, "Could not decode request body as list of edges: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Create a transaction

	trans := graph.NewGraphTrans(api.GM)

	if nDataList != nil {

		// Store nodes in transaction

		for _, ndata := range nDataList {
			node := data.NewGraphNodeFromMap(ndata)

			if err := transFuncNode(trans, resources[0], node); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	if eDataList != nil {

		// Store edges in transaction

		for _, edata := range eDataList {
			edge := data.NewGraphEdgeFromNode(data.NewGraphNodeFromMap(edata))

			if err := transFuncEdge(trans, resources[0], edge); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	// Commit transaction

	if err := trans.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (ge *graphEndpoint) SwaggerDefs(s map[string]interface{}) {

	partitionParams := []map[string]interface{}{
		{
			"name":        "partition",
			"in":          "path",
			"description": "Partition to select.",
			"required":    true,
			"type":        "string",
		},
	}

	entityParams := []map[string]interface{}{
		{
			"name": "entity_type",
			"in":   "path",
			"description": "Datastore entity type which should selected. " +
				"Either n for nodes or e for edges.",
			"required": true,
			"type":     "string",
		},
	}

	defaultParams := []map[string]interface{}{
		{
			"name":        "kind",
			"in":          "path",
			"description": "Node or edge kind to be queried.",
			"required":    true,
			"type":        "string",
		},
	}
	defaultParams = append(defaultParams, partitionParams...)
	defaultParams = append(defaultParams, entityParams...)

	optionalQueryParams := []map[string]interface{}{
		{
			"name":        "limit",
			"in":          "query",
			"description": "How many list items to return.",
			"required":    false,
			"type":        "number",
			"format":      "integer",
		},
		{
			"name":        "offset",
			"in":          "query",
			"description": "Offset in the dataset.",
			"required":    false,
			"type":        "number",
			"format":      "integer",
		},
	}

	keyParam := []map[string]interface{}{
		{
			"name":        "key",
			"in":          "path",
			"description": "Node or edge key to be queried.",
			"required":    true,
			"type":        "string",
		},
	}

	travParam := []map[string]interface{}{
		{
			"name":        "traversal_spec",
			"in":          "path",
			"description": "Traversal to be followed from a single node.",
			"required":    true,
			"type":        "string",
		},
	}

	graphPost := []map[string]interface{}{
		{
			"name":        "entities",
			"in":          "body",
			"description": "Nodes and Edges which should be stored",
			"required":    true,
			"schema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"nodes": map[string]interface{}{
						"description": "List of nodes to be inserted / updated.",
						"type":        "array",
						"items": map[string]interface{}{
							"description": "Node to be inserted / updated.",
							"type":        "object",
						},
					},
					"edges": map[string]interface{}{
						"description": "List of edges to be inserted / updated.",
						"type":        "array",
						"items": map[string]interface{}{
							"description": "Edge to be inserted / updated.",
							"type":        "object",
						},
					},
				},
			},
		},
	}

	entitiesPost := []map[string]interface{}{
		{
			"name":        "entities",
			"in":          "body",
			"description": "Nodes or Edges which should be stored",
			"required":    true,
			"schema": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"description": "Node or edge to be inserted / updated.",
					"type":        "object",
				},
			},
		},
	}

	defaultError := map[string]interface{}{
		"description": "Error response",
		"schema": map[string]interface{}{
			"$ref": "#/definitions/Error",
		},
	}

	// Add endpoint to insert a graph with nodes and edges

	s["paths"].(map[string]interface{})["/v1/graph/{partition}"] = map[string]interface{}{
		"post": map[string]interface{}{
			"summary": "Data can be send by using POST requests.",
			"description": "A whole graph can be send. " +
				"POST will store data in the datastore and always overwrite any existing data.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(partitionParams, graphPost...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "No data is returned when data is created.",
				},
				"default": defaultError,
			},
		},
		"put": map[string]interface{}{
			"summary": "Data can be send by using PUT requests.",
			"description": "A whole graph can be send. " +
				"PUT will store data in the datastore and update existing data.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(partitionParams, graphPost...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "No data is returned when data is created.",
				},
				"default": defaultError,
			},
		},
		"delete": map[string]interface{}{
			"summary": "Data can be send by using DELETE requests.",
			"description": "A whole graph can be send. " +
				"DELETE will delete data in the datastore - only key and kind are required.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(partitionParams, graphPost...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "No data is returned when data is created.",
				},
				"default": defaultError,
			},
		},
	}

	// Add endpoint to insert nodes / edges

	s["paths"].(map[string]interface{})["/v1/graph/{partition}/{entity_type}"] = map[string]interface{}{
		"post": map[string]interface{}{
			"summary": "Data can be send by using POST requests.",
			"description": "A list of nodes / edges can be send. " +
				"POST will store data in the datastore and always overwrite any existing data.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(append(partitionParams, entityParams...), entitiesPost...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "No data is returned when data is created.",
				},
				"default": defaultError,
			},
		},
		"put": map[string]interface{}{
			"summary": "Data can be send by using PUT requests.",
			"description": "A list of nodes / edges can be send. " +
				"PUT will store data in the datastore and update existing data.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(append(partitionParams, entityParams...), entitiesPost...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "No data is returned when data is created.",
				},
				"default": defaultError,
			},
		},
		"delete": map[string]interface{}{
			"summary": "Data can be send by using DELETE requests.",
			"description": "A list of nodes / edges can be send. " +
				"DELETE will delete data in the datastore - only key and kind are required.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(append(partitionParams, entityParams...), entitiesPost...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "No data is returned when data is created.",
				},
				"default": defaultError,
			},
		},
	}

	// Add endpoint to query nodes for a specific node kind

	s["paths"].(map[string]interface{})["/v1/graph/{partition}/{entity_type}/{kind}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary": "The graph endpoint is the main entry point to request data.",
			"description": "GET requests can be used to query a series of nodes. " +
				"The X-Total-Count header contains the total number of nodes which were found.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(defaultParams, optionalQueryParams...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "The return data is a list of objects",
					"schema": map[string]interface{}{
						"type": "array",
						"items": map[string]interface{}{
							"type": "object",
						},
					},
				},
				"default": defaultError,
			},
		},
	}

	// Add endpoint to query/create a specific node

	s["paths"].(map[string]interface{})["/v1/graph/{partition}/{entity_type}/{kind}/{key}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "The graph endpoint is the main entry point to request data.",
			"description": "GET requests can be used to query a single node.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(append(defaultParams, keyParam...), optionalQueryParams...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "The return data is a single object",
					"schema": map[string]interface{}{
						"type": "object",
					},
				},
				"default": defaultError,
			},
		},
	}

	// Add endpoint to traverse from a single node

	s["paths"].(map[string]interface{})["/v1/graph/{partition}/{entity_type}/{kind}/{key}/{traversal_spec}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "The graph endpoint is the main entry point to request data.",
			"description": "GET requests can be used to query a single node and then traverse to its neighbours.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(append(defaultParams, keyParam...), travParam...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "The return data are two lists containing traversed nodes and edges. " +
						"The traversal endpoint does NOT support limit and offset parameters. " +
						"Also the X-Total-Count header is not set.",
					"schema": map[string]interface{}{
						"type": "array",
						"items": map[string]interface{}{
							"type": "array",
							"items": map[string]interface{}{
								"type": "object",
							},
						},
					},
				},
				"default": defaultError,
			},
		},
	}
}

// Comparator object to sort traversal results

type traversalResultComparator struct {
	Data [][]map[string]interface{} // Data to sort
}

func (c traversalResultComparator) Len() int {
	return len(c.Data[0])
}

func (c traversalResultComparator) Less(i, j int) bool {
	c1 := c.Data[0][i]
	c2 := c.Data[0][j]

	return fmt.Sprintf("%v", c1[data.NodeKey]) < fmt.Sprintf("%v", c2[data.NodeKey])
}

func (c traversalResultComparator) Swap(i, j int) {
	c.Data[0][i], c.Data[0][j] = c.Data[0][j], c.Data[0][i]
	c.Data[1][i], c.Data[1][j] = c.Data[1][j], c.Data[1][i]
}
