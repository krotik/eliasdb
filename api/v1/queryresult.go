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
	"strings"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/eql"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
EndpointQueryResult is the query result endpoint URL (rooted). Handles everything under queryresult/...
*/
const EndpointQueryResult = api.APIRoot + APIv1 + "/queryresult/"

/*
QueryResultEndpointInst creates a new endpoint handler.
*/
func QueryResultEndpointInst() api.RestEndpointHandler {
	return &queryResultEndpoint{}
}

/*
Handler object for query result operations.
*/
type queryResultEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles info requests on query results.
*/
func (qre *queryResultEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	qre.handleRequest("get", w, r, resources)
}

/*
HandlePUT handles state changing operations on query results.
*/
func (qre *queryResultEndpoint) HandlePUT(w http.ResponseWriter, r *http.Request, resources []string) {
	qre.handleRequest("put", w, r, resources)
}

/*
HandlePOST handles state changing operations on query results.
*/
func (qre *queryResultEndpoint) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {
	qre.handleRequest("post", w, r, resources)
}

/*
HandleDELETE handles state changing operations on query results.
*/
func (qre *queryResultEndpoint) HandleDELETE(w http.ResponseWriter, r *http.Request, resources []string) {
	qre.handleRequest("delete", w, r, resources)
}

func (qre *queryResultEndpoint) handleRequest(requestType string, w http.ResponseWriter, r *http.Request, resources []string) {

	// Check parameters

	if !checkResources(w, resources, 2, 3, "Need a result ID and an operation") {
		return
	}

	// Limit is either not set (then -1) or a positive value

	limit, ok := queryParamPosNum(w, r, "limit")
	if !ok {
		return
	}

	resID := resources[0]
	op := resources[1]

	res, ok := ResultCache.Get(resID)
	if !ok {
		http.Error(w, "Unknown query result", http.StatusBadRequest)
		return
	}

	sres := res.(*APISearchResult)

	if op == "csv" {

		if requestType != "get" {
			http.Error(w, "Csv can only handle GET requests",
				http.StatusBadRequest)
			return
		}

		w.Header().Set("content-type", "text/plain; charset=utf-8")
		w.Write([]byte(sres.CSV()))

		return

	} else if op == "quickfilter" {

		qre.quickFilter(requestType, w, resources, sres, limit)

		return

	} else if op == "select" {

		qre.selectRows(requestType, w, resources, sres)

		return

	} else if op == "groupselected" {

		qre.groupSelected(requestType, w, r, resources, sres)

		return
	}

	http.Error(w, fmt.Sprintf("Unknown operation: %v", op), http.StatusBadRequest)
}

/*
groupSelected implements the adding/removing of all selected nodes to a group functionality.
*/
func (qre *queryResultEndpoint) groupSelected(requestType string, w http.ResponseWriter, r *http.Request,
	resources []string, sres *APISearchResult) {
	var col int
	var err error

	addNodeToGroup := func(trans graph.Trans, part, groupName, key, kind string) error {
		// Add to group

		edge := data.NewGraphEdge()

		edge.SetAttr("key", stringutil.MD5HexString(fmt.Sprintf("%s#%s#%s", key, kind, groupName)))
		edge.SetAttr("kind", "Containment")

		edge.SetAttr(data.EdgeEnd1Key, groupName)
		edge.SetAttr(data.EdgeEnd1Kind, eql.GroupNodeKind)
		edge.SetAttr(data.EdgeEnd1Role, "Container")
		edge.SetAttr(data.EdgeEnd1Cascading, false)

		edge.SetAttr(data.EdgeEnd2Key, key)
		edge.SetAttr(data.EdgeEnd2Kind, kind)
		edge.SetAttr(data.EdgeEnd2Role, "ContainedItem")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		return trans.StoreEdge(part, edge)
	}

	removeNodeFromGroup := func(trans graph.Trans, part, groupName, key, kind string) error {
		var nodes []data.Node
		var edges []data.Edge

		nodes, edges, err = api.GM.TraverseMulti(part, key, kind, ":::"+eql.GroupNodeKind, false)

		if err == nil {
			for i, n := range nodes {
				if n.Key() == groupName {
					errorutil.AssertOk(trans.RemoveEdge(part, edges[i].Key(), edges[i].Kind()))
				}
			}
		}

		return err
	}

	trans := graph.NewGraphTrans(api.GM)

	part := sres.Header().Partition()
	selections := sres.Selections()

	if col, err = sres.GetPrimaryNodeColumn(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(resources) == 3 {

		if requestType != "put" && requestType != "delete" {
			http.Error(w, "Groupselected for a specific group can only handle PUT and DELETE requests",
				http.StatusBadRequest)
			return
		}

		groupName := resources[2]

		for i, srcs := range sres.RowSources() {
			src := strings.Split(srcs[col], ":")
			kind := src[1]
			key := src[2]

			if selections[i] {

				// Add or remove form group

				if requestType == "put" {
					errorutil.AssertOk(addNodeToGroup(trans, part, groupName, key, kind))

				} else if requestType == "delete" {
					errorutil.AssertOk(removeNodeFromGroup(trans, part, groupName, key, kind))
				}
			}
		}

	} else {

		if requestType != "get" && requestType != "post" {
			http.Error(w, "Groupselected can only handle GET and POST requests",
				http.StatusBadRequest)
			return
		}

		if requestType == "post" {
			var reqGroups []interface{}
			var reqKeys, reqKinds []interface{}

			// Apply the given state

			gdata := make(map[string]interface{})

			// Parse the data

			dec := json.NewDecoder(r.Body)
			if err := dec.Decode(&gdata); err != nil {
				http.Error(w, "Could not decode request body as object with lists of groups, keys and kinds: "+err.Error(),
					http.StatusBadRequest)
				return
			}

			reqGroupsVal, ok1 := gdata["groups"]
			reqKeysVal, ok2 := gdata["keys"]
			reqKindsVal, ok3 := gdata["kinds"]

			if ok1 && ok2 && ok3 {
				reqGroups, ok1 = reqGroupsVal.([]interface{})
				reqKeys, ok2 = reqKeysVal.([]interface{})
				reqKinds, ok3 = reqKindsVal.([]interface{})
			}

			if !ok1 || !ok2 || !ok3 {
				http.Error(w, "Wrong data structures in request body - expecting an object with lists of groups, keys and kinds.",
					http.StatusBadRequest)
				return
			}

			// Remove groups from all selected nodes

			trans2 := graph.NewGraphTrans(api.GM)

			for i, srcs := range sres.RowSources() {
				src := strings.Split(srcs[col], ":")
				kind := src[1]
				key := src[2]

				if selections[i] {
					var nodes []data.Node

					nodes, _, err = api.GM.TraverseMulti(part, key, kind, ":::"+eql.GroupNodeKind, false)

					if err == nil {
						for _, n := range nodes {
							errorutil.AssertOk(removeNodeFromGroup(trans2, part, n.Key(), key, kind)) // There should be no errors at this point
						}
					}
				}

				if err != nil {
					break
				}
			}

			if err == nil {
				err = trans2.Commit()

				if err == nil {
					for i, g := range reqGroups {
						reqKindsArr := reqKinds[i].([]interface{})
						for j, k := range reqKeys[i].([]interface{}) {
							errorutil.AssertOk(addNodeToGroup(trans, part,
								fmt.Sprint(g), fmt.Sprint(k), fmt.Sprint(reqKindsArr[j]))) // There should be no errors at this point
						}
					}
				}
			}
		}
	}

	if err == nil {
		if err = trans.Commit(); err == nil {
			var sstate map[string]interface{}
			if sstate, err = qre.groupSelectionState(sres, part, col, selections); err == nil {
				qre.dataWriter(w).Encode(sstate)
			}
		}
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

/*
groupSelectionState returns the current group selection state of a given query result.
*/
func (qre *queryResultEndpoint) groupSelectionState(sres *APISearchResult, part string, primaryNodeCol int, selections []bool) (map[string]interface{}, error) {
	var ret map[string]interface{}
	var err error

	// Get groups for all selected nodes

	retGroups := []string{}
	retKeys := [][]string{}
	retKinds := [][]string{}

	memberKeys := make(map[string][]string)
	memberKinds := make(map[string][]string)

	for i, srcs := range sres.RowSources() {
		src := strings.Split(srcs[primaryNodeCol], ":")
		kind := src[1]
		key := src[2]

		if selections[i] {
			var nodes []data.Node

			nodes, _, err = api.GM.TraverseMulti(part, key, kind, ":::"+eql.GroupNodeKind, false)

			if err == nil {
				for _, n := range nodes {

					nkeys, ok := memberKeys[n.Key()]
					nkinds, _ := memberKinds[n.Key()]

					if !ok {
						nkeys = make([]string, 0)
						nkinds = make([]string, 0)
					}

					memberKeys[n.Key()] = append(nkeys, key)
					memberKinds[n.Key()] = append(nkinds, kind)
				}
			}
		}

		if err != nil {
			break
		}
	}

	memberKeysList := make([]string, 0, len(memberKeys))
	for g := range memberKeys {
		memberKeysList = append(memberKeysList, g)
	}
	sort.Strings(memberKeysList)

	for _, g := range memberKeysList {
		retGroups = append(retGroups, g)
		retKeys = append(retKeys, memberKeys[g])
		retKinds = append(retKinds, memberKinds[g])
	}

	if err == nil {
		ret = map[string]interface{}{
			"groups": retGroups,
			"keys":   retKeys,
			"kinds":  retKinds,
		}
	}

	return ret, err
}

/*
selectRows implements the row selection functionality.
*/
func (qre *queryResultEndpoint) selectRows(requestType string, w http.ResponseWriter,
	resources []string, sres *APISearchResult) {

	if requestType != "put" && requestType != "get" {
		http.Error(w, "Select can only handle GET and PUT requests", http.StatusBadRequest)
		return
	}

	if requestType == "get" {
		var col int
		var err error
		var keys, kinds []string

		// Just return the current selections

		if col, err = sres.GetPrimaryNodeColumn(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		sels := sres.Selections()

		for i, srcs := range sres.RowSources() {
			if sels[i] {
				src := strings.Split(srcs[col], ":")
				keys = append(keys, src[2])
				kinds = append(kinds, src[1])
			}
		}

		qre.dataWriter(w).Encode(map[string][]string{
			"keys":  keys,
			"kinds": kinds,
		})

		return

	} else if len(resources) < 3 {
		http.Error(w,
			"Need a selection ('all', 'none', 'invert' or row number)",
			http.StatusBadRequest)
		return
	}

	if resources[2] == "all" {
		sres.AllSelection()
	} else if resources[2] == "none" {
		sres.NoneSelection()
	} else if resources[2] == "invert" {
		sres.InvertSelection()
	} else {

		i, err := strconv.ParseInt(resources[2], 10, 64)
		row := int(i)

		selections := sres.Selections()

		if err != nil || row < 0 || row >= len(selections) {
			http.Error(w, "Invalid selection row number", http.StatusBadRequest)
			return
		}

		sres.SetSelection(row, !selections[row])
	}

	// Count total selections

	totalSels := 0
	for _, s := range sres.Selections() {
		if s {
			totalSels++
		}
	}

	qre.dataWriter(w).Encode(map[string]int{
		"total_selections": totalSels,
	})
}

/*
quickfilter implements the quickfilter functionality.
*/
func (qre *queryResultEndpoint) quickFilter(requestType string, w http.ResponseWriter,
	resources []string, sres *APISearchResult, limit int) {

	if requestType != "get" {
		http.Error(w, "Quickfilter can only handle GET requests", http.StatusBadRequest)
		return
	} else if len(resources) < 3 {
		http.Error(w, "Need a query result column to filter", http.StatusBadRequest)
		return
	}

	i, err := strconv.ParseInt(resources[2], 10, 64)
	index := int(i)

	if err != nil || index < 0 || index >= len(sres.Header().Labels()) {
		http.Error(w, "Invalid query result column", http.StatusBadRequest)
		return
	}

	// Go through the column in question and collect the data

	counts := make(map[string]uint64)

	for _, row := range sres.Rows() {
		val := fmt.Sprint(row[index])
		counts[val]++
	}

	values := make([]string, 0, len(counts))
	frequencies := make([]uint64, 0, len(counts))

	for val, freq := range counts {
		values = append(values, val)
		frequencies = append(frequencies, freq)
	}

	sort.Stable(&countComparator{values, frequencies})

	if limit != -1 && len(values) > limit {
		values = values[:limit]
		frequencies = frequencies[:limit]
	}

	qre.dataWriter(w).Encode(map[string]interface{}{
		"values":      values,
		"frequencies": frequencies,
	})
}

/*
dataWriter returns an object to write result data.
*/
func (qre *queryResultEndpoint) dataWriter(w http.ResponseWriter) *json.Encoder {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	return json.NewEncoder(w)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (qre *queryResultEndpoint) SwaggerDefs(s map[string]interface{}) {

	required := []map[string]interface{}{
		{
			"name":        "rid",
			"in":          "path",
			"description": "Result ID of a query result.",
			"required":    true,
			"type":        "string",
		},
	}

	column := map[string]interface{}{
		"name":        "column",
		"in":          "path",
		"description": "Column of the query result.",
		"required":    true,
		"type":        "string",
	}

	row := map[string]interface{}{
		"name":        "row",
		"in":          "path",
		"description": "Row number of the query result or 'all', 'none' or 'invert'.",
		"required":    true,
		"type":        "string",
	}

	groupName := map[string]interface{}{
		"name":        "group_name",
		"in":          "path",
		"description": "Name of an existing group.",
		"required":    true,
		"type":        "string",
	}

	limit := map[string]interface{}{
		"name":        "limit",
		"in":          "query",
		"description": "Limit the maximum number of result items.",
		"required":    false,
		"type":        "string",
	}

	selectionStateParam := map[string]interface{}{
		"name":        "selection_state",
		"in":          "body",
		"description": "Group seletion state of a query result",
		"required":    true,
		"schema": map[string]interface{}{
			"$ref": "#/definitions/GroupSelectionState",
		},
	}

	selectionStateGroups := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"groups": map[string]interface{}{
				"description": "List of group names which include one or more selected nodes.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": "Group name.",
					"type":        "string",
				},
			},
			"keys": map[string]interface{}{
				"description": "Lists of selected node keys which are part of the groups in the 'groups' list.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": "List of node keys.",
					"type":        "array",
					"items": map[string]interface{}{
						"description": "Node key.",
						"type":        "string",
					},
				},
			},
			"kinds": map[string]interface{}{
				"description": "Lists of selected node kinds which are part of the groups in the 'groups' list.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": "List of node kinds.",
					"type":        "array",
					"items": map[string]interface{}{
						"description": "Node kind.",
						"type":        "string",
					},
				},
			},
		},
	}

	selectionState := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"keys": map[string]interface{}{
				"description": "Lists of selected node keys.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": "Node key.",
					"type":        "string",
				},
			},
			"kinds": map[string]interface{}{
				"description": "Kinds of all selected nodes.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": "Node kind.",
					"type":        "string",
				},
			},
		},
	}

	s["paths"].(map[string]interface{})["/v1/queryresult/{rid}/csv"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return the search result in CSV format.",
			"description": "The csv endpoint is used to generate a CSV string from the search result.",
			"produces": []string{
				"text/plain",
			},
			"parameters": append(required),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "A CSV string.",
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

	s["paths"].(map[string]interface{})["/v1/queryresult/{rid}/quickfilter/{column}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return quickfilter information on a given result column.",
			"description": "The quickfilter endpoint is used to determine the 10 most frequent used values in a particular result column.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(required, column, limit),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "An object containing values and frequencies.",
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

	s["paths"].(map[string]interface{})["/v1/queryresult/{rid}/select"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return the (primary) nodes which are currently selected.",
			"description": "The select endpoint is used to query all selected nodes of a given query result.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": required,
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Current total selections.",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/SelectionState",
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

	s["paths"].(map[string]interface{})["/v1/queryresult/{rid}/select/{row}"] = map[string]interface{}{
		"put": map[string]interface{}{
			"summary":     "Selects one or more rows of a given query result.",
			"description": "The select endpoint is used to select one or more rows of a given query result.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(required, row),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Current total selections.",
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

	s["paths"].(map[string]interface{})["/v1/queryresult/{rid}/groupselected/{group_name}"] = map[string]interface{}{
		"put": map[string]interface{}{
			"summary":     "Add all selected nodes (primary node of each row) to the given group.",
			"description": "The groupselected endpoint is used to add all selected nodes (primary node of each row) to the given (existing) group.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(required, groupName),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Current group selection state after the operation.",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/GroupSelectionState",
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
		"delete": map[string]interface{}{
			"summary":     "Remove all selected nodes (primary node of each row) from the given group.",
			"description": "The groupselected endpoint is used to remove all selected nodes (primary node of each row) from the given (existing) group.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(required, groupName),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Current group selection state after the operation.",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/GroupSelectionState",
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

	s["paths"].(map[string]interface{})["/v1/queryresult/{rid}/groupselected"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Get the current group selection state.",
			"description": "Returns the current selections state which contains all selected nodes which are in groups.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(required),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Current group selection state.",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/GroupSelectionState",
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
		"post": map[string]interface{}{
			"summary":     "Set a new group selection state.",
			"description": "Sets the groups in the given selection state.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(required, selectionStateParam),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Current group selection state after the operation.",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/GroupSelectionState",
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

	// Add selection states to definition

	s["definitions"].(map[string]interface{})["SelectionState"] = selectionState
	s["definitions"].(map[string]interface{})["GroupSelectionState"] = selectionStateGroups
}

/*
countComparator is a comparator object used for sorting the counts
*/
type countComparator struct {
	Values      []string
	Frequencies []uint64
}

func (c countComparator) Len() int {
	return len(c.Values)
}

func (c countComparator) Less(i, j int) bool {
	if c.Frequencies[i] == c.Frequencies[j] {
		return c.Values[i] < c.Values[j]
	}
	return c.Frequencies[i] > c.Frequencies[j]
}

func (c countComparator) Swap(i, j int) {
	c.Values[i], c.Values[j] = c.Values[j], c.Values[i]
	c.Frequencies[i], c.Frequencies[j] = c.Frequencies[j], c.Frequencies[i]
}
