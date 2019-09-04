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
	"time"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/eql"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
ResultCacheMaxSize is the maximum size for the result cache
*/
var ResultCacheMaxSize uint64

/*
ResultCacheMaxAge is the maximum age a result cache entry can have in seconds
*/
var ResultCacheMaxAge int64

/*
ResultCache is a cache for result sets (by default no expiry and no limit)
*/
var ResultCache *datautil.MapCache

/*
idCount is an ID counter for results
*/
var idCount = time.Now().Unix()

/*
EndpointQuery is the query endpoint URL (rooted). Handles everything under query/...
*/
const EndpointQuery = api.APIRoot + APIv1 + "/query/"

/*
QueryEndpointInst creates a new endpoint handler.
*/
func QueryEndpointInst() api.RestEndpointHandler {

	// Init the result cache if necessary

	if ResultCache == nil {
		ResultCache = datautil.NewMapCache(ResultCacheMaxSize, ResultCacheMaxAge)
	}

	return &queryEndpoint{}
}

/*
Handler object for search queries.
*/
type queryEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles a search query REST call.
*/
func (eq *queryEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	var err error

	// Check parameters

	if !checkResources(w, resources, 1, 1, "Need a partition") {
		return
	}

	// Get partition

	part := resources[0]

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

	// Get groups parameter

	gs := r.URL.Query().Get("groups")
	showGroups := gs != ""

	// See if a result ID was given

	resID := r.URL.Query().Get("rid")
	if resID != "" {

		res, ok := ResultCache.Get(resID)
		if !ok {
			http.Error(w, "Unknown result ID (rid parameter)", http.StatusBadRequest)
			return
		}

		err = eq.writeResultData(w, res.(*APISearchResult), part, resID, offset, limit, showGroups)

	} else {
		var res eql.SearchResult

		// Run the query

		query := r.URL.Query().Get("q")

		if query == "" {
			http.Error(w, "Missing query (q parameter)", http.StatusBadRequest)
			return
		}

		res, err = eql.RunQuery(stringutil.CreateDisplayString(part)+" query",
			part, query, api.GM)

		if err == nil {
			sres := &APISearchResult{res, nil}

			// Make sure the result has a primary node column

			_, err = sres.GetPrimaryNodeColumn()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			// Store the result in the cache

			resID = genID()

			ResultCache.Put(resID, sres)

			err = eq.writeResultData(w, sres, part, resID, offset, limit, showGroups)
		}
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

/*
writeResultData writes result data for the client.
*/
func (eq *queryEndpoint) writeResultData(w http.ResponseWriter, res *APISearchResult,
	part string, resID string, offset int, limit int, showGroups bool) error {
	var err error

	// Write out the data

	header := res.Header()

	ret := json.NewEncoder(w)

	resdata := make(map[string]interface{})

	// Count total selections

	sels := res.Selections()
	totalSels := 0
	for _, s := range sels {
		if s {
			totalSels++
		}
	}

	resdata["total_selections"] = totalSels

	rows := res.Rows()
	srcs := res.RowSources()

	if limit == -1 && offset == -1 {
		resdata["rows"] = rows
		resdata["sources"] = srcs
		resdata["selections"] = sels

	} else {

		if offset > 0 {

			if offset >= len(rows) {
				return fmt.Errorf("Offset exceeds available rows")
			}

			rows = rows[offset:]
			srcs = srcs[offset:]
			sels = sels[offset:]
		}

		if limit != -1 && limit < len(rows) {
			rows = rows[:limit]
			srcs = srcs[:limit]
			sels = sels[:limit]
		}

		resdata["rows"] = rows
		resdata["sources"] = srcs
		resdata["selections"] = sels
	}

	// Write out result header

	resdataHeader := make(map[string]interface{})

	resdata["header"] = resdataHeader

	resdataHeader["labels"] = header.Labels()
	resdataHeader["format"] = header.Format()
	resdataHeader["data"] = header.Data()

	pk := header.PrimaryKind()

	resdataHeader["primary_kind"] = pk

	if showGroups {
		groupList := make([][]string, 0, len(srcs))

		if len(srcs) > 0 {
			var col int

			// Get column for primary kind

			col, err = res.GetPrimaryNodeColumn()

			// Lookup groups for nodes

			for _, s := range resdata["sources"].([][]string) {

				if err == nil {
					var nodes []data.Node

					groups := make([]string, 0, 3)
					key := strings.Split(s[col], ":")[2]

					nodes, _, err = api.GM.TraverseMulti(part, key, pk,
						":::"+eql.GroupNodeKind, false)

					if err == nil {
						for _, n := range nodes {
							groups = append(groups, n.Key())
						}
					}

					groupList = append(groupList, groups)
				}
			}
		}

		resdata["groups"] = groupList
	}

	if err == nil {

		// Set response header values

		w.Header().Add(HTTPHeaderTotalCount, fmt.Sprint(res.RowCount()))
		w.Header().Add(HTTPHeaderCacheID, resID)

		w.Header().Set("content-type", "application/json; charset=utf-8")

		ret.Encode(resdata)
	}

	return err
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (eq *queryEndpoint) SwaggerDefs(s map[string]interface{}) {

	// Add query paths

	s["paths"].(map[string]interface{})["/v1/query/{partition}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary": "Run EQL queries to query the EliasDB datastore.",
			"description": "The query endpoint should be used to run EQL search " +
				"queries against partitions. The return value is always a list " +
				"(even if there is only a single entry). A query result gets an " +
				"ID and is stored in a cache. The ID is returned in the X-Cache-Id " +
				"header. Subsequent requests for the same result can use the ID instead of a query.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": []map[string]interface{}{
				{
					"name":        "partition",
					"in":          "path",
					"description": "Partition to query.",
					"required":    true,
					"type":        "string",
				},
				{
					"name":        "q",
					"in":          "query",
					"description": "URL encoded query to execute.",
					"required":    false,
					"type":        "string",
				},
				{
					"name":        "rid",
					"in":          "query",
					"description": "Result ID to retrieve from the result cache.",
					"required":    false,
					"type":        "number",
					"format":      "integer",
				},
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
				{
					"name":        "groups",
					"in":          "query",
					"description": "Include group information in the result if set to any value.",
					"required":    false,
					"type":        "number",
					"format":      "integer",
				},
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "A query result",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/QueryResult",
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

	// Add QueryResult to definitions

	s["definitions"].(map[string]interface{})["QueryResult"] = map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"header": map[string]interface{}{
				"description": "Header for the query result.",
				"type":        "object",
				"properties": map[string]interface{}{
					"labels": map[string]interface{}{
						"description": "All column labels of the search result.",
						"type":        "array",
						"items": map[string]interface{}{
							"description": "Column label.",
							"type":        "string",
						},
					},
					"format": map[string]interface{}{
						"description": "All column format definitions of the search result.",
						"type":        "array",
						"items": map[string]interface{}{
							"description": "Column format as specified in the show format (e.g. text).",
							"type":        "string",
						},
					},
					"data": map[string]interface{}{
						"description": "The data which is displayed in each column of the search result.",
						"type":        "array",
						"items": map[string]interface{}{
							"description": "Data source for the column (e.g. 1:n:name - Name of starting nodes, 3:e:key - Key of edge traversed in the second traversal).",
							"type":        "string",
						},
					},
				},
			},
			"rows": map[string]interface{}{
				"description": "Rows of the query result.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": "Columns of a row of the query result.",
					"type":        "array",
					"items": map[string]interface{}{
						"description": "A single cell of the query result (string, integer or null).",
						"type":        "object",
					},
				},
			},
			"sources": map[string]interface{}{
				"description": "Data sources of the query result.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": "Columns of a row of the query result.",
					"type":        "array",
					"items": map[string]interface{}{
						"description": "Data source of a single cell of the query result.",
						"type":        "string",
					},
				},
			},
			"groups": map[string]interface{}{
				"description": "Group names for each row.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": " Groups of the primary kind node.",
					"type":        "array",
					"items": map[string]interface{}{
						"description": "Group name.",
						"type":        "string",
					},
				},
			},
			"selections": map[string]interface{}{
				"description": "List of row selections.",
				"type":        "array",
				"items": map[string]interface{}{
					"description": "Row selection.",
					"type":        "boolean",
				},
			},
			"total_selections": map[string]interface{}{
				"description": "Number of total selections.",
				"type":        "number",
				"format":      "integer",
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
genID generates a unique ID.
*/
func genID() string {
	idCount++
	return fmt.Sprint(idCount)
}

/*
APISearchResult is a search result maintained by the API. It embeds
*/
type APISearchResult struct {
	eql.SearchResult        // Normal eql search result
	selections       []bool // Selections of the result
}

/*
GetPrimaryNodeColumn determines the first primary node column.
*/
func (r *APISearchResult) GetPrimaryNodeColumn() (int, error) {
	var err error

	pk := r.Header().PrimaryKind()
	col := -1
	rs := r.RowSources()

	if len(rs) > 0 {
		for i, scol := range rs[0] {
			scolParts := strings.Split(scol, ":")
			if len(scolParts) > 1 && pk == scolParts[1] {
				col = i
			}
		}
	}

	if col == -1 {
		err = fmt.Errorf("Could not determine key of primary node - query needs a primary expression")
	}

	return col, err
}

/*
Selections returns all current selections.
*/
func (r *APISearchResult) Selections() []bool {
	r.refreshSelection()
	return r.selections
}

/*
SetSelection sets a new selection.
*/
func (r *APISearchResult) SetSelection(line int, selection bool) {
	r.refreshSelection()
	if line < len(r.selections) {
		r.selections[line] = selection
	}
}

/*
AllSelection selects all rows.
*/
func (r *APISearchResult) AllSelection() {
	r.refreshSelection()
	for i := 0; i < len(r.selections); i++ {
		r.selections[i] = true
	}
}

/*
NoneSelection selects none rows.
*/
func (r *APISearchResult) NoneSelection() {
	r.refreshSelection()
	for i := 0; i < len(r.selections); i++ {
		r.selections[i] = false
	}
}

/*
InvertSelection inverts the current selection.
*/
func (r *APISearchResult) InvertSelection() {
	r.refreshSelection()
	for i := 0; i < len(r.selections); i++ {
		r.selections[i] = !r.selections[i]
	}
}

/*
refreshSelection reallocates the selection array if necessary.
*/
func (r *APISearchResult) refreshSelection() {
	l := r.SearchResult.RowCount()

	if len(r.selections) != l {

		origSelections := r.selections

		// There is a difference between the selections array and the row
		// count we need to resize

		r.selections = make([]bool, l)

		for i, s := range origSelections {
			if i < l {
				r.selections[i] = s
			}
		}
	}
}
