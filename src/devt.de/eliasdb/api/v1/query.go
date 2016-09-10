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
	"time"

	"devt.de/common/datautil"
	"devt.de/common/stringutil"
	"devt.de/eliasdb/api"
	"devt.de/eliasdb/eql"
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

	// Check parameters

	if !checkResources(w, resources, 1, 1, "Need a partition") {
		return
	}

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

	// See if a result id was given

	resID := r.URL.Query().Get("rid")
	if resID != "" {

		res, ok := ResultCache.Get(resID)
		if !ok {
			http.Error(w, "Unknown result id (rid parameter)", http.StatusBadRequest)
			return
		}

		eq.writeResultData(w, res.(eql.SearchResult), resID, offset, limit)
		return
	}

	// Run the query

	query := r.URL.Query().Get("q")
	part := resources[0]

	if query == "" {
		http.Error(w, "Missing query (q parameter)", http.StatusBadRequest)
		return
	}

	res, err := eql.RunQuery(stringutil.CreateDisplayString(part)+" query",
		part, query, api.GM)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Store the result in the cache

	resID = genID()

	ResultCache.Put(resID, res)

	eq.writeResultData(w, res, resID, offset, limit)
}

/*
writeResultData writes result data for the client.
*/
func (eq *queryEndpoint) writeResultData(w http.ResponseWriter, res eql.SearchResult,
	resID string, offset int, limit int) {

	// Write out the data

	header := res.Header()

	ret := json.NewEncoder(w)

	data := make(map[string]interface{})

	if limit == -1 && offset == -1 {
		data["rows"] = res.Rows()
		data["sources"] = res.RowSources()

	} else {

		rows := res.Rows()
		srcs := res.RowSources()

		if offset > 0 {

			if offset >= len(rows) {
				http.Error(w, "Offset exceeds available rows", http.StatusInternalServerError)
				return
			}

			rows = rows[offset:]
			srcs = srcs[offset:]
		}

		if limit != -1 && limit < len(rows) {
			rows = rows[:limit]
			srcs = srcs[:limit]
		}

		data["rows"] = rows
		data["sources"] = srcs
	}

	// Write out result header

	dataHeader := make(map[string]interface{})

	data["header"] = dataHeader

	dataHeader["labels"] = header.Labels()
	dataHeader["format"] = header.Format()
	dataHeader["data"] = header.Data()
	dataHeader["primary_kind"] = header.PrimaryKind()

	// Set response header values

	w.Header().Add(HTTPHeaderTotalCount, fmt.Sprint(res.RowCount()))
	w.Header().Add(HTTPHeaderCacheID, resID)

	w.Header().Set("content-type", "application/json; charset=utf-8")

	ret.Encode(data)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (eq *queryEndpoint) SwaggerDefs(s map[string]interface{}) {

	// Add query paths

	s["paths"].(map[string]interface{})["/v1/query/{partition}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Run EQL queries to query the EliasDB datastore.",
			"description": "The query endpoint should be used to run EQL search queries against partitions. The return value is always a list (even if there is only a single entry). A query result gets an ID and is stored in a cache. The id is returned in the X-Cache-Id header. Subsequent requests for the same result can use the id instead of a query.",
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
					"name":        "q",
					"in":          "query",
					"description": "URL encoded query to execute.",
					"required":    false,
					"type":        "string",
				},
				map[string]interface{}{
					"name":        "rid",
					"in":          "query",
					"description": "Result id to retrieve from the result cache.",
					"required":    false,
					"type":        "number",
					"format":      "integer",
				},
				map[string]interface{}{
					"name":        "limit",
					"in":          "query",
					"description": "How many list items to return.",
					"required":    false,
					"type":        "number",
					"format":      "integer",
				},
				map[string]interface{}{
					"name":        "offset",
					"in":          "query",
					"description": "Offset in the dataset.",
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
						"description": "A single cell of the query result.",
						"type": []string{
							"integer",
							"string",
						},
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
						"type": []string{
							"integer",
							"string",
						},
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
genID generates a unique ID.
*/
func genID() string {
	idCount++
	return fmt.Sprint(idCount)
}
