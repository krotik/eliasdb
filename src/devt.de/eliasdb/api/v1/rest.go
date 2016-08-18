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
Definition for EliasDB REST API Version 1
*/
package v1

import (
	"net/http"
	"strconv"
	"strings"

	"devt.de/eliasdb/api"
)

const API_VERSION_V1 = "/v1"

const HTTP_HEADER_TOTAL_COUNT = "X-Total-Count"
const HTTP_HEADER_CACHE_ID = "X-Cache-Id"

/*
Map of urls to endpoints
*/
var V1EndpointMap = map[string]api.RestEndpointInst{
	ENDPOINT_INDEX_QUERY: IndexEndpointInst,
	ENDPOINT_QUERY:       QueryEndpointInst,
	ENDPOINT_GRAPH:       GraphEndpointInst,
	ENDPOINT_INFO_QUERY:  InfoEndpointInst,
}

// Helper functions
// ================

/*
checkResources check given resources for a GET request.
*/
func checkResources(w http.ResponseWriter, resources []string, requiredMin int, requiredMax int, errorMsg string) bool {
	if len(resources) < requiredMin {
		http.Error(w, errorMsg, http.StatusBadRequest)
		return false
	} else if len(resources) > requiredMax {
		http.Error(w, "Invalid resource specification: "+strings.Join(resources[1:], "/"), http.StatusBadRequest)
		return false
	}
	return true
}

/*
Extract a positive number from a query parameter. Returns -1 and true
if the parameter was not given.
*/
func queryParamPosNum(w http.ResponseWriter, r *http.Request, param string) (int, bool) {

	val := r.URL.Query().Get(param)

	if val == "" {
		return -1, true
	}

	num, err := strconv.Atoi(val)

	if err != nil || num < 0 {
		http.Error(w, "Invalid parameter value: "+param+" should be a positive integer number", http.StatusBadRequest)
		return -1, false
	}

	return num, true
}
