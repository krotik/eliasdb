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
	"net/http"
	"strconv"
	"strings"

	"devt.de/krotik/eliasdb/api"
)

/*
APIv1 is the directory for version 1 of the API
*/
const APIv1 = "/v1"

/*
HTTPHeaderTotalCount is a special header value containing the total count of objects.
*/
const HTTPHeaderTotalCount = "X-Total-Count"

/*
HTTPHeaderCacheID is a special header value containing a cache ID for a quick follow up query.
*/
const HTTPHeaderCacheID = "X-Cache-Id"

/*
V1EndpointMap is a map of urls to endpoints for version 1 of the API
*/
var V1EndpointMap = map[string]api.RestEndpointInst{
	EndpointBlob:                 BlobEndpointInst,
	EndpointClusterQuery:         ClusterEndpointInst,
	EndpointEql:                  EqlEndpointInst,
	EndpointGraph:                GraphEndpointInst,
	EndpointGraphQL:              GraphQLEndpointInst,
	EndpointGraphQLQuery:         GraphQLQueryEndpointInst,
	EndpointGraphQLSubscriptions: GraphQLSubscriptionsEndpointInst,
	EndpointIndexQuery:           IndexEndpointInst,
	EndpointFindQuery:            FindEndpointInst,
	EndpointInfoQuery:            InfoEndpointInst,
	EndpointQuery:                QueryEndpointInst,
	EndpointQueryResult:          QueryResultEndpointInst,
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
