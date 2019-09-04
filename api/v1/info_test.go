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

import "testing"

func TestInfoQuery(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointInfoQuery

	// No special testing here - the correctness of returned values is tested
	// elsewhere

	st, _, res := sendTestRequest(queryURL, "GET", nil)
	if st != "200 OK" {
		t.Error("Unexpected response:", st, res)
		return
	}

	queryURL = "http://localhost" + TESTPORT + EndpointInfoQuery + "kind"

	_, _, res = sendTestRequest(queryURL, "GET", nil)
	if res != "Missing node kind" {
		t.Error("Unexpected response:", res)
		return
	}

	queryURL = "http://localhost" + TESTPORT + EndpointInfoQuery + "kind/foobar"

	_, _, res = sendTestRequest(queryURL, "GET", nil)
	if res != "Unknown node kind foobar" {
		t.Error("Unexpected response:", res)
		return
	}

	queryURL = "http://localhost" + TESTPORT + EndpointInfoQuery + "kind/Song"

	_, _, res = sendTestRequest(queryURL, "GET", nil)

	if res != `
{
  "edge_attrs": null,
  "node_attrs": [
    "key",
    "kind",
    "name",
    "ranking"
  ],
  "node_edges": [
    "Song:Contains:group:group",
    "Song:Wrote:Author:Author"
  ]
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	queryURL = "http://localhost" + TESTPORT + EndpointInfoQuery + "kind/Wrote"

	_, _, res = sendTestRequest(queryURL, "GET", nil)

	if res != `
{
  "edge_attrs": [
    "end1cascading",
    "end1key",
    "end1kind",
    "end1role",
    "end2cascading",
    "end2key",
    "end2kind",
    "end2role",
    "key",
    "kind",
    "number"
  ],
  "node_attrs": null,
  "node_edges": null
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}
}
