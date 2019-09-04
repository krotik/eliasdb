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
	"strings"
	"testing"

	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/storage"
)

func TestIndexQuery(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointIndexQuery

	st, _, res := sendTestRequest(queryURL+"//main/x/", "GET", nil)
	if st != "400 Bad Request" || res != "Need a partition, entity type (n or e) and a kind" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main/x/bla", "GET", nil)
	if st != "400 Bad Request" || res != "Entity type must be n (nodes) or e (edges)" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main/n/bla?attr=1", "GET", nil)
	if st != "400 Bad Request" || res != "Unknown partition or node kind" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main/n/Song", "GET", nil)
	if st != "400 Bad Request" || res != "Query string for attr (attribute) is required" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=1", "GET", nil)
	if st != "400 Bad Request" || res != "Query string for either phrase, word or value is required" {
		t.Error("Unexpected response:", st, res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=1&word=1", "GET", nil)
	if res != "{}" {
		t.Error("Unexpected response:", st, res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=1&phrase=1", "GET", nil)
	if res != "[]" {
		t.Error("Unexpected response:", st, res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=1&value=1", "GET", nil)
	if res != "[]" {
		t.Error("Unexpected response:", st, res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=name&value=Aria1", "GET", nil)
	if res != `
[
  "Aria1"
]`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=name&phrase=Aria1", "GET", nil)
	if res != `
[
  "Aria1"
]`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=name&word=Aria1", "GET", nil)
	if res != `
{
  "Aria1": [
    1
  ]
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main/e/Wrote?attr=number&word=1", "GET", nil)
	if res != `
{
  "Aria1": [
    1
  ],
  "StrangeSong1": [
    1
  ]
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	msm := gmMSM.StorageManager("main"+"Song"+graph.StorageSuffixNodesIndex,
		true).(*storage.MemoryStorageManager)

	for i := 2; i < 30; i++ {
		msm.AccessMap[uint64(i)] = storage.AccessCacheAndFetchError
	}

	st, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=name&value=Aria1", "GET", nil)

	if st != "500 Internal Server Error" ||
		strings.HasPrefix(res, "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainSong.nodeidx - Location") {
		t.Error("Unexpected response:", res)
		return
	}

	for i := 2; i < 30; i++ {
		delete(msm.AccessMap, uint64(i))
	}

	msm.AccessMap[1] = storage.AccessCacheAndFetchError

	st, _, res = sendTestRequest(queryURL+"//main/n/Song?attr=name&value=Aria1", "GET", nil)

	if st != "500 Internal Server Error" ||
		res != "GraphError: Failed to access graph storage component (Slot not found (mystorage/mainSong.nodeidx - Location:1))" {
		t.Error("Unexpected response:", res)
		return
	}

	delete(msm.AccessMap, 1)

}
