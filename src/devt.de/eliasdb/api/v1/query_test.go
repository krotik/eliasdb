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
	"testing"
)

func TestQueryPagination(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointQuery

	st, _, res := sendTestRequest(queryURL+"//main?q=get+Song+with+ordering(ascending+key)&offset=p&limit=2", "GET", nil)
	if st != "400 Bad Request" || res != "Invalid parameter value: offset should be a positive integer number" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main?q=get+Song+with+ordering(ascending+key)&offset=2&limit=p", "GET", nil)
	if st != "400 Bad Request" || res != "Invalid parameter value: limit should be a positive integer number" {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, h, res := sendTestRequest(queryURL+"//main?q=get+Song+with+ordering(ascending+key)&offset=2&limit=3", "GET", nil)

	if st != "200 OK" || res != `
{
  "header": {
    "data": [
      "1:n:key",
      "1:n:name",
      "1:n:ranking"
    ],
    "format": [
      "auto",
      "auto",
      "auto"
    ],
    "labels": [
      "Song Key",
      "Song Name",
      "Ranking"
    ],
    "primary_kind": "Song"
  },
  "rows": [
    [
      "Aria3",
      "Aria3",
      4
    ],
    [
      "Aria4",
      "Aria4",
      18
    ],
    [
      "DeadSong2",
      "DeadSong2",
      6
    ]
  ],
  "selections": [
    false,
    false,
    false
  ],
  "sources": [
    [
      "n:Song:Aria3",
      "n:Song:Aria3",
      "n:Song:Aria3"
    ],
    [
      "n:Song:Aria4",
      "n:Song:Aria4",
      "n:Song:Aria4"
    ],
    [
      "n:Song:DeadSong2",
      "n:Song:DeadSong2",
      "n:Song:DeadSong2"
    ]
  ],
  "total_selections": 0
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	// Check header values

	if tc := h.Get(HTTPHeaderTotalCount); tc != "9" {
		t.Error("Unexpected total count:", tc)
		return
	}

	rid := h.Get(HTTPHeaderCacheID)

	if _, ok := ResultCache.Get(rid); !ok {
		t.Error("Given result id should be in the cache")
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main?rid="+rid+"&offset=5&limit=0", "GET", nil)
	if st != "200 OK" || res != `
{
  "header": {
    "data": [
      "1:n:key",
      "1:n:name",
      "1:n:ranking"
    ],
    "format": [
      "auto",
      "auto",
      "auto"
    ],
    "labels": [
      "Song Key",
      "Song Name",
      "Ranking"
    ],
    "primary_kind": "Song"
  },
  "rows": [],
  "selections": [],
  "sources": [],
  "total_selections": 0
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main?rid="+rid+"&offset=5&limit=1", "GET", nil)
	if st != "200 OK" || res != `
{
  "header": {
    "data": [
      "1:n:key",
      "1:n:name",
      "1:n:ranking"
    ],
    "format": [
      "auto",
      "auto",
      "auto"
    ],
    "labels": [
      "Song Key",
      "Song Name",
      "Ranking"
    ],
    "primary_kind": "Song"
  },
  "rows": [
    [
      "FightSong4",
      "FightSong4",
      3
    ]
  ],
  "selections": [
    false
  ],
  "sources": [
    [
      "n:Song:FightSong4",
      "n:Song:FightSong4",
      "n:Song:FightSong4"
    ]
  ],
  "total_selections": 0
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main?rid="+rid, "GET", nil)
	if st != "200 OK" || res != `
{
  "header": {
    "data": [
      "1:n:key",
      "1:n:name",
      "1:n:ranking"
    ],
    "format": [
      "auto",
      "auto",
      "auto"
    ],
    "labels": [
      "Song Key",
      "Song Name",
      "Ranking"
    ],
    "primary_kind": "Song"
  },
  "rows": [
    [
      "Aria1",
      "Aria1",
      8
    ],
    [
      "Aria2",
      "Aria2",
      2
    ],
    [
      "Aria3",
      "Aria3",
      4
    ],
    [
      "Aria4",
      "Aria4",
      18
    ],
    [
      "DeadSong2",
      "DeadSong2",
      6
    ],
    [
      "FightSong4",
      "FightSong4",
      3
    ],
    [
      "LoveSong3",
      "LoveSong3",
      1
    ],
    [
      "MyOnlySong3",
      "MyOnlySong3",
      19
    ],
    [
      "StrangeSong1",
      "StrangeSong1",
      5
    ]
  ],
  "selections": [
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false
  ],
  "sources": [
    [
      "n:Song:Aria1",
      "n:Song:Aria1",
      "n:Song:Aria1"
    ],
    [
      "n:Song:Aria2",
      "n:Song:Aria2",
      "n:Song:Aria2"
    ],
    [
      "n:Song:Aria3",
      "n:Song:Aria3",
      "n:Song:Aria3"
    ],
    [
      "n:Song:Aria4",
      "n:Song:Aria4",
      "n:Song:Aria4"
    ],
    [
      "n:Song:DeadSong2",
      "n:Song:DeadSong2",
      "n:Song:DeadSong2"
    ],
    [
      "n:Song:FightSong4",
      "n:Song:FightSong4",
      "n:Song:FightSong4"
    ],
    [
      "n:Song:LoveSong3",
      "n:Song:LoveSong3",
      "n:Song:LoveSong3"
    ],
    [
      "n:Song:MyOnlySong3",
      "n:Song:MyOnlySong3",
      "n:Song:MyOnlySong3"
    ],
    [
      "n:Song:StrangeSong1",
      "n:Song:StrangeSong1",
      "n:Song:StrangeSong1"
    ]
  ],
  "total_selections": 0
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main?rid="+rid+"&offset=500&limit=1", "GET", nil)

	if res != "Offset exceeds available rows" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"//main?rid=abc&offset=5&limit=1", "GET", nil)

	if res != "Unknown result ID (rid parameter)" {
		t.Error("Unexpected response:", res)
		return
	}
}

func TestQuery(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointQuery

	// POST requests should not be allowed

	_, _, res := sendTestRequest(queryURL, "POST",
		[]byte(`{"msg":"Hello!"}`))

	if res != "Method Not Allowed" {
		t.Error("Unexpected response:", res)
		return
	}

	// Test error message

	_, _, res = sendTestRequest(queryURL+"main", "GET", nil)

	if res != "Missing query (q parameter)" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main/bla/bla?q=get+Song", "GET", nil)

	if res != "Invalid resource specification: bla/bla" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/?q=get+Song", "GET", nil)

	if res != "Need a partition" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"main/?q=get+BLA", "GET", nil)

	if res != "EQL error in Main query: Unknown node kind (BLA) (Line:1 Pos:5)" {
		t.Error("Unexpected response:", res)
		return
	}

	// Test first real query

	st, _, res := sendTestRequest(queryURL+"//main?q=get+Song+with+ordering(ascending+key)", "GET", nil)
	st, _, res2 := sendTestRequest(queryURL+"//main?q=get+Song+with+ordering(ascending+key)&offset=0&limit=9", "GET", nil)

	if st != "200 OK" || res2 != res || res != `
{
  "header": {
    "data": [
      "1:n:key",
      "1:n:name",
      "1:n:ranking"
    ],
    "format": [
      "auto",
      "auto",
      "auto"
    ],
    "labels": [
      "Song Key",
      "Song Name",
      "Ranking"
    ],
    "primary_kind": "Song"
  },
  "rows": [
    [
      "Aria1",
      "Aria1",
      8
    ],
    [
      "Aria2",
      "Aria2",
      2
    ],
    [
      "Aria3",
      "Aria3",
      4
    ],
    [
      "Aria4",
      "Aria4",
      18
    ],
    [
      "DeadSong2",
      "DeadSong2",
      6
    ],
    [
      "FightSong4",
      "FightSong4",
      3
    ],
    [
      "LoveSong3",
      "LoveSong3",
      1
    ],
    [
      "MyOnlySong3",
      "MyOnlySong3",
      19
    ],
    [
      "StrangeSong1",
      "StrangeSong1",
      5
    ]
  ],
  "selections": [
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false
  ],
  "sources": [
    [
      "n:Song:Aria1",
      "n:Song:Aria1",
      "n:Song:Aria1"
    ],
    [
      "n:Song:Aria2",
      "n:Song:Aria2",
      "n:Song:Aria2"
    ],
    [
      "n:Song:Aria3",
      "n:Song:Aria3",
      "n:Song:Aria3"
    ],
    [
      "n:Song:Aria4",
      "n:Song:Aria4",
      "n:Song:Aria4"
    ],
    [
      "n:Song:DeadSong2",
      "n:Song:DeadSong2",
      "n:Song:DeadSong2"
    ],
    [
      "n:Song:FightSong4",
      "n:Song:FightSong4",
      "n:Song:FightSong4"
    ],
    [
      "n:Song:LoveSong3",
      "n:Song:LoveSong3",
      "n:Song:LoveSong3"
    ],
    [
      "n:Song:MyOnlySong3",
      "n:Song:MyOnlySong3",
      "n:Song:MyOnlySong3"
    ],
    [
      "n:Song:StrangeSong1",
      "n:Song:StrangeSong1",
      "n:Song:StrangeSong1"
    ]
  ],
  "total_selections": 0
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}
}

func TestGroupingInfo(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointQuery

	st, _, res := sendTestRequest(queryURL+"//main?q=get+Song+with+ordering(ascending+key)&offset=2&limit=3&groups=1", "GET", nil)

	if st != "200 OK" || res != `
{
  "groups": [
    [
      "Best"
    ],
    [],
    []
  ],
  "header": {
    "data": [
      "1:n:key",
      "1:n:name",
      "1:n:ranking"
    ],
    "format": [
      "auto",
      "auto",
      "auto"
    ],
    "labels": [
      "Song Key",
      "Song Name",
      "Ranking"
    ],
    "primary_kind": "Song"
  },
  "rows": [
    [
      "Aria3",
      "Aria3",
      4
    ],
    [
      "Aria4",
      "Aria4",
      18
    ],
    [
      "DeadSong2",
      "DeadSong2",
      6
    ]
  ],
  "selections": [
    false,
    false,
    false
  ],
  "sources": [
    [
      "n:Song:Aria3",
      "n:Song:Aria3",
      "n:Song:Aria3"
    ],
    [
      "n:Song:Aria4",
      "n:Song:Aria4",
      "n:Song:Aria4"
    ],
    [
      "n:Song:DeadSong2",
      "n:Song:DeadSong2",
      "n:Song:DeadSong2"
    ]
  ],
  "total_selections": 0
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"//main?q=get+Song+primary+'group'+with+ordering(ascending+key)&offset=2&limit=3&groups=1", "GET", nil)

	if st != "400 Bad Request" || res != "Could not determine key of primary node - query needs a primary expression" {
		t.Error("Unexpected response:", st, res)
		return
	}
}
