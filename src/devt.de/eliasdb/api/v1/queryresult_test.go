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
	"fmt"
	"testing"

	"devt.de/eliasdb/api"
	"devt.de/eliasdb/eql/interpreter"
	"devt.de/eliasdb/graph/data"
)

func TestResultGroupingWithState(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointQuery

	st, header, res := sendTestRequest(queryURL+"/main?q=get+Song+with+ordering(ascending+key)", "GET", nil)

	if st != "200 OK" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	id := header.Get(HTTPHeaderCacheID)

	sr, _ := ResultCache.Get(id)
	ssr := sr.(*APISearchResult)

	ssr.SetSelection(2, true)
	ssr.SetSelection(4, true)
	ssr.SetSelection(5, true)
	ssr.SetSelection(6, true)

	if res := fmt.Sprint(ssr.Selections()); res != "[false false true false true true true false false]" {
		t.Error("Unexpected result: ", res)
		return
	}

	queryURL2 := "http://localhost" + TESTPORT + EndpointQueryResult

	// Put nodes into a groups

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected/g1", "PUT", nil)

	if st != "200 OK" || res == "" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	ssr.NoneSelection()
	ssr.SetSelection(2, true)
	ssr.SetSelection(3, true)

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected/g2", "PUT", nil)

	if st != "200 OK" || res == "" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	ssr.SetSelection(4, true)
	ssr.SetSelection(5, true)
	ssr.SetSelection(6, true)

	if res := fmt.Sprint(ssr.Rows()); res != "[[Aria1 Aria1 8] "+
		"[Aria2 Aria2 2] "+
		"[Aria3 Aria3 4] "+ // Selected - group Best g1 g2
		"[Aria4 Aria4 18] "+ // Selected - group g2
		"[DeadSong2 DeadSong2 6] "+ // Selected - group g1
		"[FightSong4 FightSong4 3] "+ // Selected - group g1
		"[LoveSong3 LoveSong3 1] "+ // Selected - group Best g1
		"[MyOnlySong3 MyOnlySong3 19] "+
		"[StrangeSong1 StrangeSong1 5]]" {
		t.Error("Unexpected result: ", res)
		return
	}
	if res := fmt.Sprint(ssr.Selections()); res != "[false false true true true true true false false]" {
		t.Error("Unexpected result: ", res)
		return
	}

	// Get information about groups

	st, _, gsres := sendTestRequest(queryURL2+id+"/groupselected", "GET", nil)

	if st != "200 OK" || gsres != `
{
  "groups": [
    "Best",
    "g1",
    "g2"
  ],
  "keys": [
    [
      "Aria3",
      "LoveSong3"
    ],
    [
      "Aria3",
      "DeadSong2",
      "FightSong4",
      "LoveSong3"
    ],
    [
      "Aria3",
      "Aria4"
    ]
  ],
  "kinds": [
    [
      "Song",
      "Song"
    ],
    [
      "Song",
      "Song",
      "Song",
      "Song"
    ],
    [
      "Song",
      "Song"
    ]
  ]
}`[1:] {
		t.Error("Unexpected result: ", st, gsres)
		return
	}

	ssr.NoneSelection()
	ssr.SetSelection(2, true)
	ssr.SetSelection(3, true)

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected/g2", "DELETE", nil)

	if st != "200 OK" || res == "" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	ssr.NoneSelection()
	ssr.SetSelection(2, true)
	ssr.SetSelection(4, true)
	ssr.SetSelection(5, true)
	ssr.SetSelection(6, true)

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected/g1", "DELETE", nil)

	if st != "200 OK" || res == "" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected", "PUT", nil)

	if st != "400 Bad Request" || res != "Groupselected can only handle GET and POST requests" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	ssr.Header().(*interpreter.SearchHeader).ResPartition = "foo bar"

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected", "GET", nil)

	if st != "500 Internal Server Error" || res != "GraphError: Invalid data (Partition name foo bar is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	ssr.Header().(*interpreter.SearchHeader).ResPartition = "main"

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected", "GET", nil)

	if st != "200 OK" || res != `
{
  "groups": [
    "Best"
  ],
  "keys": [
    [
      "Aria3",
      "LoveSong3"
    ]
  ],
  "kinds": [
    [
      "Song",
      "Song"
    ]
  ]
}`[1:] {
		t.Error("Unexpected result: ", st, res)
		return
	}

	n, _, err := api.GM.TraverseMulti("main", "Aria3", "Song", ":::group", true)
	data.NodeSort(n)
	if err != nil || fmt.Sprint(n) != `[GraphNode:
     key : Best
    kind : group
]` {
		t.Error(n, err)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected", "POST", []byte(gsres))
	if st != "200 OK" || res == "" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	n, _, err = api.GM.TraverseMulti("main", "Aria3", "Song", ":::group", true)
	data.NodeSort(n)
	if err != nil || fmt.Sprint(n) != `
[GraphNode:
     key : Best
    kind : group
 GraphNode:
     key : g1
    kind : group
 GraphNode:
     key : g2
    kind : group
]`[1:] {
		t.Error(n, err)
		return
	}

	ssr.SetSelection(3, true) // Make sure also item 3 is selected

	st, _, gsres = sendTestRequest(queryURL2+id+"/groupselected", "GET", nil)

	if st != "200 OK" || gsres != `
{
  "groups": [
    "Best",
    "g1",
    "g2"
  ],
  "keys": [
    [
      "Aria3",
      "LoveSong3"
    ],
    [
      "Aria3",
      "DeadSong2",
      "FightSong4",
      "LoveSong3"
    ],
    [
      "Aria3",
      "Aria4"
    ]
  ],
  "kinds": [
    [
      "Song",
      "Song"
    ],
    [
      "Song",
      "Song",
      "Song",
      "Song"
    ],
    [
      "Song",
      "Song"
    ]
  ]
}`[1:] {
		t.Error("Unexpected result: ", st, gsres)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected", "POST", []byte("{:"))

	if st != "400 Bad Request" || res != "Could not decode request body as object with lists of groups, keys and kinds: invalid character ':' looking for beginning of object key string" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected", "POST", []byte(`{"foo":"bar"}`))

	if st != "400 Bad Request" || res != "Wrong data structures in request body - expecting an object with lists of groups, keys and kinds." {
		t.Error("Unexpected result: ", st, res)
		return
	}

	sstate := `
{
  "groups": [
    "Best"
  ],
  "keys": [
    [
      "Aria3",
      "LoveSong3"
    ]
  ],
  "kinds": [
    [
      "Song",
      "Song"
    ]
  ]
}`[1:]

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected", "POST", []byte(sstate))

	if st != "200 OK" || res != sstate { // Response should be the same state which we have put in!
		t.Error("Unexpected result: ", st, res)
		return
	}

	ssr.Header().(*interpreter.SearchHeader).ResPartition = "foo bar"

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected", "POST", []byte(sstate))

	if st != "500 Internal Server Error" || res != "GraphError: Invalid data (Partition name foo bar is not alphanumeric - can only contain [a-zA-Z0-9_])" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	ssr.Header().(*interpreter.SearchHeader).ResPartition = "main"

	// Check the state has been restored

	st, _, gsres = sendTestRequest(queryURL2+id+"/groupselected", "GET", nil)

	if st != "200 OK" || gsres != `
{
  "groups": [
    "Best"
  ],
  "keys": [
    [
      "Aria3",
      "LoveSong3"
    ]
  ],
  "kinds": [
    [
      "Song",
      "Song"
    ]
  ]
}`[1:] {
		t.Error("Unexpected result: ", st, gsres)
		return
	}
}

func TestResultGroupingSpecifiedGroup(t *testing.T) {

	queryURL := "http://localhost" + TESTPORT + EndpointQuery

	st, header, res := sendTestRequest(queryURL+"/main?q=get+Song+with+ordering(ascending+key)", "GET", nil)

	if st != "200 OK" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	id := header.Get(HTTPHeaderCacheID)

	sr, _ := ResultCache.Get(id)
	ssr := sr.(*APISearchResult)

	ssr.SetSelection(2, true)

	if res := fmt.Sprint(ssr.Selections()); res != "[false false true false false false false false false]" {
		t.Error("Unexpected result: ", res)
		return
	}

	if fmt.Sprint(ssr.RowSources()[2]) != "[n:Song:Aria3 n:Song:Aria3 n:Song:Aria3]" {
		t.Error("Unexpected row source:", ssr.RowSources()[2])
		return
	}

	n, _, err := api.GM.TraverseMulti("main", "Aria3", "Song", ":::group", true)
	data.NodeSort(n)
	if err != nil || fmt.Sprint(n) != `[GraphNode:
     key : Best
    kind : group
]` {
		t.Error(n, err)
		return
	}

	queryURL2 := "http://localhost" + TESTPORT + EndpointQueryResult

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected/foo", "GET", nil)

	if st != "400 Bad Request" || res != "Groupselected for a specific group can only handle PUT and DELETE requests" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected/foo", "PUT", nil)

	if st != "200 OK" || res == "" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	n, _, err = api.GM.TraverseMulti("main", "Aria3", "Song", ":::group", true)
	data.NodeSort(n)
	if err != nil || fmt.Sprint(n) != `[GraphNode:
     key : Best
    kind : group
 GraphNode:
     key : foo
    kind : group
]` {
		t.Error(n, err)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected/foo", "DELETE", nil)

	if st != "200 OK" || res == "" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	n, _, err = api.GM.TraverseMulti("main", "Aria3", "Song", ":::group", true)
	data.NodeSort(n)
	if err != nil || fmt.Sprint(n) != `[GraphNode:
     key : Best
    kind : group
]` {
		t.Error(n, err)
		return
	}

	ssr.SearchResult.Header().(*interpreter.SearchHeader).ResPrimaryKind = "foo"

	st, _, res = sendTestRequest(queryURL2+id+"/groupselected/foo", "DELETE", nil)

	if st != "400 Bad Request" || res != "Could not determine key of primary node - query needs a primary expression" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/select", "GET", nil)

	if st != "400 Bad Request" || res != "Could not determine key of primary node - query needs a primary expression" {
		t.Error("Unexpected result: ", st, res)
		return
	}
}

func TestResultSelection(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointQuery

	st, header, res := sendTestRequest(queryURL+"/main?q=get+Song+with+ordering(ascending+key)&offset=2&limit=3", "GET", nil)
	id := header.Get(HTTPHeaderCacheID)

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

	queryURL2 := "http://localhost" + TESTPORT + EndpointQueryResult
	st, _, res = sendTestRequest(queryURL2+id+"/select/", "PUT", []byte(""))

	if st != "400 Bad Request" || res != "Need a selection ('all', 'none', 'invert' or row number)" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	// Test CSV output

	st, _, res = sendTestRequest(queryURL2+id+"/csv", "PUT", nil)
	if st != "400 Bad Request" || res != "Csv can only handle GET requests" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/csv", "GET", nil)

	if st != "200 OK" || res != `
Song Key,Song Name,Ranking
Aria1,Aria1,8
Aria2,Aria2,2
Aria3,Aria3,4
Aria4,Aria4,18
DeadSong2,DeadSong2,6
FightSong4,FightSong4,3
LoveSong3,LoveSong3,1
MyOnlySong3,MyOnlySong3,19
StrangeSong1,StrangeSong1,5`[1:] {
		t.Error("Unexpected result: ", st, res)
		return
	}

	// Test selection

	st, _, res = sendTestRequest(queryURL2+id+"/select/all", "PUT", nil)

	if st != "200 OK" || res != `{
  "total_selections": 9
}` {
		t.Error("Unexpected result: ", st, res)
		return
	}

	sr, _ := ResultCache.Get(id)
	ssr := sr.(*APISearchResult)

	if res := fmt.Sprint(ssr.Selections()); res != "[true true true true true true true true true]" {
		t.Error("Unexpected result: ", res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/select/5", "PUT", nil)

	if st != "200 OK" || res != `{
  "total_selections": 8
}` {
		t.Error("Unexpected result: ", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/select/0", "PUT", nil)

	if st != "200 OK" || res != `{
  "total_selections": 7
}` {
		t.Error("Unexpected result: ", st, res)
		return
	}

	sr, _ = ResultCache.Get(id)
	ssr = sr.(*APISearchResult)

	if res := fmt.Sprint(ssr.Selections()); res != "[false true true true true false true true true]" {
		t.Error("Unexpected result: ", res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/select/0", "PUT", nil)

	if st != "200 OK" || res != `{
  "total_selections": 8
}` {
		t.Error("Unexpected result: ", st, res)
		return
	}

	sr, _ = ResultCache.Get(id)
	ssr = sr.(*APISearchResult)

	if res := fmt.Sprint(ssr.Selections()); res != "[true true true true true false true true true]" {
		t.Error("Unexpected result: ", res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/select/invert", "PUT", nil)

	if st != "200 OK" || res != `{
  "total_selections": 1
}` {
		t.Error("Unexpected result: ", st, res)
		return
	}

	sr, _ = ResultCache.Get(id)
	ssr = sr.(*APISearchResult)

	if res := fmt.Sprint(ssr.Selections()); res != "[false false false false false true false false false]" {
		t.Error("Unexpected result: ", res)
		return
	}

	// Test an error in between

	st, _, res = sendTestRequest(queryURL2+id+"/select/invert2", "PUT", nil)

	if st != "400 Bad Request" || res != "Invalid selection row number" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/select/none", "PUT", nil)

	if st != "200 OK" || res != `{
  "total_selections": 0
}` {
		t.Error("Unexpected result: ", st, res)
		return
	}

	sr, _ = ResultCache.Get(id)
	ssr = sr.(*APISearchResult)

	if res := fmt.Sprint(ssr.Selections()); res != "[false false false false false false false false false]" {
		t.Error("Unexpected result: ", res)
		return
	}

	// Blow away the selection aray and simulate a previous selection

	ssr.selections = []bool{true, false}

	st, _, res = sendTestRequest(queryURL2+id+"/select/2", "PUT", nil)

	if st != "200 OK" || res != `{
  "total_selections": 2
}` {
		t.Error("Unexpected result: ", st, res)
		return
	}

	// Produce an error

	st, _, res = sendTestRequest(queryURL2+id+"/select/2", "DELETE", nil)

	if st != "400 Bad Request" || res != "Select can only handle GET and PUT requests" {
		t.Error("Unexpected result: ", st, res)
		return
	}

	sr, _ = ResultCache.Get(id)
	ssr = sr.(*APISearchResult)

	if res := fmt.Sprint(ssr.Selections()); res != "[true false true false false false false false false]" {
		t.Error("Unexpected result: ", res)
		return
	}

	st, _, res = sendTestRequest(queryURL2+id+"/select", "GET", nil)

	if st != "200 OK" || res != `
{
  "keys": [
    "Aria1",
    "Aria3"
  ],
  "kinds": [
    "Song",
    "Song"
  ]
}`[1:] {
		t.Error("Unexpected result: ", st, res)
		return
	}

	st, _, res = sendTestRequest(queryURL+"/main?rid="+id+"&offset=2&limit=3", "GET", nil)

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
    true,
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
  "total_selections": 2
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}
}

func TestResultFiltering(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointQuery

	st, header, res := sendTestRequest(queryURL+"/main?q=get+filtertest+with+ordering(ascending+key)", "GET", nil)

	if st != "200 OK" || res != `
{
  "header": {
    "data": [
      "1:n:key",
      "1:n:val1",
      "1:n:val2",
      "1:n:val3"
    ],
    "format": [
      "auto",
      "auto",
      "auto",
      "auto"
    ],
    "labels": [
      "Filtertest Key",
      "Val1",
      "Val2",
      "Val3"
    ],
    "primary_kind": "filtertest"
  },
  "rows": [
    [
      "1",
      "test",
      "Hans",
      "foo"
    ],
    [
      "2",
      "test1",
      "Hans",
      "foo"
    ],
    [
      "3",
      "test2",
      "Hans",
      "foo"
    ],
    [
      "4",
      "test3",
      "Peter",
      "foo"
    ],
    [
      "5",
      "test4",
      "Peter",
      "foo"
    ],
    [
      "6",
      "test5",
      "Peter",
      "foo"
    ],
    [
      "7",
      "test6",
      "Anna",
      "foo"
    ],
    [
      "8",
      "test7",
      "Anna",
      "foo"
    ],
    [
      "9",
      "test8",
      "Steve",
      "foo"
    ],
    [
      "10",
      "test9",
      "Steve",
      "foo"
    ],
    [
      "11",
      "test10",
      "Franz",
      "foo"
    ],
    [
      "12",
      "test11",
      "Kevin",
      "foo"
    ],
    [
      "13",
      "test12",
      "Kevin",
      "foo"
    ],
    [
      "14",
      "test13",
      "Kevin",
      "foo"
    ],
    [
      "15",
      "test14",
      "X1",
      "foo"
    ],
    [
      "16",
      "test15",
      "X2",
      "foo"
    ],
    [
      "17",
      "test16",
      "X3",
      "foo"
    ],
    [
      "18",
      "test17",
      "X4",
      "foo"
    ],
    [
      "19",
      "test18",
      "X5",
      "foo"
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
    false,
    false,
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
      "n:filtertest:1",
      "n:filtertest:1",
      "n:filtertest:1",
      "n:filtertest:1"
    ],
    [
      "n:filtertest:2",
      "n:filtertest:2",
      "n:filtertest:2",
      "n:filtertest:2"
    ],
    [
      "n:filtertest:3",
      "n:filtertest:3",
      "n:filtertest:3",
      "n:filtertest:3"
    ],
    [
      "n:filtertest:4",
      "n:filtertest:4",
      "n:filtertest:4",
      "n:filtertest:4"
    ],
    [
      "n:filtertest:5",
      "n:filtertest:5",
      "n:filtertest:5",
      "n:filtertest:5"
    ],
    [
      "n:filtertest:6",
      "n:filtertest:6",
      "n:filtertest:6",
      "n:filtertest:6"
    ],
    [
      "n:filtertest:7",
      "n:filtertest:7",
      "n:filtertest:7",
      "n:filtertest:7"
    ],
    [
      "n:filtertest:8",
      "n:filtertest:8",
      "n:filtertest:8",
      "n:filtertest:8"
    ],
    [
      "n:filtertest:9",
      "n:filtertest:9",
      "n:filtertest:9",
      "n:filtertest:9"
    ],
    [
      "n:filtertest:10",
      "n:filtertest:10",
      "n:filtertest:10",
      "n:filtertest:10"
    ],
    [
      "n:filtertest:11",
      "n:filtertest:11",
      "n:filtertest:11",
      "n:filtertest:11"
    ],
    [
      "n:filtertest:12",
      "n:filtertest:12",
      "n:filtertest:12",
      "n:filtertest:12"
    ],
    [
      "n:filtertest:13",
      "n:filtertest:13",
      "n:filtertest:13",
      "n:filtertest:13"
    ],
    [
      "n:filtertest:14",
      "n:filtertest:14",
      "n:filtertest:14",
      "n:filtertest:14"
    ],
    [
      "n:filtertest:15",
      "n:filtertest:15",
      "n:filtertest:15",
      "n:filtertest:15"
    ],
    [
      "n:filtertest:16",
      "n:filtertest:16",
      "n:filtertest:16",
      "n:filtertest:16"
    ],
    [
      "n:filtertest:17",
      "n:filtertest:17",
      "n:filtertest:17",
      "n:filtertest:17"
    ],
    [
      "n:filtertest:18",
      "n:filtertest:18",
      "n:filtertest:18",
      "n:filtertest:18"
    ],
    [
      "n:filtertest:19",
      "n:filtertest:19",
      "n:filtertest:19",
      "n:filtertest:19"
    ]
  ],
  "total_selections": 0
}`[1:] {
		t.Error("Unexpected response:", st, res)
		return
	}

	queryURL = "http://localhost" + TESTPORT + EndpointQueryResult
	id := header.Get(HTTPHeaderCacheID)

	st, _, res = sendTestRequest(queryURL+id+"/quickfilter/2?limit=10", "PUT", nil)

	if st != "400 Bad Request" || res != "Quickfilter can only handle GET requests" {
		t.Error("Unexpected response:", st, res)
		return
	}

	_, _, res = sendTestRequest(queryURL+id+"/quickfilter/2?limit=10", "GET", nil)

	// The normal case - some values are the same

	if res != `
{
  "frequencies": [
    3,
    3,
    3,
    2,
    2,
    1,
    1,
    1,
    1,
    1
  ],
  "values": [
    "Hans",
    "Kevin",
    "Peter",
    "Anna",
    "Steve",
    "Franz",
    "X1",
    "X2",
    "X3",
    "X4"
  ]
}`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/"+id+"/quickfilter/2", "GET", nil)

	if res != `
{
  "frequencies": [
    3,
    3,
    3,
    2,
    2,
    1,
    1,
    1,
    1,
    1,
    1
  ],
  "values": [
    "Hans",
    "Kevin",
    "Peter",
    "Anna",
    "Steve",
    "Franz",
    "X1",
    "X2",
    "X3",
    "X4",
    "X5"
  ]
}`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	// All values are unique

	_, _, res = sendTestRequest(queryURL+"/"+id+"/quickfilter/1?limit=10", "GET", nil)

	if res != `
{
  "frequencies": [
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1
  ],
  "values": [
    "test",
    "test1",
    "test10",
    "test11",
    "test12",
    "test13",
    "test14",
    "test15",
    "test16",
    "test17"
  ]
}`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	// All values are the same

	_, _, res = sendTestRequest(queryURL+"/"+id+"/quickfilter/3", "GET", nil)

	if res != `
{
  "frequencies": [
    19
  ],
  "values": [
    "foo"
  ]
}`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	// Test error cases

	_, _, res = sendTestRequest(queryURL+"/"+id+"/quickfilter/4", "GET", nil)
	if res != "Invalid query result column" {
		t.Error("Unexpected result: ", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/"+id+"/quickfilter/1?limit=-5", "GET", nil)
	if res != "Invalid parameter value: limit should be a positive integer number" {
		t.Error("Unexpected result: ", res)
		return
	}
	_, _, res = sendTestRequest(queryURL+"/"+id+"/quickfilter/", "GET", nil)
	if res != "Need a query result column to filter" {
		t.Error("Unexpected result: ", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/"+id+"xx/quickfilter/2", "GET", nil)
	if res != "Unknown query result" {
		t.Error("Unexpected result: ", res)
		return
	}

	_, _, res = sendTestRequest(queryURL, "GET", nil)
	if res != "Need a result ID and an operation" {
		t.Error("Unexpected result: ", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"/"+id+"/foo/", "GET", nil)
	if res != "Unknown operation: foo" {
		t.Error("Unexpected result: ", res)
		return
	}
}
