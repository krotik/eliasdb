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

	"devt.de/krotik/eliasdb/api"
)

func TestECAL(t *testing.T) {
	internalURL := "http://localhost" + TESTPORT + EndpointECALInternal
	publicURL := "http://localhost" + TESTPORT + EndpointECALPublic

	// Test normal log output

	writeScript(`
log("test insert")
`)

	if err := api.SI.Run(); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if err := checkLog(`test insert
`); err != nil {
		t.Error(err)
	}

	writeScript(`
log("test sinks")
sink mysink
  kindmatch [ "db.web.api" ],
  statematch { "method" : "POST", "path" : "xx/ss" }
{
  del(event.state.header, "Accept-Encoding")
  del(event.state.header, "User-Agent")
  log("Got public web request: ", event)
  log("Body data: ", event.state.bodyJSON.data)
  db.raiseWebEventHandled({
	"status" : 201,
	"body" : {
		"mydata" : [1,2,3]
	}
  })
}
sink mysink2
  kindmatch [ "db.web.ecal" ],
  statematch { "method" : "GET" }
{
  del(event.state.header, "Accept-Encoding")
  del(event.state.header, "User-Agent")
  log("Got internal web request: ", event)
  log("Query data: ", event.state.query.xxx)
  raise("aaa")
}
`)

	if err := api.SI.Run(); err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	st, _, res := sendTestRequest(internalURL+"main/n/Test/bar?xxx=1", "GET", nil)

	if st != "404 Not Found" || res != "Resource was not found" {
		t.Error("Unexpected result:", st, res)
		return
	}

	st, header, res := sendTestRequest(publicURL+"xx/ss/?a=1&b=2", "POST", []byte(`
{
  "data": 123
}
`[1:]))

	if st != "201 Created" || header["Content-Type"][0] != "application/json; charset=utf-8" || string(res) != `{
  "mydata": [
    1,
    2,
    3
  ]
}` {
		t.Error("Unexpected result:", st, header, string(res))
		return
	}

	if err := checkLog(`test sinks
Got internal web request: {
  "kind": "db.web.ecal",
  "name": "WebRequest",
  "state": {
    "bodyJSON": null,
    "bodyString": "",
    "header": {
      "Content-Type": [
        "application/json"
      ]
    },
    "method": "GET",
    "path": "main/n/Test/bar",
    "pathList": [
      "main",
      "n",
      "Test",
      "bar"
    ],
    "query": {
      "xxx": [
        "1"
      ]
    }
  }
}
Query data: [
  "1"
]
error: ECAL error in eliasdb-runtime (testscripts/main.ecal): aaa () (Line:26 Pos:3)
Got public web request: {
  "kind": "db.web.api",
  "name": "WebRequest",
  "state": {
    "bodyJSON": {
      "data": 123
    },
    "bodyString": "{\n  \"data\": 123\n}\n",
    "header": {
      "Content-Length": [
        "18"
      ],
      "Content-Type": [
        "application/json"
      ]
    },
    "method": "POST",
    "path": "xx/ss",
    "pathList": [
      "xx",
      "ss"
    ],
    "query": {
      "a": [
        "1"
      ],
      "b": [
        "2"
      ]
    }
  }
}
Body data: 123
`); err != nil {
		t.Error(err)
		return
	}

	oldSI := api.SI
	defer func() {
		api.SI = oldSI
	}()

	api.SI = nil

	st, _, res = sendTestRequest(internalURL, "PUT", nil)

	if st != "404 Not Found" || res != "Resource was not found" {
		t.Error("Unexpected result:", st, res)
		return
	}

	st, _, res = sendTestRequest(internalURL, "DELETE", nil)

	if st != "404 Not Found" || res != "Resource was not found" {
		t.Error("Unexpected result:", st, res)
		return
	}
}
