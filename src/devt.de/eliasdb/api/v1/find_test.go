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

func TestFindQuery(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointFindQuery

	_, _, res := sendTestRequest(queryURL+"?value=Aria1", "GET", nil)
	if res != `
{
  "main": {
    "Song": [
      {
        "key": "Aria1",
        "kind": "Song"
      }
    ]
  },
  "test": {}
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"?text=best-selling+artists", "GET", nil)
	if res != `
{
  "main": {
    "Author": [
      {
        "key": "000",
        "kind": "Author"
      }
    ]
  },
  "test": {
    "Author": [
      {
        "key": "000",
        "kind": "Author"
      }
    ]
  }
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"?text=best-selling+artists&part=test&lookup=1", "GET", nil)
	if res != `
{
  "test": {
    "Author": [
      {
        "desc": "One of the most popular acoustic artists of the decade and one of its best-selling artists.",
        "key": "000",
        "kind": "Author",
        "name": "John"
      }
    ]
  }
}`[1:] {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"?tuxt=best-selling", "GET", nil)
	if res != "Query string for text (word or phrase) or value (exact match) is required" {
		t.Error("Unexpected response:", res)
		return
	}

	_, _, res = sendTestRequest(queryURL+"?text=best-selling&part=foo", "GET", nil)
	if res != "Partition foo does not exist" {
		t.Error("Unexpected response:", res)
		return
	}

}
