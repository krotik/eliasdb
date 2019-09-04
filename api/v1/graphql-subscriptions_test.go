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

	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/storage"
	"github.com/gorilla/websocket"
)

func TestGraphQLSubscriptionConnectionErrors(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointGraphQLSubscriptions

	_, _, res := sendTestRequest(queryURL+"main", "GET", nil)

	if res != `Bad Request
websocket: the client is not using the websocket protocol: 'upgrade' token not found in 'Connection' header` {
		t.Error("Unexpected response:", res)
		return
	}
}

func TestGraphQLSubscriptionMissingPartition(t *testing.T) {
	queryURL := "ws://localhost" + TESTPORT + EndpointGraphQLSubscriptions

	// Test missing partition

	c, _, err := websocket.DefaultDialer.Dial(queryURL, nil)
	if err != nil {
		t.Error("Could not open websocket:", err)
		return
	}

	_, message, err := c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "id": "",
  "payload": {
    "errors": [
      "Need a 'partition' in path or as url parameter"
    ]
  },
  "type": "subscription_fail"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	_, _, err = c.ReadMessage()
	if err == nil || err.Error() != "websocket: close 1003 (unsupported data): Need a 'partition' in path or as url parameter" {
		t.Error("Unexpected response:", err)
		return
	}

	if err = c.Close(); err != nil {
		t.Error("Could not close websocket:", err)
		return
	}
}

func TestGraphQLSubscription(t *testing.T) {
	queryURL := "ws://localhost" + TESTPORT + EndpointGraphQLSubscriptions + "main"

	// Test missing partition

	c, _, err := websocket.DefaultDialer.Dial(queryURL, nil)
	if err != nil {
		t.Error("Could not open websocket:", err)
		return
	}

	_, message, err := c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "type": "init_success",
  "payload": {}
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	err = c.WriteMessage(websocket.TextMessage, []byte("buu"))
	if err != nil {
		t.Error("Could not send message:", err)
		return
	}

	_, message, err = c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "id": "",
  "payload": {
    "errors": [
      "invalid character 'b' looking for beginning of value"
    ]
  },
  "type": "subscription_fail"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	err = c.WriteJSON(map[string]interface{}{
		"type":  "subscription_start",
		"id":    "123",
		"query": "subscription { Author { key, ",
	})
	if err != nil {
		t.Error("Could not send message:", err)
		return
	}

	_, message, err = c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "id": "123",
  "payload": {
    "errors": [
      "Parse error in Main query: Unexpected end (Line:1 Pos:29)"
    ]
  },
  "type": "subscription_fail"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	err = c.WriteJSON(map[string]interface{}{
		"type":  "subscription_start",
		"id":    "123",
		"query": "subscription { Author { key, name }}",
	})
	if err != nil {
		t.Error("Could not send message:", err)
		return
	}

	_, message, err = c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "id": "123",
  "type": "subscription_success",
  "payload": {}
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	_, message, err = c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "id": "123",
  "payload": {
    "data": {
      "Author": [
        {
          "key": "123",
          "name": "Mike"
        },
        {
          "key": "456",
          "name": "Hans"
        },
        {
          "key": "000",
          "name": "John"
        }
      ]
    }
  },
  "type": "subscription_data"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	api.GM.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "Hans",
		"kind": "Author",
	}))

	_, message, err = c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "id": "123",
  "payload": {
    "data": {
      "Author": [
        {
          "key": "123",
          "name": "Mike"
        },
        {
          "key": "456",
          "name": "Hans"
        },
        {
          "key": "000",
          "name": "John"
        },
        {
          "key": "Hans",
          "name": null
        }
      ]
    }
  },
  "type": "subscription_data"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	// Insert an error into the db

	sm := gmMSM.StorageManager("mainAuthor.nodes", false)
	msm := sm.(*storage.MemoryStorageManager)
	msm.AccessMap[8] = storage.AccessCacheAndFetchSeriousError

	err = api.GM.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "Hans2",
		"kind": "Author",
	}))

	if err != nil {
		t.Error(err)
		return
	}

	_, message, err = c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "id": "123",
  "payload": {
    "data": {
      "Author": []
    },
    "errors": [
      {
        "locations": [
          {
            "column": 23,
            "line": 1
          }
        ],
        "message": "GraphError: Could not read graph information (Record is already in-use (? - ))",
        "path": [
          "Author"
        ]
      }
    ]
  },
  "type": "subscription_data"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	delete(msm.AccessMap, 8)

	// Create a callback error

	subscriptionCallbackError = fmt.Errorf("Oh dear")

	err = api.GM.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "Hans3",
		"kind": "Author",
	}))

	if err != nil {
		t.Error(err)
		return
	}

	_, message, err = c.ReadMessage()
	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "id": "123",
  "payload": {
    "errors": [
      "Oh dear"
    ]
  },
  "type": "subscription_fail"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	_, _, err = c.ReadMessage()
	if err == nil || err.Error() != "websocket: close 1003 (unsupported data): Oh dear" {
		t.Error("Unexpected response:", err)
		return
	}

	if err = c.Close(); err != nil {
		t.Error("Could not close websocket:", err)
		return
	}
}
