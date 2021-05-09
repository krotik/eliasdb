/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package ecal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/httputil"
	"devt.de/krotik/ecal/engine"
	"github.com/gorilla/websocket"
)

const TESTPORT = ":9090"

func TestWebsocketHandling(t *testing.T) {
	sockUpgrader := websocket.Upgrader{
		Subprotocols:    []string{"ecal-sock"},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	si := NewScriptingInterpreter("", nil)

	http.HandleFunc("/httpserver_test", func(w http.ResponseWriter, r *http.Request) {

		conn, err := sockUpgrader.Upgrade(w, r, nil)
		errorutil.AssertOk(err)

		wsconn := NewWebsocketConnection("123", conn)
		si.RegisterECALSock(wsconn)
		defer func() {
			si.DeregisterECALSock(wsconn)
		}()

		wc := NewWebsocketConnection("123", conn)

		wc.Init()

		data, _, err := wc.ReadData()
		errorutil.AssertOk(err)
		errorutil.AssertTrue(fmt.Sprint(data) == "map[foo:bar]", fmt.Sprint("data is:", data))

		// Simulate that an event is injectd and writes to the websocket

		event := engine.NewEvent(fmt.Sprintf("WebSocketRequest"), []string{"db", "web", "sock", "msg"},
			map[interface{}]interface{}{
				"commID":  "123",
				"payload": "bla",
				"close":   true,
			})

		si.HandleECALSockEvent(nil, nil, event, 0)
	})

	hs := &httputil.HTTPServer{}

	var wg sync.WaitGroup
	wg.Add(1)

	go hs.RunHTTPServer(TESTPORT, &wg)

	wg.Wait()

	// Server is started

	if hs.LastError != nil {
		t.Error(hs.LastError)
		return

	}

	queryURL := "ws://localhost" + TESTPORT + "/httpserver_test"

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

	err = c.WriteMessage(websocket.TextMessage, []byte(`{"foo":"bar"}`))
	if err != nil {
		t.Error("Could not send message:", err)
		return
	}

	_, message, err = c.ReadMessage()

	if msg := formatJSONString(string(message)); err != nil || msg != `{
  "commID": "123",
  "payload": {
    "close": true,
    "commID": "123",
    "payload": "bla"
  },
  "type": "data"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}
}

/*
formatJSONString formats a given JSON string.
*/
func formatJSONString(str string) string {
	out := bytes.Buffer{}
	errorutil.AssertOk(json.Indent(&out, []byte(str), "", "  "))
	return out.String()
}
