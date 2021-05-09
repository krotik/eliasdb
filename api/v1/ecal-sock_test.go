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
	"strings"
	"sync"
	"testing"
	"time"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/ecal/cli/tool"
	"devt.de/krotik/ecal/engine"
	"devt.de/krotik/ecal/util"
	"devt.de/krotik/eliasdb/api"
	"github.com/gorilla/websocket"
)

func TestECALSockConnectionErrors(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT + EndpointECALSock

	_, _, res := sendTestRequest(queryURL+"foo?bar=123", "GET", nil)

	if res != `Bad Request
websocket: the client is not using the websocket protocol: 'upgrade' token not found in 'Connection' header` {
		t.Error("Unexpected response:", res)
		return
	}

	oldSI := api.SI
	api.SI = nil
	defer func() {
		api.SI = oldSI
	}()

	_, _, res = sendTestRequest(queryURL+"foo?bar=123", "GET", nil)

	if res != `Resource was not found` {
		t.Error("Unexpected response:", res)
		return
	}
}

func TestECALSock(t *testing.T) {
	queryURL := "ws://localhost" + TESTPORT + EndpointECALSock + "foo?bar=123"
	lastUUID := ""
	var lastDataEvent *engine.Event

	resetSI()
	api.SI.Interpreter = tool.NewCLIInterpreter()
	testScriptDir := "testscripts"
	api.SI.Interpreter.Dir = &testScriptDir
	errorutil.AssertOk(api.SI.Interpreter.CreateRuntimeProvider("eliasdb-runtime"))
	logger := util.NewMemoryLogger(10)
	api.SI.Interpreter.RuntimeProvider.Logger = logger

	errorutil.AssertOk(api.SI.Interpreter.RuntimeProvider.Processor.AddRule(&engine.Rule{
		Name:            "WebSocketRegister",                 // Name
		Desc:            "Handles a websocket communication", // Description
		KindMatch:       []string{"db.web.sock"},             // Kind match
		ScopeMatch:      []string{},
		StateMatch:      nil,
		Priority:        0,
		SuppressionList: nil,
		Action: func(p engine.Processor, m engine.Monitor, e *engine.Event, tid uint64) error {
			lastUUID = fmt.Sprint(e.State()["commID"])
			return nil
		},
	}))

	wg := &sync.WaitGroup{}

	errorutil.AssertOk(api.SI.Interpreter.RuntimeProvider.Processor.AddRule(&engine.Rule{
		Name:            "WebSocketHandler",                  // Name
		Desc:            "Handles a websocket communication", // Description
		KindMatch:       []string{"db.web.sock.data"},        // Kind match
		ScopeMatch:      []string{},
		StateMatch:      nil,
		Priority:        0,
		SuppressionList: nil,
		Action: func(p engine.Processor, m engine.Monitor, e *engine.Event, tid uint64) error {
			lastDataEvent = e
			wg.Done()
			return nil
		},
	}))

	api.SI.Interpreter.RuntimeProvider.Processor.Start()
	defer api.SI.Interpreter.RuntimeProvider.Processor.Finish()

	// Now do the actual testing

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
  "commID": "`+lastUUID+`",
  "payload": {
    "error": "invalid character 'b' looking for beginning of value"
  },
  "type": "data"
}` {
		t.Error("Unexpected response:", msg, err)
		return
	}

	wg.Add(1)

	err = c.WriteMessage(websocket.TextMessage, []byte(`{"foo":"bar"}`))
	if err != nil {
		t.Error("Could not send message:", err)
		return
	}

	wg.Wait()

	if data := lastDataEvent.State()["data"]; err != nil || fmt.Sprint(data) != `map[foo:bar]` {
		t.Error("Unexpected response:", data, err)
		return
	}

	err = c.WriteMessage(websocket.TextMessage, []byte(`{"close":true}`))
	if err != nil {
		t.Error("Could not send message:", err)
		return
	}

	// Reset the connection and provoke an error

	c, _, err = websocket.DefaultDialer.Dial(queryURL, nil)
	if err != nil {
		t.Error("Could not open websocket:", err)
		return
	}

	c.Close()

	for {

		if logger.Size() > 0 {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if !strings.Contains(logger.String(), "unexpected EOF") && !strings.Contains(logger.String(), "connection reset by peer") {
		t.Error("Unexpected log output:", logger.String())
		return
	}
}
