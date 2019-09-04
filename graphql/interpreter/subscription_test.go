/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package interpreter

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"devt.de/krotik/eliasdb/graph/data"
)

type testCallbackHandler struct {
	wg       *sync.WaitGroup
	messages *bytes.Buffer
	finished bool
}

func (ch *testCallbackHandler) Publish(data map[string]interface{}, err error) {
	ch.messages.WriteString(formatData(data))
	ch.messages.WriteString("\n--\n")
}

func (ch *testCallbackHandler) IsFinished() bool {
	if ch.wg != nil {
		ch.wg.Done()
	}
	return ch.finished
}

func TestSubscription(t *testing.T) {
	gm, _ := songGraphGroups()

	if err := gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "StrangeSong1",
		"kind": "NewSong",
		"name": "bar",
	})); err != nil {
		t.Error(err)
		return
	}

	// Test fragments for different return types

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
subscription {
  Song(key : "StrangeSong1") {
    name
  }
}
`,
		"variables": nil,
	}

	cbh := &testCallbackHandler{&sync.WaitGroup{}, bytes.NewBufferString(""), false}
	cbh2 := &testCallbackHandler{nil, bytes.NewBufferString(""), false}

	res, err := runQuery("test", "main", query, gm, cbh, false)
	runQuery("test", "main", query, gm, cbh2, false)

	if f := formatData(res); f != `{
  "data": {
    "Song": [
      {
        "name": "StrangeSong1"
      }
    ]
  }
}` || err != nil {
		t.Error("Unexpected result:", f, err)
		return
	}

	if cbh.messages.String() != `` {
		t.Error("Unexpected result:", cbh.messages.String())
		return
	}

	cbh.wg.Add(2)

	if err = gm.UpdateNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "StrangeSong1",
		"kind": "Song",
		"name": "foo",
	})); err != nil {
		t.Error(err)
		return
	}

	if err = gm.UpdateNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "StrangeSong1",
		"kind": "NewSong",
		"name": "bar",
	})); err != nil {
		t.Error(err)
		return
	}

	time.Sleep(5 * time.Millisecond)

	cbh.wg.Wait()

	if m := cbh.messages.String(); m != `{
  "data": {
    "Song": [
      {
        "name": "foo"
      }
    ]
  }
}
--
` {
		t.Error("Unexpected result:", m)
		return
	}

	cbh.wg.Add(1)

	cbh.finished = true

	if err = gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "StrangeSong1",
		"kind": "Song",
		"name": "bar",
	})); err != nil {
		t.Error(err)
		return
	}

	time.Sleep(5 * time.Millisecond)

	cbh.wg.Wait()

	if m := cbh.messages.String(); m != `{
  "data": {
    "Song": [
      {
        "name": "foo"
      }
    ]
  }
}
--
{
  "data": {
    "Song": [
      {
        "name": "bar"
      }
    ]
  }
}
--
` {
		t.Error("Unexpected result:", m)
		return
	}

	// Ensure the subscription handler is gone

	if r := len(ruleMap); r != 1 {
		t.Error("Unexpected number of created rules:", r)
		return
	}

	for _, rule := range ruleMap {
		if r := len(rule.handlers); r != 1 {
			t.Error("Unexpected number of handlers:", r)
		}
	}

	// Reset rule Map

	ruleMap = make(map[string]*SystemRuleGraphQLSubscriptions)
}
