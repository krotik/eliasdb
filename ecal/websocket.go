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
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

/*
WebsocketConnection models a single websocket connection.

Websocket connections support one concurrent reader and one concurrent writer.
See: https://godoc.org/github.com/gorilla/websocket#hdr-Concurrency
*/
type WebsocketConnection struct {
	CommID string
	Conn   *websocket.Conn
	RMutex *sync.Mutex
	WMutex *sync.Mutex
}

/*
NewWebsocketConnection creates a new WebsocketConnection object.
*/
func NewWebsocketConnection(commID string, c *websocket.Conn) *WebsocketConnection {
	return &WebsocketConnection{
		CommID: commID,
		Conn:   c,
		RMutex: &sync.Mutex{},
		WMutex: &sync.Mutex{}}
}

/*
Init initializes the websocket connection.
*/
func (wc *WebsocketConnection) Init() {
	wc.WMutex.Lock()
	defer wc.WMutex.Unlock()
	wc.Conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"init_success","payload":{}}`))
}

/*
ReadData reads data from the websocket connection.
*/
func (wc *WebsocketConnection) ReadData() (map[string]interface{}, bool, error) {
	var data map[string]interface{}
	var fatal = true

	wc.RMutex.Lock()
	_, msg, err := wc.Conn.ReadMessage()
	wc.RMutex.Unlock()

	if err == nil {
		fatal = false
		err = json.Unmarshal(msg, &data)
	}

	return data, fatal, err
}

/*
WriteData writes data to the websocket.
*/
func (wc *WebsocketConnection) WriteData(data map[string]interface{}) {
	wc.WMutex.Lock()
	defer wc.WMutex.Unlock()

	jsonData, _ := json.Marshal(map[string]interface{}{
		"commID":  wc.CommID,
		"type":    "data",
		"payload": data,
	})

	wc.Conn.WriteMessage(websocket.TextMessage, jsonData)
}

/*
Close closes the websocket connection.
*/
func (wc *WebsocketConnection) Close(msg string) {
	wc.Conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(
			websocket.CloseNormalClosure, msg), time.Now().Add(10*time.Second))

	wc.Conn.Close()
}
