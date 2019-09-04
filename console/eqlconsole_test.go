/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package console

import (
	"bytes"
	"testing"

	"devt.de/krotik/eliasdb/config"
)

func TestEQLConsole(t *testing.T) {
	var out bytes.Buffer

	ResetDB()
	credGiver.Reset()
	createSongGraph()

	// Dummy test

	eqlc := &EQLConsole{}
	eqlc.Commands()

	// Enable access control

	config.Config[config.EnableAccessControl] = true
	defer func() {
		config.Config[config.EnableAccessControl] = false
	}()

	c := NewConsole("http://localhost"+TESTPORT, &out, credGiver.GetCredentials,
		func() string { return "***pass***" },
		func(args []string, e *bytes.Buffer) error {
			return nil
		})

	// Now force the login - we should get one failed login

	out.Reset()

	credGiver.UserQueue = []string{"elias"}
	credGiver.PassQueue = []string{"elias"}

	if ok, err := c.Run("users"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Login as user elias
┌─────────┬─────────────┐
│Username │Groups       │
├─────────┼─────────────┤
│elias    │admin/public │
│johndoe  │public       │
└─────────┴─────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("get Song"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌─────────────┬─────────────┬────────────┐
│Song Key     │Song Name    │Ranking     │
│1:n:key      │1:n:name     │1:n:ranking │
├─────────────┼─────────────┼────────────┤
│StrangeSong1 │StrangeSong1 │5           │
│FightSong4   │FightSong4   │3           │
│DeadSong2    │DeadSong2    │6           │
│LoveSong3    │LoveSong3    │1           │
│MyOnlySong3  │MyOnlySong3  │19          │
│Aria1        │Aria1        │8           │
│Aria2        │Aria2        │2           │
│Aria3        │Aria3        │4           │
│Aria4        │Aria4        │18          │
└─────────────┴─────────────┴────────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()
}
