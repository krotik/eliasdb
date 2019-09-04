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

func TestGraphQLConsole(t *testing.T) {
	var out bytes.Buffer

	ResetDB()
	credGiver.Reset()
	createSongGraph()

	// Dummy test

	graphqlc := &GraphQLConsole{}
	graphqlc.Commands()

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

	if ok, err := c.Run("{ Song { key, name, ranking }}"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
{
  "data": {
    "Song": [
      {
        "key": "StrangeSong1",
        "name": "StrangeSong1",
        "ranking": 5
      },
      {
        "key": "FightSong4",
        "name": "FightSong4",
        "ranking": 3
      },
      {
        "key": "DeadSong2",
        "name": "DeadSong2",
        "ranking": 6
      },
      {
        "key": "LoveSong3",
        "name": "LoveSong3",
        "ranking": 1
      },
      {
        "key": "MyOnlySong3",
        "name": "MyOnlySong3",
        "ranking": 19
      },
      {
        "key": "Aria1",
        "name": "Aria1",
        "ranking": 8
      },
      {
        "key": "Aria2",
        "name": "Aria2",
        "ranking": 2
      },
      {
        "key": "Aria3",
        "name": "Aria3",
        "ranking": 4
      },
      {
        "key": "Aria4",
        "name": "Aria4",
        "ranking": 18
      }
    ]
  }
}`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()
}
