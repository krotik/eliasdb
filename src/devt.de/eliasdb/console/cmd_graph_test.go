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

	"devt.de/eliasdb/config"
)

func TestGraphCommands(t *testing.T) {
	var out bytes.Buffer
	var export bytes.Buffer

	ResetDB()
	credGiver.Reset()
	createSongGraph()

	// Enable access control

	config.Config[config.EnableAccessControl] = true
	defer func() {
		config.Config[config.EnableAccessControl] = false
	}()

	c := NewConsole("http://localhost"+TESTPORT, &out, credGiver.GetCredentials,
		func() string { return "***pass***" },
		func(args []string, e *bytes.Buffer) error {
			export = *e
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

	if ok, err := c.Run("info"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
┌─────────┬───────────┐
│Kind     │Count      │
├─────────┼───────────┤
│Author   │         2 │
│Producer │         1 │
│Song     │         9 │
│Spam     │        21 │
│Writer   │         1 │
└─────────┴───────────┘
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("part"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
main
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("part foo"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Current partition is: foo
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("part"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
foo
`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

	if ok, err := c.Run("find"); ok || err == nil || err.Error() != "Please specify a search phrase" {
		t.Error(ok, err)
		return
	}

	out.Reset()

	if ok, err := c.Run("find artist"); !ok || err != nil {
		t.Error(ok, err)
		return
	}

	if res := out.String(); res != `
Partition main
Kind      Author
┌───────────────────┬────┬───────┬─────┐
│desc               │key │kind   │name │
├───────────────────┼────┼───────┼─────┤
│A lonely artisT    │000 │Author │John │
│An annoying artist │123 │Author │Mike │
└───────────────────┴────┴───────┴─────┘

Partition main
Kind      Writer
┌────┬───────┬─────┬────────────────────────────┐
│key │kind   │name │text                        │
├────┼───────┼─────┼────────────────────────────┤
│456 │Writer │Hans │A song writer for an artist │
└────┴───────┴─────┴────────────────────────────┘

Partition second
Kind      Producer
┌────┬─────────┬─────┬────────────────────────┐
│key │kind     │name │occupation              │
├────┼─────────┼─────┼────────────────────────┤
│123 │Producer │Jack │A producer of an aRtIsT │
└────┴─────────┴─────┴────────────────────────┘

`[1:] && res != `
Partition main
Kind      Author
┌───────────────────┬────┬───────┬─────┐
│desc               │key │kind   │name │
├───────────────────┼────┼───────┼─────┤
│An annoying artist │123 │Author │Mike │
│A lonely artisT    │000 │Author │John │
└───────────────────┴────┴───────┴─────┘

Partition main
Kind      Writer
┌────┬───────┬─────┬────────────────────────────┐
│key │kind   │name │text                        │
├────┼───────┼─────┼────────────────────────────┤
│456 │Writer │Hans │A song writer for an artist │
└────┴───────┴─────┴────────────────────────────┘

Partition second
Kind      Producer
┌────┬─────────┬─────┬────────────────────────┐
│key │kind     │name │occupation              │
├────┼─────────┼─────┼────────────────────────┤
│123 │Producer │Jack │A producer of an aRtIsT │
└────┴─────────┴─────┴────────────────────────┘

`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	out.Reset()

}
