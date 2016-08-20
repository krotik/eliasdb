/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package util

import "testing"

func TestNamesManager(t *testing.T) {
	nm := NewNamesManager(make(map[string]string))

	if res := nm.newCode16(); res != string([]byte{0x01, 0x00}) {
		t.Error("Unexpected result:", res)
		return
	}
	if res := nm.newCode16(); res != string([]byte{0x02, 0x00}) {
		t.Error("Unexpected result:", res)
		return
	}
	if res := nm.newCode16(); res != string([]byte{0x03, 0x00}) {
		t.Error("Unexpected result:", res)
		return
	}

	if res := nm.newCode32(); res != string([]byte{0x01, 0x00, 0x00, 0x00}) {
		t.Error("Unexpected result:", res)
		return
	}
	if res := nm.newCode32(); res != string([]byte{0x02, 0x00, 0x00, 0x00}) {
		t.Error("Unexpected result:", res)
		return
	}
	if res := nm.newCode32(); res != string([]byte{0x03, 0x00, 0x00, 0x00}) {
		t.Error("Unexpected result:", res)
		return
	}

	code := nm.encode("bb", "myentry", true)

	if name := nm.decode("bb", code); name != "myentry" {
		t.Error("Unexpected result:", name)
		return
	}

	if res := nm.decode("b123b", "123"); res != "" {
		t.Error("Unexpected result:", res)
		return
	}

	if nm.Decode16(nm.Encode16("mykind", true)) != "mykind" {
		t.Error("Unexpected result")
		return
	}
	if nm.Decode32(nm.Encode32("myrole", true)) != "myrole" {
		t.Error("Unexpected result")
		return
	}

	if res := nm.Encode32("mynonexistentstring", false); res != "" {
		t.Error("Unexpected lookup result:", res)
		return
	}
}
