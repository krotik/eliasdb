/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package cluster

import (
	"fmt"
	"testing"
)

func TestDistributionTable(t *testing.T) {

	// Test simple case - 3 members with replication factor of 2 (normal uint64 location range)

	dt, _ := NewDistributionTable([]string{"a", "b", "c"}, 2)

	if dts := dt.String(); dts != `
Location ranges:
a: 0 -> 6148914691236517204
b: 6148914691236517205 -> 12297829382473034409
c: 12297829382473034410 -> 18446744073709551615
Replicas (factor=2) :
a: [b]
b: [c]
c: [a]
`[1:] {
		t.Error("Unexpected distribution table:", dts)
		return
	}

	// 6 members with replication factor of 4 (location range of 30)

	dt, _ = createDistributionTable([]string{"a", "b", "c", "d", "e", "f"}, 4, 30)

	if dts := dt.String(); dts != `
Location ranges:
a: 0 -> 4
b: 5 -> 9
c: 10 -> 14
d: 15 -> 19
e: 20 -> 24
f: 25 -> 30
Replicas (factor=4) :
a: [b c d]
b: [c d e]
c: [d e f]
d: [e f a]
e: [f a b]
f: [a b c]
`[1:] {
		t.Error("Unexpected distribution table:", dts)
		return
	}

	// Check other functions

	if mr := fmt.Sprint(dt.MemberRange("f")); mr != "25 30" {
		t.Error("Unexpected member range:", mr)
		return
	}

	if mr := fmt.Sprint(dt.MemberRange("a")); mr != "0 4" {
		t.Error("Unexpected member range:", mr)
		return
	}

	if mr := fmt.Sprint(dt.MemberRange("c")); mr != "10 14" {
		t.Error("Unexpected member range:", mr)
		return
	}

	if r := fmt.Sprint(dt.Replicas("c")); r != "[d e f]" {
		t.Error("Unexpected replicas:", r)
		return
	}

	if lh := fmt.Sprint(dt.LocationHome(24)); lh != "e[f a b]" {
		t.Error("Unexpected location home:", lh)
		return
	}

	if lh := fmt.Sprint(dt.LocationHome(20)); lh != "e[f a b]" {
		t.Error("Unexpected location home:", lh)
		return
	}

	if lh := fmt.Sprint(dt.LocationHome(0)); lh != "a[b c d]" {
		t.Error("Unexpected location home:", lh)
		return
	}

	if lh := fmt.Sprint(dt.LocationHome(40)); lh != "f[a b c]" {
		t.Error("Unexpected location home:", lh)
		return
	}

	if om := fmt.Sprint(dt.OtherReplicationMembers(20, "f")); om != "[e a b]" {
		t.Error("Unexpected other replication members:", om)
		return
	}

	if om := fmt.Sprint(dt.OtherReplicationMembers(20, "e")); om != "[f a b]" {
		t.Error("Unexpected other replication members:", om)
		return
	}

	if mr := fmt.Sprint(dt.ReplicationRange("c")); mr != "0 30" {
		t.Error("Unexpected member range:", mr)
		return
	}

	// 2 members with replication factor of 2 (location range of 30)

	dt, _ = createDistributionTable([]string{"a", "b"}, 2, 30)

	if dts := dt.String(); dts != `
Location ranges:
a: 0 -> 14
b: 15 -> 30
Replicas (factor=2) :
a: [b]
b: [a]
`[1:] {
		t.Error("Unexpected distribution table:", dts)
		return
	}

	// 2 members with replication factor of 1 (location range of 30)

	dt, _ = createDistributionTable([]string{"a", "b"}, 1, 30)

	if dts := dt.String(); dts != `
Location ranges:
a: 0 -> 14
b: 15 -> 30
Replicas (factor=1) :
a: []
b: []
`[1:] {
		t.Error("Unexpected distribution table:", dts)
		return
	}

	// 1 members with replication factor of 1 (location range of 30)

	dt, _ = createDistributionTable([]string{"a"}, 1, 30)

	if dts := dt.String(); dts != `
Location ranges:
a: 0 -> 30
Replicas (factor=1) :
a: []
`[1:] {
		t.Error("Unexpected distribution table:", dts)
		return
	}

	// Error cases

	_, err := createDistributionTable([]string{"a"}, 0, 30)
	if err.Error() != "Replication factor must be > 0" {
		t.Error("Unexpected result:", err)
		return
	}

	_, err = createDistributionTable([]string{"a", "b"}, 3, 30)
	if err.Error() != "Not enough members (2) for given replication factor: 3" {
		t.Error("Unexpected result:", err)
		return
	}
}
