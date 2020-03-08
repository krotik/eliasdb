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

import "testing"

func TestToUInt64(t *testing.T) {

	if res := toUInt64("1"); res != 1 {
		t.Error("Unexpected result: ", res)
		return
	}

	if res := toUInt64(uint64(1)); res != 1 {
		t.Error("Unexpected result: ", res)
		return
	}
}
