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

import (
	"errors"
	"testing"
)

func TestGraphError(t *testing.T) {
	err := GraphError{errors.New("TestError"), ""}

	if err.Error() != "GraphError: TestError" {
		t.Error("Unexpected result", err.Error())
		return
	}

	err = GraphError{errors.New("TestError"), "SomeDetail"}

	if err.Error() != "GraphError: TestError (SomeDetail)" {
		t.Error("Unexpected result", err.Error())
		return
	}
}
