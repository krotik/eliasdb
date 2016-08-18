/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package datautil

import (
	"testing"

	"devt.de/common/testutil"
)

func TestCopyObject(t *testing.T) {

	var ret2 string

	if err := CopyObject("test", &ret2); err != nil {
		t.Error(err)
		return
	}

	// Test encoding errors

	var ret3 testutil.GobTestObject

	gobtest := &testutil.GobTestObject{"test", true, false}

	if err := CopyObject(gobtest, &ret3); err == nil || err.Error() != "Encode error" {
		t.Error("Unexpected result:", err)
		return
	}

	gobtest = &testutil.GobTestObject{"test", false, false}
	ret3 = testutil.GobTestObject{"test", false, true}

	if err := CopyObject(gobtest, &ret3); err == nil || err.Error() != "Decode error" {
		t.Error("Unexpected result:", err)
		return
	}

	ret3 = testutil.GobTestObject{"test", true, false}

	if err := CopyObject(&ret3, gobtest); err == nil || err.Error() != "Encode error" {
		t.Error("Unexpected result:", err)
		return
	}

	ret3 = testutil.GobTestObject{"test", false, false}
	gobtest = &testutil.GobTestObject{"test", false, true}

	if err := CopyObject(&ret3, gobtest); err == nil || err.Error() != "Decode error" {
		t.Error("Unexpected result:", err)
		return
	}
}
