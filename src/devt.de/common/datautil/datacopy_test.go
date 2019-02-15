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

func TestMergeMaps(t *testing.T) {
	m := MergeMaps(map[string]interface{}{
		"a": 1,
		"b": 2,
	}, map[string]interface{}{
		"b": 3,
		"c": 4,
	})

	if len(m) != 3 {
		t.Error("Unexpected number of result entries:", len(m))
		return
	}

	if m["a"] != 1 || m["b"] != 3 || m["c"] != 4 {
		t.Error("Unexpected entries:", m)
		return
	}
}

func TestCopyObject(t *testing.T) {

	var ret2 string

	if err := CopyObject("test", &ret2); err != nil {
		t.Error(err)
		return
	}

	// Test encoding errors

	var ret3 testutil.GobTestObject

	gobtest := &testutil.GobTestObject{Name: "test", EncErr: true, DecErr: false}

	if err := CopyObject(gobtest, &ret3); err == nil || err.Error() != "Encode error" {
		t.Error("Unexpected result:", err)
		return
	}

	gobtest = &testutil.GobTestObject{Name: "test", EncErr: false, DecErr: false}
	ret3 = testutil.GobTestObject{Name: "test", EncErr: false, DecErr: true}

	if err := CopyObject(gobtest, &ret3); err == nil || err.Error() != "Decode error" {
		t.Error("Unexpected result:", err)
		return
	}

	ret3 = testutil.GobTestObject{Name: "test", EncErr: true, DecErr: false}

	if err := CopyObject(&ret3, gobtest); err == nil || err.Error() != "Encode error" {
		t.Error("Unexpected result:", err)
		return
	}

	ret3 = testutil.GobTestObject{Name: "test", EncErr: false, DecErr: false}
	gobtest = &testutil.GobTestObject{Name: "test", EncErr: false, DecErr: true}

	if err := CopyObject(&ret3, gobtest); err == nil || err.Error() != "Decode error" {
		t.Error("Unexpected result:", err)
		return
	}
}
