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
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
)

func TestNesting(t *testing.T) {

	// Create a nested piece of data which is serialized and deserialized

	var testData1 = map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"atom": 42,
			},
			"atom2": "test5",
		},
	}

	var bb1 bytes.Buffer

	// Only register the generic map[string]interface{}

	gob.Register(map[string]interface{}{})

	if err := gob.NewEncoder(&bb1).Encode(testData1); err != nil {
		t.Error(err)
		return
	}

	var testOut map[string]interface{}

	if err := gob.NewDecoder(&bb1).Decode(&testOut); err != nil {
		t.Error(err)
		return
	}

	val, err := GetNestedValue(testOut, []string{"level1", "level2", "atom"})
	if val != 42 || err != nil {
		t.Error("Unexpected result:", val, err)
		return
	}

	val, err = GetNestedValue(testOut, []string{"level1", "level2"})
	if fmt.Sprint(val) != "map[atom:42]" || err != nil {
		t.Error("Unexpected result:", val, err)
		return
	}

	val, err = GetNestedValue(testOut, []string{"level1", "atom2"})
	if val != "test5" || err != nil {
		t.Error("Unexpected result:", val, err)
		return
	}

	val, err = GetNestedValue(testOut, []string{"level1", "atom3"})
	if val != nil || err != nil {
		t.Error("Unexpected result:", val, err)
		return
	}

	val, err = GetNestedValue(testOut, []string{"level1", "level2", "atom", "test"})
	if val != nil || err.Error() != "Unexpected data type int as value of atom" {
		t.Error("Unexpected result:", val, err)
		return
	}
}
