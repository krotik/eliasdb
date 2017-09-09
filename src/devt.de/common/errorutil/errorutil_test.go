/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package errorutil

import (
	"errors"
	"testing"
)

func TestAssertOk(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Giving AssertOk an error should cause a panic.")
		}
	}()

	AssertOk(errors.New("test"))
}

func TestAssertTrue(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Giving AssertTrue a negative condition should cause a panic.")
		}
	}()

	AssertTrue(false, "bla")
}

func TestCompositeError(t *testing.T) {

	ce := NewCompositeError()

	if ce.HasErrors() {
		t.Error("CompositeError object shouldn't have any errors yet")
		return
	}

	ce.Add(errors.New("test1"))

	if !ce.HasErrors() {
		t.Error("CompositeError object should have one error by now")
		return
	}

	ce.Add(errors.New("test2"))

	// Add a CompositeError to a CompositeError

	ce2 := NewCompositeError()
	ce2.Add(errors.New("test3"))
	ce.Add(ce2)

	if ce.Error() != "test1; test2; test3" {
		t.Error("Unexpected output:", ce.Error())
	}
}
