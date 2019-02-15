/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package cryptutil

import (
	"testing"
)

func TestStringEncryption(t *testing.T) {

	secret := "This is a test"

	encString, err := EncryptString("foo", secret)
	if err != nil {
		t.Error(err)
		return
	}

	decString, err := DecryptString("foo", encString)
	if err != nil {
		t.Error(err)
		return
	}

	if decString != secret {
		t.Error("Unexpected result:", decString, secret)
		return
	}

	decString, err = DecryptString("foo1", encString)
	if err.Error() != "Could not decrypt data" {
		t.Error(err)
		return
	}

	if decString != "" {
		t.Error("Unexpected result:", decString)
		return
	}

	decString, err = DecryptString("foo1", "bar")
	if err.Error() != "Ciphertext is too short - must be at least: 16" {
		t.Error(err)
		return
	}
}
