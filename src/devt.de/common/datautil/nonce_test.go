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
)

func TestNonces(t *testing.T) {

	n1 := NewNonce()
	n2 := NewNonce()

	// Test normal check

	if err := CheckNonce(n1); err != nil {
		t.Error(err)
		return
	}

	// Test consumption

	if err := ConsumeNonce(n1); err != nil {
		t.Error(err)
		return
	}

	if err := CheckNonce(n1); err != ErrInvlaidNonce {
		t.Error("Nonce should no longer be valid")
		return
	}

	// Simulate timeout

	nonces = nil

	if err := CheckNonce(n2); err != ErrInvlaidNonce {
		t.Error("Nonce should no longer be valid")
		return
	}

	// Test error case

	if err := CheckNonce("test"); err != ErrInvlaidNonce {
		t.Error("Nonce should no longer be valid")
		return
	}
}
