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
	"crypto/sha256"
	"errors"
	"fmt"

	"devt.de/common/cryptutil"
	"devt.de/common/timeutil"
)

/*
MaxNonceLifetime is the maximum lifetime for nonces in seconds.
*/
var MaxNonceLifetime int64 = 3600 // One hour

/*
Default nonce related errors
*/
var (
	ErrInvlaidNonce = errors.New("Invalid nonce value")
)

/*
nonces is an internal map which holds all valid nonces
*/
var nonces *MapCache

/*
NewNonce generates a new nonce value. The nonce is invalidated either
after it was consumed or automatically after MaxNonceLifetime seconds.
*/
func NewNonce() string {

	if nonces == nil {

		// Create nonce cache if it doesn't exist yet

		nonces = NewMapCache(0, MaxNonceLifetime)
	}

	// Get a timestamp

	ts := timeutil.MakeTimestamp()

	// Calculate a hash based on a UUID

	uuid := cryptutil.GenerateUUID()
	secPart := sha256.Sum256(uuid[:])

	// Construct the actual nonce and save it

	ret := fmt.Sprintf("%x-%s", secPart, ts)

	nonces.Put(ret, nil)

	return ret
}

/*
CheckNonce checks if a given nonce is valid. The nonce is still valid
after this operation.
*/
func CheckNonce(nonce string) error {

	// Check length

	if len(nonce) == 78 && nonces != nil {

		// Check if the nonce is still valid

		if _, ok := nonces.Get(nonce); ok {
			return nil
		}
	}

	return ErrInvlaidNonce
}

/*
ConsumeNonce consumes a given nonce. The nonce will no longer be valid
after this operation.
*/
func ConsumeNonce(nonce string) error {

	err := CheckNonce(nonce)

	if err == nil {
		nonces.Remove(nonce)
	}

	return nil
}
