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
	"crypto/rand"

	"devt.de/common/errorutil"
)

/*
GenerateUUID generates a version 4 (randomly generated) UUID according to RFC4122.
*/
func GenerateUUID() [16]byte {
	var u [16]byte

	_, err := rand.Read(u[:])
	errorutil.AssertOk(err)

	// Set version 4

	u[6] = (u[6] & 0x0f) | 0x40

	// Set variant bits - variant of RFC 4122

	u[8] = (u[8] & 0xbf) | 0x80

	return u

}
