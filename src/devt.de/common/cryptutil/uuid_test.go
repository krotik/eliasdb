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
	"fmt"
	"testing"
)

func TestUUID(t *testing.T) {
	if fmt.Sprint(GenerateUUID()) == "" {
		t.Error("Unexpected result")
		return
	}
}
