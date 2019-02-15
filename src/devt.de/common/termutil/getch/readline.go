/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package getch

import (
	"bytes"
	"io"
)

/*
ReadLine reads a single line from the terminal. Can optionally include
an echo writer. If the mask is not 0 then the echo will be the mask
character.
*/
func ReadLine(echo io.Writer, mask rune) (string, error) {
	var ret bytes.Buffer
	var err error

	if err = Start(); err == nil {
		var e *KeyEvent

		defer Stop()

		for err == nil && (e == nil || e.Code != KeyEnter) {
			if e, err = Getch(); e != nil {
				if e.Rune != 0 {
					ebytes := []byte(string(e.Rune))

					ret.Write(ebytes)

					if echo != nil {
						if mask == 0 {
							echo.Write(ebytes)
						} else {
							echo.Write([]byte(string(mask)))
						}
					}
				}
			}
		}
	}

	return ret.String(), err
}
