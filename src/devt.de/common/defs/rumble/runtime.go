/*
 * Rambazamba
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the MIT
 * License, If a copy of the MIT License was not distributed with this
 * file, You can obtain one at https://opensource.org/licenses/MIT.
 */

package rumble

/*
Runtime accesses the runtime environment of the function.
*/
type Runtime interface {

	/*
	   NewRuntimeError creates a new runtime error.
	*/
	NewRuntimeError(t error, d string) RuntimeError
}
