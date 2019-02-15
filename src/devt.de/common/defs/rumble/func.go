/*
 * Rambazamba
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the MIT
 * License, If a copy of the MIT License was not distributed with this
 * file, You can obtain one at https://opensource.org/licenses/MIT.
 */

/*
Package rumble contains all definitions which external code should use to
integrate with Rambazamba.
*/
package rumble

/*
Function is a function in Rumble.
*/
type Function interface {

	/*
		Name returns the name of the function. A function should be camelCase
		and should only contain alphanumerical characters.
	*/
	Name() string

	/*
		Validate is called to validate the number of arguments, check the
		environment and to execute any initialisation code which might be
		necessary for the function.
	*/
	Validate(argsNum int, runtime Runtime) RuntimeError

	/*
		Execute executes the rumble function. This function might be called
		by several threads concurrently.
	*/
	Execute(argsVal []interface{}, vars Variables, runtime Runtime) (interface{}, RuntimeError)
}
