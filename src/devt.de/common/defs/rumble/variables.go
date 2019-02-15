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
Variables accesses the variable scope of the function.
*/
type Variables interface {

	/*
	   SetValue sets a new value for a variable.
	*/
	SetValue(varName string, varValue interface{}) error

	/*
	   GetValue gets the current value of a variable.
	*/
	GetValue(varName string) (interface{}, bool, error)
}
