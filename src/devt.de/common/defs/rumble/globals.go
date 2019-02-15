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

import (
	"errors"
)

/*
Default variables for sinks
*/
const (
	VarProcessor = "processor" // Current event processor (new sinks will be added to this)
	VarMonitor   = "monitor"   // Current event monitor (new events will be using this)
	VarEvent     = "event"     // Current event which triggered a sink
)

/*
Runtime related error types - these errors are generic errors of Rumble
where the code will not check for object equality
*/
var (
	ErrInvalidConstruct = errors.New("Invalid construct")
	ErrInvalidState     = errors.New("Invalid state")
	ErrVarAccess        = errors.New("Cannot access variable")
	ErrNotANumber       = errors.New("Operand is not a number")
	ErrNotABoolean      = errors.New("Operand is not a boolean")
	ErrNotAList         = errors.New("Operand is not a list")
	ErrNotAMap          = errors.New("Operand is not a map")
	ErrNotAListOrMap    = errors.New("Operand is not a list nor a map")
)

/*
RuntimeError is a special error which contains additional internal
information which are not exposed (e.g. code line).
*/
type RuntimeError error
