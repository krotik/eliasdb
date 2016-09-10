/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package parser

/*
RuntimeProvider provides runtime components for a parse tree.
*/
type RuntimeProvider interface {

	/*
	   Runtime returns a runtime component for a given ASTNode.
	*/
	Runtime(node *ASTNode) Runtime
}

/*
Runtime provides the runtime for an ASTNode.
*/
type Runtime interface {

	/*
	   Validate this runtime component and all its child components.
	*/
	Validate() error

	/*
		Eval evaluate this runtime component.
	*/
	Eval() (interface{}, error)
}
