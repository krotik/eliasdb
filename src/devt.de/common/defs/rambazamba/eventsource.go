/*
 * Rambazamba
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the MIT
 * License, If a copy of the MIT License was not distributed with this
 * file, You can obtain one at https://opensource.org/licenses/MIT.
 */

package rambazamba

/*
EventPublisher is the API for external event sources to publish events
to Rambazamba engines. The event source should use a given EventPublisher
object to inject events. Use api.RegisterEventSource to create a new
EventPublisher object.
*/
type EventPublisher interface {

	/*
		AddEvent adds a new event to one or more Rambazamba engines.
		Expects 3 parameters: Name - a name which identifies the event,
		Kind - an event kind which is checked against the kind match of
		sinks and State - an event state which contains additional data.
		All of the given parameter will be accessible from Rumble if
		the event triggers a Rumble sink.
	*/
	AddEvent(name string, kind []string, state map[interface{}]interface{}) error
}
