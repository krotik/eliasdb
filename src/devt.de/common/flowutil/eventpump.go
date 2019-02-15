/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package flowutil contains utilities to manage control flow.
*/
package flowutil

import "sync"

/*
EventPump implements the observer pattern. Observers can subscribe to receive
notifications on certain events. Observed objects can send notifications.
*/
type EventPump struct {
	eventsObservers     map[string]map[interface{}][]EventCallback
	eventsObserversLock *sync.Mutex
}

/*
EventCallback is the callback function which is called when an event was observed.
*/
type EventCallback func(event string, eventSource interface{})

/*
NewEventPump creates a new event pump.
*/
func NewEventPump() *EventPump {
	return &EventPump{make(map[string]map[interface{}][]EventCallback), &sync.Mutex{}}
}

/*
AddObserver adds a new observer to the event pump. An observer can subscribe to
a given event from a given event source. If the event is an empty string then
the observer subscribes to all events from the event source. If the
eventSource is nil then the observer subscribes to all event sources.
*/
func (ep *EventPump) AddObserver(event string, eventSource interface{}, callback EventCallback) {

	// Ignore requests with non-existent callbacks

	if callback == nil {
		return
	}

	ep.eventsObserversLock.Lock()
	defer ep.eventsObserversLock.Unlock()

	sources, ok := ep.eventsObservers[event]
	if !ok {
		sources = make(map[interface{}][]EventCallback)
		ep.eventsObservers[event] = sources
	}

	callbacks, ok := sources[eventSource]
	if !ok {
		callbacks = []EventCallback{callback}
		sources[eventSource] = callbacks
	} else {
		sources[eventSource] = append(callbacks, callback)
	}
}

/*
PostEvent posts an event to this event pump from a given event source.
*/
func (ep *EventPump) PostEvent(event string, eventSource interface{}) {
	if event == "" || eventSource == nil {
		panic("Posting an event requires the event and its source")
	}

	ep.eventsObserversLock.Lock()
	defer ep.eventsObserversLock.Unlock()

	postEvent := func(event string, eventSource interface{}) {

		if sources, ok := ep.eventsObservers[event]; ok {
			for source, callbacks := range sources {
				if source == eventSource || source == nil {
					for _, callback := range callbacks {
						ep.eventsObserversLock.Unlock()
						callback(event, eventSource)
						ep.eventsObserversLock.Lock()
					}
				}
			}
		}
	}

	postEvent(event, eventSource)
	postEvent("", eventSource)
}

/*
RemoveObservers removes observers from the event pump. If the event is an
empty string then the observer is removed from all events. If the
eventSource is nil then all observers of the event are dropped.
*/
func (ep *EventPump) RemoveObservers(event string, eventSource interface{}) {
	ep.eventsObserversLock.Lock()
	defer ep.eventsObserversLock.Unlock()

	// Clear everything

	if event == "" && eventSource == nil {
		ep.eventsObservers = make(map[string]map[interface{}][]EventCallback)

	} else if eventSource == nil {
		delete(ep.eventsObservers, event)

	} else if event == "" {
		for _, sources := range ep.eventsObservers {
			delete(sources, eventSource)
		}

	} else {
		if sources, ok := ep.eventsObservers[event]; ok {
			delete(sources, eventSource)
		}
	}
}
