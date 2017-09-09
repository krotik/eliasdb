/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package flowutil

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"
)

func TestEventPump(t *testing.T) {
	var res []string

	source1 := &bytes.Buffer{}
	source2 := errors.New("TEST")

	ep := NewEventPump()

	// Add observer 1

	ep.AddObserver("event1", source1, func(event string, eventSource interface{}) {
		if eventSource != source1 {
			t.Error("Unexpected event source:", eventSource)
			return
		}
		res = append(res, "1")
		sort.Strings(res)

	})

	// Add observer 2

	ep.AddObserver("event2", source2, func(event string, eventSource interface{}) {
		if eventSource != source2 {
			t.Error("Unexpected event source:", eventSource)
			return
		}
		res = append(res, "2")
		sort.Strings(res)

	})

	// Add observer 3

	ep.AddObserver("event2", source2, func(event string, eventSource interface{}) {
		if eventSource != source2 {
			t.Error("Unexpected event source:", eventSource)
			return
		}
		res = append(res, "3")
		sort.Strings(res)

	})

	// Run the tests

	// Test 1 straight forward case

	ep.PostEvent("event1", source1)

	if fmt.Sprint(res) != "[1]" {
		t.Error("Unexpected result:", res)
		return
	}

	res = make([]string, 0) // Reset res

	ep.PostEvent("event2", source2)

	if fmt.Sprint(res) != "[2 3]" {
		t.Error("Unexpected result:", res)
		return
	}

	res = make([]string, 0) // Reset res

	ep.PostEvent("event1", source2)

	if fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res)
		return
	}

	// Add observer 4

	ep.AddObserver("", source1, func(event string, eventSource interface{}) {
		if eventSource != source1 {
			t.Error("Unexpected event source:", eventSource)
			return
		}
		res = append(res, "4")
		sort.Strings(res)
	})

	// Add observer 5

	ep.AddObserver("", nil, func(event string, eventSource interface{}) {
		res = append(res, "5")
		sort.Strings(res)
	})

	// Add observer 6

	ep.AddObserver("", source2, func(event string, eventSource interface{}) {
		if eventSource != source2 {
			t.Error("Unexpected event source:", eventSource)
			return
		}
		res = append(res, "6")
		sort.Strings(res)
	})

	res = make([]string, 0) // Reset res

	ep.PostEvent("event1", source2)

	if fmt.Sprint(res) != "[5 6]" {
		t.Error("Unexpected result:", res)
		return
	}

	res = make([]string, 0) // Reset res

	ep.PostEvent("event3", source2)

	if fmt.Sprint(res) != "[5 6]" {
		t.Error("Unexpected result:", res)
		return
	}

	res = make([]string, 0) // Reset res

	ep.PostEvent("event3", source1)

	if fmt.Sprint(res) != "[4 5]" {
		t.Error("Unexpected result:", res)
		return
	}

	res = make([]string, 0) // Reset res

	ep.PostEvent("event3", errors.New("test"))

	if fmt.Sprint(res) != "[5]" {
		t.Error("Unexpected result:", res)
		return
	}

	// Remove observers

	res = make([]string, 0) // Reset res

	ep.PostEvent("event2", source2)

	if fmt.Sprint(res) != "[2 3 5 6]" {
		t.Error("Unexpected result:", res)
		return
	}
	ep.RemoveObservers("event2", source2)

	res = make([]string, 0) // Reset res

	ep.PostEvent("event2", source2)

	if fmt.Sprint(res) != "[5 6]" {
		t.Error("Unexpected result:", res)
		return
	}

	ep.RemoveObservers("", source2) // Remove all handlers specific to source 2

	res = make([]string, 0) // Reset res

	ep.PostEvent("event2", source2)

	if fmt.Sprint(res) != "[5]" {
		t.Error("Unexpected result:", res)
		return
	}

	ep.PostEvent("event1", source1)

	if fmt.Sprint(res) != "[1 4 5 5]" {
		t.Error("Unexpected result:", res)
		return
	}

	ep.RemoveObservers("event1", nil) // Remove all handlers specific to source 2

	res = make([]string, 0) // Reset res

	ep.PostEvent("event2", source2)

	if fmt.Sprint(res) != "[5]" {
		t.Error("Unexpected result:", res)
		return
	}

	ep.RemoveObservers("", nil) // Remove all handlers

	res = make([]string, 0) // Reset res

	ep.PostEvent("event2", source2)

	if fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res)
		return
	}

	// This call should be ignored

	ep.AddObserver("event1", source1, nil)

	if fmt.Sprint(ep.eventsObservers) != "map[]" {
		t.Error("Event map should be empty at this point:", ep.eventsObservers)
		return
	}
}

func TestWrongPostEvent(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Posting events with empty values shouldn't work.")
		}
	}()

	ep := NewEventPump()
	ep.PostEvent("", nil)
}
