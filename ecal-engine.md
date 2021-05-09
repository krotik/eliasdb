ECA Engine
==========
The ECA engine is ECAL's low-level event engine which does the actual concurrent event processing. Through ECAL a user can define rules which execute certain actions under certain conditions. The engine is defined in `ecal.engine`.

Priorities
----------
The event-based system relies heavily on priorities for control flow. Both events and rules (which are triggered by events) have priorities. By default events and rules have the priority 0 which is the highest priority. Events are processed according to their priority and all triggering rules of a single event are executed according to their priority.

Processor
---------
The processor is the central piece of the event engine. It controls the thread pool, contains the rule index and handles the event processing.

The engines behaviour is solely defined by rules. These rules are added before the engine is started. Each added rule has a priority which determines their execution order if multiple rules are triggered by the same event. The main processing cycle, once the engine has been started, can be described as:

Event injection -> Triggering check -> Rule Matching -> Fire Rules

When injecting a new event it is possible to also pass a monitor with a certain scope and a priority. The scope is used by the processor to narrow down the triggering rules. A possible scenario for scopes are different types of analysis (e.g. quick analysis or deep analysis - only a subset of rules is required for the quick analysis). The priority determines when an event is processed - higher priority events are processed first.

After an event is injected the Processor first checks if anything triggers on the event. The result of this is cached. The trigger check is just a first quick check to determine if the event can be discarded right away - even if the event passes the check, it is possible, that no rule will actually fire.

After the first triggering check passed, the event is handed over to a task which runs in the thread pool. The task uses the rule index to determine all triggering rules. After filtering rules which are out of scope or which are suppressed by other rules, the remaining rules are sorted by their priority and then their action is executed.

A rule action can inject new events into the processor which starts the processing cycle again. The processor supports two modes of execution for rule sequences (rules triggered by an event in order of priority):

1. Do not fail on errors: all rules in a trigger sequence for a specific event
are executed.

2. Fail on first error: the first rule which returns an error will stop
the trigger sequence. Events which have been added by the failing rule are still processed.

Failing on the first error can be useful in scenarios where authorization is required. High priority rules can block lower priority rules from being executed.


Monitor
-------
For every event there is a monitor following the event. Monitors form trees as the events cascade. Monitor objects hold additional information such as priority (how quickly should the associated event be processed), processing errors, rule scope, as well as context objects.


Rules
-----
Rules define the conditions under which a particular action should be executed. Every rule must have the following properties:

- [Name] A name which identifies the rule.
- [KindMatch] Match on event kinds: A list of strings in dot notation which describes event kinds. May contain '*' characters as wildcards (e.g. core.tests.*).
- [ScopeMatch] Match on event cascade scope: A list of strings in dot notation which describe the required scopes which are required for this rule to trigger. The included / excluded scopes for an event are stored in its monitor.
- [StateMatch] Match on event state: A simple list of required key / value states in the event state. Nil values can be used as wildcards (i.e. match is only on key).
- [Priority] Rules are sorted by their priority before their actions are executed.
- [SuppressionList] A list of rules (identified by their name) which should be suppressed if this rule fires.
- [Action] A function which will be executed if this rule fires.


Events
------
Events are injected into the processor and cause rules to fire. An event is a simple object which contains:

- [Name] A name which identifies the event.
- [Kind] An event kind - this is checked against the kind match of rules during the triggering check.
- [State] An event state which contains additional data.

Events are always processed together with a monitor which is either implicitly created or explicitly given together with the event. If the monitor is explicitly given it is possible to specify an event scope which limits the triggering rules and a priority which determines the event processing order. An event with a lower priority is guaranteed to be processed after all events of a higher priority if these have been added before the lower priority event.

Example
-------
- A client instantiates a new Processor giving the number of worker threads which should be used to process rules (a good number here are the cores of the physical processor).

```
proc := NewProcessor(1)
```

- The client adds rules to the processor.

```
rule1 := &Rule{
		"TestRule1",                            // Name
		"My test rule",                         // Description
		[]string{"core.main.event1"},           // Kind match
		[]string{"data"},                       // Match on event cascade scope
		nil,                                    // No state match
		2,                                      // Priority of the rule
		[]string{"TestRule3", "TestRule2"},     // List of suppressed rules by this rule
		func(p Processor, m Monitor, e *Event) error { // Action of the rule
      ... code of the rule

			p.AddEvent(&Event{
							"Next Event",
							[]string{"core", "main", "event2"},
							nil,
						}, m.NewChildMonitor(1))        // New monitor with priority for new event
		},
	}

proc.AddRule(rule1)
...
```

- The processor is started. At this point the thread pool inside the processor is waiting for tasks with the defined number of worker threads.

```
proc.SetRootMonitorErrorObserver(func(rm *RootMonitor) { // Called once a root monitor has finished
	errs := rm.AllErrors()
	...
})

proc.Start()
```

- A root monitor is instantiated and an initial event is added.

```
e := NewEvent(
  "InitialEvent",                      // Name
  []string{"core", "main", "event1"},  // Kind
  map[interface{}]interface{}{         // State
    "foo":  "bar",
  },
)

rootm := proc.NewRootMonitor(nil, nil)

rootm.SetFinishHandler(func(p Processor) { // Handler for end of event cascade
  ...
})

proc.AddEvent(e, rootm)
```

- The event is processed as follows:

	- The event is injected into the procesor with or without a parent monitor.

		- Quick (not complete!) check if the event triggers any rules. This is to avoid unnecessary computation.
			- Check that the event kind is not too general (e.g. the rule is for a.b.c event is for a.b)
			- Check if 	at least one rule matches the kind. At least on rule should either be triggering on all kinds or triggering on the specific kind of the event.

		- Create a new root monitor if no parent monitor has been given.

		- Add a task to the thread pool of the processor (containing the event, parent/root monitor and processor).

	- Thread pool of the processor takes the next task according to the highest priority.

		- Determine the triggering rules (matching via kind, state and scope without suppressed rules).

		- Execute the action of each triggering rule according to their priority.

- The processor can run as long as needed and can be finished when the application should be terminated.

```
proc.Finish()
```
Calling `Finish()` will finish all remaining tasks and then stop the processor.
