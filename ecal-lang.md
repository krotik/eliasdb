ECAL - Event Condition Action Language
--
ECAL is a language to create a rule based system which reacts to events. Events are handled by actions which are guarded by conditions:

Event -> Condition -> Action

The condition and action part are defined by rules called event sinks which are the core constructs of ECAL.

Notation
--
Source code is Unicode text encoded in UTF-8. Single language statements are separated by a semicolon or a newline.

Constant values are usually enclosed in double quotes "" or single quotes '', both supporting escape sequences. Constant values can also be provided as raw strings prefixing a single or double quote with an 'r'. A raw string can contain any character including newlines and does not contain escape sequences.

Blocks are denoted with curly brackets. Most language constructs (conditions, loops, etc.) are very similar to other languages.

Scope and imports
--
ECAL is a block scoped language. Everything in ECAL is defined as a symbol within a scope. Scopes form a composition structure in which a given scope can contain multiple inner scopes. Inner scopes can access symbols in outer scopes while outer scopes cannot access symbols defined in inner scopes. A symbol defined in an outer scope can be redefined within the boundaries of an inner scope without modifying the symbol of the outer scope. The widest scope is the global scope which contains all top-level definitions. Sinks, Functions and variables are possible symbols in a scope.

ECAL has import statements which can import ECAL symbol definitions from another file into the current scope. All import locations are relative to the root directory from which all ECAL files are being parsed. It is not possible to import ECAL files relative to the directory of an importing ECAL file.

Example:
```
import "foo/bar.ecal" as foobar

foobar.doSomething()
```

Event Sinks
--
Event sinks are the core constructs of ECAL which provide concurrency and the means to respond to events of an external system. Sinks provide ECAL with an interface to an [event condition action engine](ecal-engine.md) which coordinates the parallel execution of code. Sinks cannot be scoped into modules or objects and are usually declared at the top level. They must only access top level variables within mutex blocks. Sinks have the following form:
```
sink mysink
    kindmatch [ "foo.bar.*" ],
    scopematch [ "data.read", "data.write" ],
    statematch { "a" : 1, "b" : NULL },
    priority 0,
    suppresses [ "myothersink" ]
    {
      <ECAL Code>
    }
```
Sinks must have unique names and may have the following attributes:

Attribute | Description
-|-
kindmatch  | Matching condition for event kind. A list of strings in dot notation which describes event kinds which should trigger this event. May contain `*` characters as wildcards.
scopematch | Matching condition for event cascade scope. A list of strings in dot notation which describe the scopes which are required for this sink to trigger.
statematch | Match on event state: A simple map of required key / value states in the event state. `NULL` values can be used as wildcards (i.e. match is only on key).
priority | Priority of the sink. Sinks of higher priority are executed first. The higher the number the lower the priority - 0 is the highest priority.
suppresses | A list of sink names which should be suppressed if this sink is executed.

It is possible to add events through code via the asynchronous function `addEvent` and the synchronous function `addEventAndWait`. The former should be used within sinks to form event cascades which allow the code to run concurrently. The latter should be used to start event cascades. The function will wait until all sinks which were triggered by this event have finished and then return an error object. The error object is a data structure which contains all errors which have happened during an event cascade. Errors can either happen as runtime errors or explicitly when using the `raise` function.
```
sink mysink
    kindmatch [ "web.page.*" ],
	{
    ...
    raise("MyCustomError", "Custom message", ["some detail data"])
    ...
	}

res := addEventAndWait("request", "web.page.index", {})
```
In the example above `res` will have the following form:
```
[
  {
    "errors": {
      "mysink": {
        "detail": [
          "some detail data"
        ],
        "message": "ECAL error in ECALTestRuntime: MyCustomError (Custom message) (Line:xx Pos:xx)"
      }
    },
    "event": {
      "kind": "web.page.index",
      "name": "request",
      "state": {}
    }
  }
]
```
The event function has the required parameters of event name, kind, state and an optional parameter which defines the scope. The event name has no operational meaning other than identifying a particular event. The event kind is the main mechanism for selecting sinks - sinks can match kinds with different levels of precision. The event state is mainly used to attach data to events but can also be used by sinks for a triggering condition. Scopes can be used to define domains for rules. Defining a scope will always start a new event cascade. A sink will only trigger if all it's scopes are met by an event cascade.
 ```
res := addEventAndWait("request", "foo.bar.xxx", {
  "payload" : 123  
}, {
  "data.read" : true,
  "data.write" : false
})
 ```
The order of execution of sinks can be controlled via their priority. All sinks which are triggered by a particular event will be executed in order of their priority.

Mutex blocks
--
To protect shared resource when handling concurrent events, ECAL supports mutex blocks. Mutex blocks which share the same name can only be accessed by one thread at a given time:
```
mutex myresource {
  globalResource := "new value"
}
```

Functions
--
Functions define reusable pieces of code dedicated to perform a particular task based on a set of given input values. In ECAL functions are first-class citizens in that they can be assigned to variables and  passed as arguments. Each parameter can have a default value which is by default NULL.

Example:
```
func myfunc(a, b, c=1) {
  <ECAL Code>
}
```

Primitive values are passed by value, composition structures like maps and lists are passed by reference. Local variables should be defined using the `let` statement.

Example:
```
a := 1
func myfunc(a, b, c=1) {
  let a := 2 # Local to myfunc
}
```

Comments
--
Comments are defined with `#` as single line comments and `/*` `*/` for multiline comments.
Single line comments will comment all characters after the `#` until the next newline.
```
/*
  Multi line comment
  Some comment text
*/

# Single line comment

a := 1 # Single line comment after a statement
```

Literal Values
--
Literal values are used to initialize variables or as operands in expressions.

Numbers can be expressed in all common notations:
Formatting|Description
-|-
123|Normal integer
123.456|With decimal point
1.234560e+02|Scientific notation

Strings can be normal quoted stings which interpret backslash escape characters:
```
\a → U+0007 alert or bell
\b → U+0008 backspace
\f → U+000C form feed
\n → U+000A line feed or newline
\r → U+000D carriage return
\t → U+0009 horizontal tab
\v → U+000b vertical tab
\\ → U+005c backslash
\" → U+0022 double quote
\uhhhh → a Unicode character whose codepoint can be expressed in 4 hexadecimal digits. (pad 0 in front)
```

Normal quoted strings also interpret inline expressions and statements escaped with `{{}}`:
```
"Foo bar {{1+2}}"
```

Strings can also be expressed in raw form which will not interpret any escape characters.
```
r"Foo bar {{1+2}}"
```

Some examples:

Expression|Value
-|-
`"foo'bar"`| `foo'bar`
`'foo"bar'`| `foo"bar`
`'foo\u0028bar'`| `foo(bar`
`"foo\u0028bar"`| `foo(bar`
`"Foo bar {{1+2}}"`| `Foo bar 3`
`r"Foo bar {{1+2}}"`| `Foo bar {{1+2}}`

Variable Assignments
--
A variable is a storage bucket for holding a value. Variables can hold primitive values (strings and numbers) or composition structures like an array or a map. Variables names can only contain [a-zA-Z] and [a-zA-Z0-9] from the second character.

A variable is assigned with the assign operator ':='
```
a := 1
b := "test"
c := [1,2,3]
d := {1:2,3:4}
```
Multi-assignments are possible using lists:
```
[a, b] := [1, 2]
```

Expressions
--
Variables and constants can be combined with operators to form expressions. Boolean expressions can also be formed with variables:
```
a := 1 + 2 * 5
b := a > 10
c := a == 11
d := false or c
```

Operators
--
The following operators are available:

Boolean: `and`, `or`, `not`, `>`, `>=`, `<`, `<=`, `==`, `!=`

Arithmetic: `+`, `-`, `*`, `/`, `//` (integer division), `%` (integer modulo)

String:
Operator|Description|Example
-|-|-
like|Regex match|`"Hans" like "H??s"`
hasPrefix|prefix match|`"Hans" hasPrefix "Ha"`
hasSuffix|suffix match|`"Hans" hasSuffix "ns"`

List:
Operator|Description|Example
-|-|-
in|Item is in list|`6 in [1, 6, 7]`
notin|Item is not in list|`6 notin [1, 6, 7]`

Composition structures access
--
Composition structures like lists and maps can be accessed with access operators:

Structure|Accessor|Description
-|-|-
List|variable[index]|Access the n-th element starting from 0.
Map|variable[field]|Access a map
Map|variable.field|Access a map (field name can only contain [a-zA-Z] and [a-zA-Z0-9] from the second character)
```
a := [1, 2, 3]
b := a[1] # B has the value 2

c := { "foo" : 2 }
d := c["foo"]
e := c.foo
```

Object-oriented programming structures
--
ECAL supports Object-oriented programming by providing the concept of objects containing data as properties and code in the form of methods. Methods can access properties of their object by using the variable `this`. Objects can be initialized with a constructor. Objects can inherit data and properties from each other. Multiple inheritance is allowed. Constructors of super map structures can be called by using the `super` function list variable available to the constructor of an object.

Operator|Description
-|-|-
new|In-build function to instantiate a map structure into an object
super|Property with a list value containing all super map structures and constructor method variable which contains a list of all super map structure constructors
init|Attribute with a constructor function as value - this function can use the variable `super` to access constructors of super map structures
this|Method variable containing the instantiated object

Example:
```
Bar := {
  ...
}

Foo := {
  "super" : [ Bar ]

  # Object IDs
  #
  "id" : 0
  "idx" : 0

  # Constructor
  #
  "init" : func(id) {
    super[0]()
    this.id := id
  }

  # Return the object ID
  #
  "getId" : func() {
      return this.idx
  }

  # Set the object ID
  #
  "setId" : func(id) {
      this.idx := id
  }
}

FooObject := new(Foo, 123)
FooObject.setId(500)
result := FooObject.getId() + FooObject.id # 623
```

Loop statements
--
All loops are defined as a 'for' block statement. Counting loops are defined with the 'range' function. The following code iterates from 2 until 10 in steps of 2:
```
for a in range(2, 10, 2) {
	<ECAL Code>
}
```

Conditional loops are using a condition after the for statement:
```
for a > 0 {
  <ECAL Code>
}
```

It is possible to loop over lists and even have multiple assignments:
```
for [a, b] in [[1, 1], [2, 2], [3, 3]] {

}
```
or
```
x := { "c" : 0, "a" : 2, "b" : 4}
for [a, b] in x {
  <ECAL Code>
}
```

Conditional statements
--
The "if" statement specifies the conditional execution of multiple branches based on defined conditions:
```
if a == 1 {
    a := a + 1
} elif a == 2 {
    a := a + 2
} else {
    a := 99
}
```

Try-except blocks
--
ECAL uses try-except blocks to handle error states. Errors can either happen while executing statements or explicitly by using the `raise` function. Code which should only be executed if no errors happened can be put into an `otherwise` block. Code which should be executed regardless can be put into a `finally` block.
```
try {
    raise("MyError", "My error message", [1,2,3])
} except "MyError" as e {
    log(e)
} otherwise {
    log("No error happened")
} finally {
    log("Try block was left")
}
```
The variable `e` has the following structure:
```
{
  "data": [
    1,
    2,
    3
  ],
  "detail": "My error message",
  "error": "ECAL error in console: MyError (My error message) (Line:1 Pos:7)",
  "line": 1,
  "pos": 7,
  "source": "console",
  "type": "MyError"
}
```

Build-in Functions
--
ECAL has a number of function which are build-in that are always available:

#### `raise([error type], [error detail], [data]) : error`
Raise returns a runtime error. Outside of sinks this will stop the code execution
if the error is not handled by try / except. Inside a sink only the specific sink
will fail.

Parameter | Description
-|-
error type | Error type e.g. 'Permission error'
error detail | Error details e.g. human-readable error message
data | Additional data for the error handling

Example:
```
raise("MyError", "Some detail message", [1, 2, 3])
```

#### `range([start], end, [step]) : <iterator>`
Range function which can be used to iterate over number ranges. The parameters start and step are optional.

Parameter | Description
-|-
start | Start of the number range (first returned number)
end | End of the range (last returned number within step)
step | Difference between each number (can be negative)

Example:
```
for i in range(10, 2, -2) {
  ...
}
```

#### `len(listormap) : number`
Len returns the size of a list or map.

Parameter | Description
-|-
listormap | A list or a map

Example:
```
len([1,2,3])
```

#### `del(listormap, indexorkey) : listormap`
Del removes an item from a list or map. Only the returned value should be used further.

Parameter | Description
-|-
listormap | A list or a map
indexorkey | The index of a list or key of a map which should be removed

Example:
```
del([1,2,3], 1)
```


#### `add(list, value, [index]) : list`
Add adds an item to a list. The item is added at the optionally given index or at the end if no index is specified. Only the returned value should be used further.

Parameter | Description
-|-
list | A list
value | The value which should be added to the list
index | The index at which the item should be added

Example:
```
add([1,2,3], 1, 0)
```

#### `concat(list1, list2, [listn ...]) : list`
Joins one or more lists together. The result is a new list.

Parameter | Description
-|-
list1 ... n | Lists to join

Example:
```
concat([1,2,3], [4,5,6], [7,8,9])
```

#### `dumpenv() : string`
Returns the current variable environment as a string.

Example:
```
dumpenv()
```

#### `doc(function) : string`
Returns the doc string of a function.

Parameter | Description
-|-
function | A function object

Example:
```
doc(len)
```

#### `sleep(micros)`
Sleep pauses the current thread for a number of micro seconds.

Parameter | Description
-|-
micros | Number of micro seconds to sleep

Example:
```
sleep(1000000) // Sleep a millisecond
```

#### `setCronTrigger(cronspec, eventname, eventkind) : string`
Adds a periodic cron job which fires events. Use this function for long running
periodic tasks.

The function requires a cronspec which defines the time schedule in which events
should be fired. The cronspec is a single text string which must have the
following 6 entries separated by whitespace:

```
Field	         Valid values
-----	         ------------
second         * or 0-59 or *%1-59
minute         * or 0-59 or *%1-59
hour           * or 0-23 or *%1-23
day of month   * or 1-31 or *%1-31
month          * or 1-12 or *%1-12
day of week    * or 0-6 (0 is Sunday) or *%1-7
```

Multiple values for an entry can be separated by commas e.g. `1,3,5,7`.
A `*` in any field matches all values i.e. execute every minute, every
day, etc. A `*%<number>` in any field entry matches when the time is a
multiple of <number>.

Returns a human readable string representing the cronspec.

For example `0 0 12 1 * *` returns `at the beginning of hour 12:00 on 1st of every month`.

Parameter | Description
-|-
cronspec  | The cron job specification string
eventname | Event name for the cron triggered events
eventkind | Event kind for the cron triggered events

Example:
```
# at second 1 of minute 1 of every 10th hour every day
setCronTrigger("1 1 *%10 * * *", "cronevent", "foo.bar")
```

#### `setPulseTrigger(micros, eventname, eventkind)`
Adds recurring events in very short intervals.

Parameter | Description
-|-
micros    | Microsecond interval between events
eventname | Event name for the triggered events
eventkind | Event kind for the triggered events

Example:
```
setPulseTrigger(100, "foo", "bar")
```

Logging Functions
--
ECAL has a build-in logging system and provides by default the functions `debug`, `log` and `error` to log messages.

Stdlib Functions
--
It is possible to extend the ECAL interpreter with additional functions and constants. This can either be done through:

- Using [Go plugins](https://golang.org/pkg/plugin/) which allow the loading of code via dynamic linking. See ECAL's [documentation](https://devt.de/krotik/ecal#using-go-plugins-in-ecal).
- Using a bridge to Go. Standard Go functions using primitive argument types can be easily added using [code generation](https://blog.golang.org/generate). See the ECAL's [stdlib/generate](https://devt.de/krotik/ecal/src/master/stdlib/generate/generate.go#L51).
- Adding custom function definitions to the ECAL interpreter by extending the interpreter code. See ECAL's [stdlib package](https://pkg.go.dev/devt.de/krotik/ecal/stdlib).

For the first one you need to define a `.ecal.json` in the scripts folder. For the other options you need to recompile the ECAL interpreter code for EliasDB. Use the [replace directive](https://github.com/golang/go/wiki/Modules#when-should-i-use-the-replace-directive) in EliasDB's `go.mod` file to point the compiler to an alternative version of the ECAL interpreter which includes your changes.

Please see the [documentation](https://devt.de/krotik/ecal) of the ECAL interpreter for more information.
