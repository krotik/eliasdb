package logutil

import (
	"fmt"
	"strings"

	"devt.de/common/testutil"
	"devt.de/common/timeutil"
)

/*
Formatter is used to format log messages.
*/
type Formatter interface {

	/*
	   Format formats a given log message into a string.
	*/
	Format(level Level, scope string, msg ...interface{}) string
}

/*
ConsoleFormatter returns a simple formatter which does a simple fmt.Sprintln
on all log messages. It only adds the log level.
*/
func ConsoleFormatter() Formatter {
	return &consoleFormatter{}
}

/*
consoleFormatter is the console formatter implementation.
*/
type consoleFormatter struct {
}

/*
Format formats a given log message into a string.
*/
func (sf *consoleFormatter) Format(level Level, scope string, msg ...interface{}) string {
	return fmt.Sprintln(fmt.Sprintf("%v:", level), fmt.Sprint(msg...))
}

/*
SimpleFormatter returns a simple formatter which does a simple fmt.Sprintln
on all log messages. It also adds a current timestamp, the message scope and
log level.
*/
func SimpleFormatter() Formatter {
	return &simpleFormatter{timeutil.MakeTimestamp}
}

/*
simpleFormatter is the simple formatter implementation.
*/
type simpleFormatter struct {
	tsFunc func() string // Timestamp function
}

/*
Format formats a given log message into a string.
*/
func (sf *simpleFormatter) Format(level Level, scope string, msg ...interface{}) string {
	if scope == "" {
		return fmt.Sprintln(sf.tsFunc(), level, fmt.Sprint(msg...))
	}

	return fmt.Sprintln(sf.tsFunc(), level, scope, fmt.Sprint(msg...))
}

/*
TemplateFormatter returns a formatter which produces log messages according to
a given template string. The template string may contain one or more of the
following directives:

%s         The scope of the log message
%l         The level of the log message
%t         Current timestamp (milliseconds elapsed since January 1, 1970 UTC)
%f         Function in which the log message was issued e.g. foo.bar.MyFunc()
%c         Code location of the log statement which issuing the log message e.g. package/somefile.go:12
%m         The log message and its arguments formatted with fmt.Sprintf()
*/
func TemplateFormatter(template string) Formatter {
	return &templateFormatter{template, timeutil.MakeTimestamp}
}

/*
templateFormatter is the template formatter implementation.
*/
type templateFormatter struct {
	template string        // Template for a log message
	tsFunc   func() string // Timestamp function
}

/*
Format formats a given log message into a string.
*/
func (sf *templateFormatter) Format(level Level, scope string, msg ...interface{}) string {

	name, loc := testutil.GetCaller(2)

	out := sf.template

	out = strings.Replace(out, "%s", scope, -1)
	out = strings.Replace(out, "%l", fmt.Sprint(level), -1)
	out = strings.Replace(out, "%t", sf.tsFunc(), -1)
	out = strings.Replace(out, "%f", name, -1)
	out = strings.Replace(out, "%c", loc, -1)
	out = strings.Replace(out, "%m", fmt.Sprint(msg...), -1)

	return fmt.Sprintln(out)
}
