/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package logutil contains a simple leveled logging infrastructure supporting
different log levels, package scopes, formatters and handlers.

The main object is the Logger object which requires a scope. Use
GetLogger(scope string) to get an instance. Log messages are published
by various log methods (e.g. Info).

The logger object is also used to add sinks which consume log messages.
Each sinks requires a formatter which formats / decorades incoming log
messages. Log messages are handled by the most specific scoped sinks which
allow the message level.

Example:

	logger = GetLogger("foo.bar")

	logger.AddLogSink(Info, SimpleFormatter(), myLogFile)

	logger.Info("A log message")
*/
package logutil

import (
	"fmt"
	"io"
	"log"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
)

/*
fallbackLogger is used if there are error during regular logging
*/
var fallbackLogger = log.Print

/*
Level represents a logging level
*/
type Level string

/*
Log levels
*/
const (
	Debug   Level = "Debug"
	Info          = "Info"
	Warning       = "Warning"
	Error         = "Error"
)

/*
LogLevelPriority is a map assigning priorities to log level (lower number means a higher priority)
*/
var logLevelPriority = map[Level]int{
	Debug:   1,
	Info:    2,
	Warning: 3,
	Error:   4,
}

/*
stringToLoglevel is a map assigning log levels to strings.
*/
var stringToLoglevel = map[string]Level{
	strings.ToLower(fmt.Sprint(Debug)):   Debug,
	strings.ToLower(fmt.Sprint(Info)):    Info,
	strings.ToLower(fmt.Sprint(Warning)): Warning,
	strings.ToLower(fmt.Sprint(Error)):   Error,
}

/*
StringToLoglevel tries to turn a given string into a log level.
*/
func StringToLoglevel(loglevelString string) Level {
	level, _ := stringToLoglevel[strings.ToLower(loglevelString)]
	return level
}

/*
Logger is the main logging object which is used to add sinks and publish
log messages. A log messages is only handled by the most appropriate sink
in terms of level and scope. Multiple sinks can be registered for the same
level and scope.
*/
type Logger interface {

	/*
	   AddLogSink adds a log sink to a logger. A log sink can be a file or console
	   which satisfies the io.Writer interface.
	*/
	AddLogSink(loglevel Level, formatter Formatter, appender io.Writer)

	/*
		Debug logs a message at debug level.
	*/
	Debug(msg ...interface{})

	/*
		Info logs a message at info level.
	*/
	Info(msg ...interface{})

	/*
		Warning logs a message at warning level.
	*/
	Warning(msg ...interface{})

	/*
		Error logs a message at error level.
	*/
	Error(msg ...interface{})

	/*
		Error logs a message at error level and a stacktrace.
	*/
	LogStackTrace(loglevel Level, msg ...interface{})
}

/*
GetLogger returns a logger of a certain scope. Use the empty string '' for the
root scope.
*/
func GetLogger(scope string) Logger {
	return &logger{scope}
}

/*
ClearLogSinks removes all configured log sinks.
*/
func ClearLogSinks() {
	logSinksLock.Lock()
	defer logSinksLock.Unlock()

	logSinks = make([][]*logSink, 0)
}

/*
logger is the  main Logger interface implementation.
*/
type logger struct {
	scope string
}

/*
AddLogSink adds a log sink to a logger. A log sink can be a file or console
which satisfies the io.Writer interface.
*/
func (l *logger) AddLogSink(loglevel Level, formatter Formatter, appender io.Writer) {
	addLogSink(loglevel, l.scope, formatter, appender)
}

/*
Debug logs a message at debug level.
*/
func (l *logger) Debug(msg ...interface{}) {
	publishLog(Debug, l.scope, msg...)
}

/*
Info logs a message at info level.
*/
func (l *logger) Info(msg ...interface{}) {
	publishLog(Info, l.scope, msg...)
}

/*
Warning logs a message at warning level.
*/
func (l *logger) Warning(msg ...interface{}) {
	publishLog(Warning, l.scope, msg...)
}

/*
Error logs a message at error level.
*/
func (l *logger) Error(msg ...interface{}) {
	publishLog(Error, l.scope, msg...)
}

/*
Error logs a message at error level and a stacktrace.
*/
func (l *logger) LogStackTrace(loglevel Level, msg ...interface{}) {
	msg = append(msg, fmt.Sprintln())
	msg = append(msg, string(debug.Stack()))
	publishLog(loglevel, l.scope, msg...)
}

// Singleton logger
// ================

/*
logSink models a single log sink.
*/
type logSink struct {
	io.Writer
	level     Level
	scope     string
	formatter Formatter
}

/*
Implementation of sort interface for logSinks
*/
type sinkSlice [][]*logSink

func (p sinkSlice) Len() int           { return len(p) }
func (p sinkSlice) Less(i, j int) bool { return p[i][0].scope > p[j][0].scope }
func (p sinkSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

/*
logSinks contains all registered log sinks.
*/
var logSinks = make([][]*logSink, 0)
var logSinksLock = sync.RWMutex{}

/*
addLogSink adds a new logging sink.
*/
func addLogSink(level Level, scope string, formatter Formatter, sink io.Writer) {
	logSinksLock.Lock()
	defer logSinksLock.Unlock()

	// First see if the new sink can be appended to an existing list

	for i, scopeSinks := range logSinks {
		if scopeSinks[0].scope == scope {
			scopeSinks = append(scopeSinks, &logSink{sink, level, scope, formatter})
			logSinks[i] = scopeSinks
			return
		}
	}

	// Insert the new sink in the appropriate place

	logSinks = append(logSinks, []*logSink{&logSink{sink, level, scope, formatter}})
	sort.Sort(sinkSlice(logSinks))
}

/*
publishLog publishes a log message.
*/
func publishLog(loglevel Level, scope string, msg ...interface{}) {

	// Go through the sorted list of sinks

	for _, sinks := range logSinks {

		// Check if the log scope is within the message scope

		if strings.HasPrefix(scope, sinks[0].scope) {

			handled := false

			for _, sink := range sinks {

				// Check if the level is ok

				if logLevelPriority[sink.level] <= logLevelPriority[loglevel] {

					handled = true

					fmsg := sink.formatter.Format(loglevel, scope, msg...)

					if _, err := sink.Write([]byte(fmsg)); err != nil {

						// Something went wrong use the fallback logger

						fallbackLogger(fmt.Sprintf(
							"Cloud not publish log message: %v (message: %v)",
							err, fmsg))
					}
				}
			}

			if handled {
				return
			}
		}
	}

	// No handler for log message use the fallback logger

	fmsg := SimpleFormatter().Format(loglevel, scope, msg...)

	fallbackLogger(fmt.Sprintf("No log handler for log message: %v", fmsg))
}
