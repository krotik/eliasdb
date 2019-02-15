/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package timeutil contains common function for time related operations.
*/
package timeutil

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"devt.de/common/errorutil"
)

/*
Cron is an object which implements cron-like functionality. It can be
used to schedule jobs at certain time intervals based on local time.

Each Cron object runs a single scheduling thread. Very time consuming
tasks which are triggered by this object are expected to run in a
separate thread otherwise other registered tasks might not be
triggered on time.

Time can be speed up for testing purposes by changing the NowFunc and Tick
properties. See NewTestingCron for details.

Example code:

	c := NewCron()

	c.Register("0 0 12 1 * *", func() {

		// Do something at the beginning of hour 12:00 on 1st of every month
	})

	c.Start() // Start cron thread

	...

	c.Stop() // Shutdown cron thread
*/
type Cron struct {
	newNowFunc func() time.Time     // Function to get the current local time
	NowFunc    func() time.Time     // Function to get the current local time
	Tick       time.Duration        // Cron check interval
	cronLock   *sync.Mutex          // Lock for data operations
	handlerMap map[string][]func()  // Map of spec to handler functions
	specMap    map[string]*CronSpec // Map of spec to spec object
	stopChan   chan bool            // Channel for the kill command
}

/*
NewCron creates a new Cron object.
*/
func NewCron() *Cron {
	return &Cron{
		time.Now,
		time.Now,
		time.Second * 1, // Cron check interval is a second by default
		&sync.Mutex{},
		make(map[string][]func()),
		make(map[string]*CronSpec),
		nil,
	}
}

/*
run is the actual cron thread.
*/
func (c *Cron) run() {
	t := c.NowFunc()

Mainloop:
	for {

		select {
		case <-c.stopChan:
			break Mainloop
		case <-time.After(c.Tick):
			break
		}

		c.cronLock.Lock()

		for specString, cs := range c.specMap {

			if cs.MatchesTime(t) {

				// Execute all handlers if the current time matches a cron spec

				for _, handler := range c.handlerMap[specString] {
					handler()
				}
			}
		}

		c.cronLock.Unlock()

		if t = c.newNowFunc(); t == TestingEndOfTime {

			// We are a testing cron and stop after the given testing time range
			// has passed

			c.cronLock.Lock()

			close(c.stopChan)
			c.stopChan = nil

			c.cronLock.Unlock()

			return
		}
	}

	c.stopChan <- true
}

/*
Start starts the cron thread. Start is a NOP if cron is already running.
*/
func (c *Cron) Start() {
	c.cronLock.Lock()
	defer c.cronLock.Unlock()

	if c.stopChan == nil {
		c.stopChan = make(chan bool)
		go c.run()
	}
}

/*
Stop stops the cron thread. Stop is a NOP if cron is not running.
*/
func (c *Cron) Stop() {
	c.cronLock.Lock()
	defer c.cronLock.Unlock()

	if c.stopChan != nil {

		// Do the closing handshake

		c.stopChan <- true
		<-c.stopChan

		// Dispose of the stop channel

		close(c.stopChan)
		c.stopChan = nil
	}
}

/*
Register registers a new handler to be called every interval, defined
by the given spec.
*/
func (c *Cron) Register(specString string, handler func()) error {
	spec, err := NewCronSpec(specString)

	if err == nil {
		c.RegisterSpec(spec, handler)
	}

	return err
}

/*
RegisterSpec registers a new handler to be called every interval, defined
by the given spec.
*/
func (c *Cron) RegisterSpec(spec *CronSpec, handler func()) {
	specString := spec.SpecString()

	c.cronLock.Lock()
	defer c.cronLock.Unlock()

	if _, ok := c.specMap[spec.SpecString()]; !ok {
		c.specMap[specString] = spec
	}

	handlers, _ := c.handlerMap[specString]
	c.handlerMap[specString] = append(handlers, handler)
}

/*
CronSpec is a data structure which is used to specify time schedules.
A CronSpec can be stated as a single text string which must have the
following 6 entries separated by whitespace:

	Field	        Valid values
	-----	        ------------
	second         * or 0-59 or *%1-59
	minute         * or 0-59 or *%1-59
	hour           * or 0-23 or *%1-23
	day of month   * or 1-31 or *%1-31
	month          * or 1-12 or *%1-12
	day of week    * or 0-6 (0 is Sunday) or *%1-7

Multiple values for an entry can be separated by commas e.g. 1,3,5,7.
A * in any field matches all values i.e. execute every minute, every
day, etc. A *%<number> in any field entry matches when the time is a
multiple of <number>.

Example code:

	ss, _ := NewCronSpec("0 0 12 1 * *")
	fmt.Println(ss.String())

Output:

	at the beginning of hour 12:00 on 1st of every month
*/
type CronSpec struct {
	Second     []string
	Minute     []string
	Hour       []string
	DayOfMonth []string
	Month      []string
	DayOfWeek  []string
}

/*
NewCronSpec creates a new CronSpec from a given spec string.
*/
func NewCronSpec(spec string) (*CronSpec, error) {
	sspec := strings.Split(spec, " ")

	if len(sspec) != 6 {
		return nil, fmt.Errorf("Cron spec must have 6 entries separated by space")
	}

	checkNumberRange := func(num, min, max int) string {
		if num < min {
			return fmt.Sprintf("must be greater or equal than %v", min)
		} else if num > max {
			return fmt.Sprintf("must be smaller or equal than %v", max)
		}
		return ""
	}

	entries := make([][]string, 6)

	for i, entry := range sspec {
		field := fields[i]
		vals := strings.Split(entry, ",")

	valsLoop:
		for _, val := range vals {

			// Auto convert pointless things like *%1 -> *

			if val == "*%1" || (val == "*" && len(vals) > 1) {
				val = "*"
				vals = []string{val}
				break valsLoop
			}

			if strings.HasPrefix(val, "*%") {
				var res string

				// Deal with multiple-of entries

				num, err := strconv.Atoi(val[2:])

				if err != nil {
					return nil, fmt.Errorf("Cron %v entry needs a number after '*%%'", field)
				}

				// Check number range

				switch i {
				case 0: // Second
					res = checkNumberRange(num, 1, 59)
				case 1: // Minute
					res = checkNumberRange(num, 1, 59)
				case 2: // Hour
					res = checkNumberRange(num, 1, 23)
				case 3: // Day of month
					res = checkNumberRange(num, 1, 31)
				case 4: // Month
					res = checkNumberRange(num, 1, 12)
				case 5: // Day of week
					res = checkNumberRange(num, 1, 7)
				}

				if res != "" {
					return nil, fmt.Errorf("Cron %v entry %v", field, res)
				}

			} else if val != "*" {
				var res string

				num, err := strconv.Atoi(val)

				if err != nil {
					return nil, fmt.Errorf("Cron entries must be a number, '*' or *%% and a number")
				}

				// Check number range

				switch i {
				case 0: // Second
					res = checkNumberRange(num, 0, 59)
				case 1: // Minute
					res = checkNumberRange(num, 0, 59)
				case 2: // Hour
					res = checkNumberRange(num, 0, 23)
				case 3: // Day of month
					res = checkNumberRange(num, 1, 31)
				case 4: // Month
					res = checkNumberRange(num, 1, 12)
				case 5: // Day of week
					res = checkNumberRange(num, 0, 6)
				}

				if res != "" {
					return nil, fmt.Errorf("Cron %v entry %v", field, res)
				}
			}
		}

		entries[i] = vals
	}

	ret := &CronSpec{entries[0], entries[1], entries[2], entries[3],
		entries[4], entries[5]}

	// Sort all entries

	sort.Sort(numberStringSlice(ret.Second))
	sort.Sort(numberStringSlice(ret.Minute))
	sort.Sort(numberStringSlice(ret.Hour))
	sort.Sort(numberStringSlice(ret.DayOfMonth))
	sort.Sort(numberStringSlice(ret.Month))
	sort.Sort(numberStringSlice(ret.DayOfWeek))

	return ret, nil
}

/*
MatchesTime checks if a given time object matches this CronSpec.
*/
func (cs *CronSpec) MatchesTime(t time.Time) bool {

	matchItem := func(timeItem int, specItems []string) bool {

		for _, specItem := range specItems {

			if specItem == "*" {
				return true
			}

			if strings.HasPrefix(specItem, "*%") {
				interval, err := strconv.Atoi(specItem[2:])

				if err == nil && interval != 0 && timeItem%interval == 0 {
					return true
				}
			} else {
				item, err := strconv.Atoi(specItem)

				if err == nil && item == timeItem {
					return true
				}
			}
		}

		return false
	}

	return matchItem(t.Second(), cs.Second) &&
		matchItem(t.Minute(), cs.Minute) &&
		matchItem(t.Hour(), cs.Hour) &&
		matchItem(t.Day(), cs.DayOfMonth) &&
		matchItem(int(t.Month()), cs.Month) &&
		matchItem(int(t.Weekday()), cs.DayOfWeek)
}

/*
Generate2000Examples generates matching time examples from the year 2000
for this CronSpec. This function returns the first n examples starting
from 01. January 2000 00:00:00.
*/
func (cs *CronSpec) Generate2000Examples(n int) []string {
	var t time.Time

	res := make([]string, 0, n)

	// Create reference time boundaries for the year 2000

	startTime, err := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	errorutil.AssertOk(err)
	startNano := startTime.UnixNano()

	// Loop over all seconds of the year (2000 had 366 days)

	timerange := int64(60 * 60 * 24 * 366)

	for i := int64(0); i < timerange; i++ {

		if t = time.Unix(i, startNano).In(time.UTC); cs.MatchesTime(t) {
			res = append(res, t.String())
		}

		if len(res) > n-1 {
			break
		}
	}

	return res
}

/*
SpecString returns the spec object as a spec string. This string can
be used to construct the object again using NewCronSpec.
*/
func (cs *CronSpec) SpecString() string {
	var res bytes.Buffer

	res.WriteString(strings.Join(cs.Second, ","))
	res.WriteString(" ")
	res.WriteString(strings.Join(cs.Minute, ","))
	res.WriteString(" ")
	res.WriteString(strings.Join(cs.Hour, ","))
	res.WriteString(" ")
	res.WriteString(strings.Join(cs.DayOfMonth, ","))
	res.WriteString(" ")
	res.WriteString(strings.Join(cs.Month, ","))
	res.WriteString(" ")
	res.WriteString(strings.Join(cs.DayOfWeek, ","))

	return res.String()
}

/*
Constants for pretty printing
*/
var (
	fields = []string{"second", "minute", "hour", "day of month",
		"month", "day of week"}

	days = []string{"Sunday", "Monday", "Tuesday",
		"Wednesday", "Thursday", "Friday", "Saturday"}

	months = []string{"January", "February", "March", "April",
		"May", "June", "July", "August", "September", "October",
		"November", "December"}
)

/*
TimeString returns on which time during the day this spec will trigger.
*/
func (cs *CronSpec) TimeString() string {
	secondString := func() []string {
		ret := make([]string, len(cs.Second))
		for i, s := range cs.Second {
			if strings.HasPrefix(s, "*%") {
				ret[i] = fmt.Sprintf("every %v second",
					ordinalNumberString(s[2:]))
			} else {

				if s == "0" {
					ret[i] = "the beginning"
				} else if s == "59" {
					ret[i] = "the end"
				} else {
					ret[i] = fmt.Sprintf("second %v", s)
				}
			}
		}
		return ret
	}

	minuteString := func() []string {
		ret := make([]string, len(cs.Minute))
		for i, m := range cs.Minute {
			if strings.HasPrefix(m, "*%") {
				ret[i] = fmt.Sprintf("every %v minute",
					ordinalNumberString(m[2:]))
			} else {
				if m == "0" {
					ret[i] = "the beginning"
				} else if m == "59" {
					ret[i] = "the end"
				} else {
					ret[i] = fmt.Sprintf("minute %v", m)
				}
			}
		}
		return ret
	}

	hourString := func() []string {
		ret := make([]string, len(cs.Hour))
		for i, h := range cs.Hour {
			if strings.HasPrefix(h, "*%") {
				ret[i] = fmt.Sprintf("every %v hour",
					ordinalNumberString(h[2:]))
			} else {
				hour, _ := strconv.Atoi(h)
				ret[i] = fmt.Sprintf("hour %02d:00", hour)
			}
		}
		return ret
	}

	if len(cs.Hour) == 1 && cs.Hour[0] == "*" {
		if len(cs.Minute) == 1 && cs.Minute[0] == "*" {
			if len(cs.Second) == 1 && cs.Second[0] == "*" {
				return "every second"
			}

			// Specs of the format [Seconds] * * ? ? ?

			return fmt.Sprintf("at %v of every minute", andJoin(secondString()))
		}

		if len(cs.Second) == 1 {

			if cs.Second[0] == "*" {

				// Specs of the format * [Minutes] * ? ? ?

				return fmt.Sprintf("every second of %v of every hour", andJoin(minuteString()))

			} else if cs.Second[0] == "0" {

				// Specs of the format 0 [Minutes] * ? ? ?

				return fmt.Sprintf("at %v of every hour", andJoin(minuteString()))
			}
		}

		// Specs of the format [Seconds] [Minutes] * ? ? ?

		return fmt.Sprintf("at %v of %v of every hour",
			andJoin(secondString()), andJoin(minuteString()))
	}

	if len(cs.Minute) == 1 && cs.Minute[0] == "*" {

		if len(cs.Second) == 1 {

			if cs.Second[0] == "*" {

				// Specs of the format * * [Hours] ? ? ?

				return fmt.Sprintf("every second of %v", andJoin(hourString()))

			} else if cs.Second[0] == "0" {

				// Specs of the format 0 * [Hours] ? ? ?

				return fmt.Sprintf("every minute of %v", andJoin(hourString()))
			}
		}

		// Specs of the format [Seconds] * [Hours] ? ? ?

		return fmt.Sprintf("at %v of every minute of %v",
			andJoin(secondString()), andJoin(hourString()))
	}

	if len(cs.Second) == 1 {

		if cs.Second[0] == "*" {

			// Specs of the format * [Minutes] [Hours] ? ? ?

			return fmt.Sprintf("every second of %v of %v", andJoin(minuteString()),
				andJoin(hourString()))

		} else if cs.Second[0] == "0" {

			// Specs of the format 0 [Minutes] [Hours] ? ? ?

			return fmt.Sprintf("at %v of %v", andJoin(minuteString()),
				andJoin(hourString()))
		}
	}

	// Specs of the format [Seconds] [Minutes] [Hours] ? ? ?

	return fmt.Sprintf("at %v of %v of %v",
		andJoin(secondString()), andJoin(minuteString()),
		andJoin(hourString()))
}

/*
DaysString returns on which days this spec will trigger.
*/
func (cs *CronSpec) DaysString() string {

	dayOfWeekToString := func() []string {
		ret := make([]string, len(cs.DayOfWeek))
		for i, dow := range cs.DayOfWeek {
			if strings.HasPrefix(dow, "*%") {
				ret[i] = fmt.Sprintf("every %v day of the week",
					ordinalNumberString(dow[2:]))
			} else {
				idx, _ := strconv.Atoi(dow)
				ret[i] = days[idx]
			}
		}
		return ret
	}

	dayOfMonthString := func() []string {
		ret := make([]string, len(cs.DayOfMonth))
		for i, dom := range cs.DayOfMonth {
			if strings.HasPrefix(dom, "*%") {

				// No need to write here "of the month" since a cron spec
				// addresses months specifically i.e. the month(s) will always
				// be included in every DaysString.

				ret[i] = fmt.Sprintf("every %v day",
					ordinalNumberString(dom[2:]))
			} else {
				ret[i] = ordinalNumberString(dom)
			}
		}
		return ret
	}

	monthString := func() []string {
		ret := make([]string, len(cs.Month))
		for i, m := range cs.Month {
			if strings.HasPrefix(m, "*%") {
				ret[i] = fmt.Sprintf("every %v month",
					ordinalNumberString(m[2:]))
			} else {
				idx, _ := strconv.Atoi(m)
				ret[i] = months[idx-1]
			}
		}
		return ret
	}

	if len(cs.Month) == 1 && cs.Month[0] == "*" {

		if len(cs.DayOfMonth) == 1 && cs.DayOfMonth[0] == "*" {

			if len(cs.DayOfWeek) == 1 && cs.DayOfWeek[0] == "*" {
				return "every day"
			}

			// Specs of the format ? ? ? * * [Days of week]

			return fmt.Sprintf("on %v", andJoin(dayOfWeekToString()))
		}

		if len(cs.DayOfWeek) == 1 && cs.DayOfWeek[0] == "*" {

			// Specs of the format ? ? ? [Days of month] * *

			return fmt.Sprintf("on %v of every month", andJoin(dayOfMonthString()))
		}

		// Specs of the format ? ? ? [Days of month] * [Days of week]

		return fmt.Sprintf("on %v and %v of every month",
			andJoin(dayOfWeekToString()), andJoin(dayOfMonthString()))
	}

	if len(cs.DayOfMonth) == 1 && cs.DayOfMonth[0] == "*" {

		if len(cs.DayOfWeek) == 1 && cs.DayOfWeek[0] == "*" {
			return fmt.Sprintf("in %v", andJoin(monthString()))
		}

		// Specs of the format ? ? ? * [Months] [Days of week]

		return fmt.Sprintf("on %v in %v", andJoin(dayOfWeekToString()),
			andJoin(monthString()))
	}

	if len(cs.DayOfWeek) == 1 && cs.DayOfWeek[0] == "*" {

		// Specs of the format ? ? ? [Days of month] [Months] *

		return fmt.Sprintf("on %v of %v", andJoin(dayOfMonthString()),
			andJoin(monthString()))
	}

	// Specs of the format ? ? ? [Days of month] [Months] [Days of week]

	return fmt.Sprintf("on %v and %v of %v",
		andJoin(dayOfWeekToString()), andJoin(dayOfMonthString()),
		andJoin(monthString()))
}

/*
String returns a human readable string representing the spec.
*/
func (cs *CronSpec) String() string {
	return fmt.Sprintf("%v %v", cs.TimeString(), cs.DaysString())
}

// Testing functions
// =================

/*
NewTestingCronMonth creates a new test Cron object which goes through a full month.
*/
func NewTestingCronMonth() *Cron {

	startTime, _ := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	endTime, _ := time.Parse(time.RFC3339, "2000-01-31T23:59:59Z")

	return NewTestingCron(startTime, endTime)
}

/*
NewTestingCronWeek creates a new test Cron object which goes through a full week.
*/
func NewTestingCronWeek() *Cron {

	startTime, _ := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	endTime, _ := time.Parse(time.RFC3339, "2000-01-07T23:59:59Z")

	return NewTestingCron(startTime, endTime)
}

/*
NewTestingCronDay creates a new test Cron object which goes through a full day.
*/
func NewTestingCronDay() *Cron {

	startTime, _ := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	endTime, _ := time.Parse(time.RFC3339, "2000-01-01T23:59:59Z")

	return NewTestingCron(startTime, endTime)
}

/*
NewTestingCron creates a new Cron object which can be used for testing.
When started it goes through all seconds of the the given time range in
a fraction of the time (seconds are instantly increased). All handler functions
are still called the exact number of times as if they would be running
during the given time range in normal time. Use the NowFunc of the cron
object to get the current testing time - the testing time will always
return the same time unless advanced by the cron thread.

Example code:

	c = NewTestingCronDay()

	c.Register("0 12 12,8 * * *", func() {

		// Do something at minute 12 of hour 08:00 and hour 12:00 every day
	})

	c.Start()
	WaitTestingCron(c)
*/
func NewTestingCron(startTime, endTime time.Time) *Cron {

	tn, _ := NewTestingNow(startTime, endTime)

	ret := NewCron()

	ret.newNowFunc = tn.NewNow
	ret.NowFunc = tn.Now
	ret.Tick = 0 // A normal second is instantly increased

	return ret
}

/*
WaitTestingCron waits for a testing cron object to end. No need to call
stop afterwards.
*/
func WaitTestingCron(c *Cron) {
	<-c.stopChan
}

/*
TestingEndOfTime is a special time returned by TestingNow objects to indicate
that the end time has been reached.
*/
var TestingEndOfTime = time.Time{}

/*
TestingNow is a testing object which can provide a specialized Now function
which runs from a given start to a given end time.
*/
type TestingNow struct {
	start time.Time
	end   time.Time
	tick  int64
}

/*
NewTestingNow creates a new TestingNow object with a given start and end time.
*/
func NewTestingNow(start, end time.Time) (*TestingNow, error) {
	if !end.After(start) {
		return nil, fmt.Errorf("End time %v is not after start time %v", end, start)
	}
	return &TestingNow{start, end, 0}, nil
}

/*
Now returns the current testing time.
*/
func (tn *TestingNow) Now() time.Time {
	return tn.testingTime(false)
}

/*
NewNow returns the current testing time and advances the clock.
*/
func (tn *TestingNow) NewNow() time.Time {
	return tn.testingTime(true)
}

/*
testingTime returns the current testing time and optionally advances the clock.
*/
func (tn *TestingNow) testingTime(advance bool) time.Time {

	if advance {
		tn.tick++
	}

	if newTime := time.Unix(tn.tick, tn.start.UnixNano()); newTime.Before(tn.end) {
		return newTime.UTC()
	}

	return TestingEndOfTime
}

// Helper functions
// ================

func andJoin(ss []string) string {
	var buf bytes.Buffer

	sslen := len(ss) - 1

	for i, s := range ss {
		buf.WriteString(s)

		if i == sslen-1 {
			buf.WriteString(" and ")
		} else if i < sslen {
			buf.WriteString(", ")
		}
	}

	return buf.String()
}

/*
ordinalNumber produces an ordinal number string from a given number
string (e.g. 1 -> 1st, 2 -> 2nd).
*/
func ordinalNumberString(number string) string {
	var suffix = "th"

	if strings.HasSuffix(number, "1") && !strings.HasSuffix(number, "11") {
		suffix = "st"
	} else if strings.HasSuffix(number, "2") && !strings.HasSuffix(number, "12") {
		suffix = "nd"
	} else if strings.HasSuffix(number, "3") && !strings.HasSuffix(number, "13") {
		suffix = "rd"
	}

	return number + suffix
}

// Comparator for number strings

type numberStringSlice []string

func (p numberStringSlice) Len() int { return len(p) }
func (p numberStringSlice) Less(i, j int) bool {
	val1 := p[i]
	val2 := p[j]

	convert := func(val string) string {
		if strings.HasPrefix(val, "*%") {

			if num, err := strconv.Atoi(val[2:]); err == nil && num < 10 {
				return fmt.Sprintf("*%%0%v", num)
			}

		} else if val != "*" {

			if num, err := strconv.Atoi(val); err == nil && num < 10 {
				return fmt.Sprintf("0%v", num)
			}
		}

		return val
	}

	return convert(val1) < convert(val2)
}

func (p numberStringSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
