/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package timeutil

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

/*
func TestExampleCron(t *testing.T) {

	// For manual testing with real Cron object

	c := NewCron()

	c.Register("* * * * * *", func() {

		fmt.Println(time.Now().UTC())
	})

	c.Start() // Start cron thread

	time.Sleep(5 * time.Second)

	c.Stop() // Shutdown cron thread
}
*/

func TestCron(t *testing.T) {

	c := NewCron()

	// Check repeated running and stopping

	if c.stopChan != nil {
		t.Error("Unexpected cron state")
		return
	}

	c.Start()

	if c.stopChan == nil {
		t.Error("Unexpected cron state")
		return
	}

	c.Stop()

	if c.stopChan != nil {
		t.Error("Unexpected cron state")
		return
	}

	c.Start()

	if c.stopChan == nil {
		t.Error("Unexpected cron state")
		return
	}

	c.Stop()

	if c.stopChan != nil {
		t.Error("Unexpected cron state")
		return
	}

	// Test now with a cron testing object for a day

	var buf bytes.Buffer

	c = NewTestingCronDay()

	c.Register("0 12 12,8 * * *", func() {
		buf.WriteString(fmt.Sprintf("Test ... %v\n", c.NowFunc().UTC()))
	})

	c.Start()
	WaitTestingCron(c)

	if buf.String() != `
Test ... 2000-01-01 08:12:00 +0000 UTC
Test ... 2000-01-01 12:12:00 +0000 UTC
`[1:] {
		t.Error("Unexpected result:", buf.String())
		return
	}

	if NewTestingCronWeek() == nil {
		t.Error("Unexpected result")
		return
	}

	if NewTestingCronMonth() == nil {
		t.Error("Unexpected result")
		return
	}

	// Make sure it is impossible to create a never ending TestingNow object

	startTime, _ := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	endTime, _ := time.Parse(time.RFC3339, "2000-01-07T23:59:59Z")

	_, err := NewTestingNow(endTime, startTime)

	if err == nil || err.Error() != "End time 2000-01-01 00:00:00 +0000 UTC is not after start time 2000-01-07 23:59:59 +0000 UTC" {
		t.Error("Unexpected result:", err)
		return
	}
}

func TestCronSpec(t *testing.T) {

	// Do some table testing - spec vs. expected human readable string
	// vs. expected error message vs. expected different spec string (sorted)

	testDaysTable := [][]string{

		// Test * wildcard and numbers

		[]string{"* * * * * *", "every day", "", ""},
		[]string{"* * * 23 * *", "on 23rd of every month", "", ""},
		[]string{"* * * * 12 *", "in December", "", ""},
		[]string{"* * * * * 0", "on Sunday", "", ""},
		[]string{"* * * 12,19,31,23 * *", "on 12th, 19th, 23rd and 31st of every month", "", "* * * 12,19,23,31 * *"},
		[]string{"* * * * 12,11 *", "in November and December", "", "* * * * 11,12 *"},
		[]string{"* * * * * 4,5,6", "on Thursday, Friday and Saturday", "", ""},
		[]string{"* * * * 1,5,8 *", "in January, May and August", "", ""},
		[]string{"* * * 1,2,3,4,18 12 *", "on 1st, 2nd, 3rd, 4th and 18th of December", "", ""},
		[]string{"* * * 1,2,3,4,18 12 4,3", "on Wednesday and Thursday and 1st, 2nd, 3rd, 4th and 18th of December", "", "* * * 1,2,3,4,18 12 3,4"},
		[]string{"* * * 1,2 * 3,4", "on Wednesday and Thursday and 1st and 2nd of every month", "", ""},
		[]string{"* * * 4,5 1,2 *", "on 4th and 5th of January and February", "", ""},
		[]string{"* * * * 1,2 3,4", "on Wednesday and Thursday in January and February", "", ""},

		// Test multiple-of entries

		[]string{"* * * *%23,*%1 * *", "every day", "", "* * * * * *"},
		[]string{"* * * *%23,*%2 * *", "on every 2nd day and every 23rd day of every month", "", "* * * *%2,*%23 * *"},
		[]string{"* * * * *%2,*%12 *", "in every 2nd month and every 12th month", "", ""},
		[]string{"* * * * * *%2,*%7,3", "on every 2nd day of the week, every 7th day of the week and Wednesday", "", ""},

		// Test mix of normal and multiple-of entries

		[]string{"* * * 1,*%2,3,4,*%11,18 11,*%4,12 4,*%3,1", "on every 3rd day of the week, Monday and Thursday and every 2nd day, every 11th day, 1st, 3rd, 4th and 18th of every 4th month, November and December", "", "* * * *%2,*%11,1,3,4,18 *%4,11,12 *%3,1,4"},

		// Test errors

		[]string{"* * * * *", "", "Cron spec must have 6 entries separated by space", ""},
		[]string{"* * * * * x", "", "Cron entries must be a number, '*' or *% and a number", ""},
	}

	for i, row := range testDaysTable {
		res, err := checkDaysSpec(row[0], row[3])

		if err != nil {
			if row[2] == "" || row[2] != err.Error() {
				t.Errorf("Unexpected error for row %v %v: %v", (i + 1), row, err)
				return
			}
		} else if res != row[1] {
			t.Errorf("Unexpected result for row %v %v\nexpected: %v\ngot: %v",
				(i + 1), row, row[1], res)
			return
		}
	}

	testTimeTable := [][]string{

		// Test * wildcard and numbers

		[]string{"* * * * * *", "every second", "", ""},
		[]string{"55 * * * * *", "at second 55 of every minute", "", ""},
		[]string{"* 0 * * * *", "every second of the beginning of every hour", "", ""},
		[]string{"* * 12 * * *", "every second of hour 12:00", "", ""},
		[]string{"0 * * * * *", "at the beginning of every minute", "", ""},
		[]string{"0,59 * * * * *", "at the beginning and the end of every minute", "", ""},
		[]string{"* 0,59 * * * *", "every second of the beginning and the end of every hour", "", ""},
		[]string{"1,3,55 * * * * *", "at second 1, second 3 and second 55 of every minute", "", ""},
		[]string{"0 0,59,30 * * * *", "at the beginning, minute 30 and the end of every hour", "", "0 0,30,59 * * * *"},
		[]string{"* * 0,12,23 * * *", "every second of hour 00:00, hour 12:00 and hour 23:00", "", ""},
		[]string{"0 * 0,12,23 * * *", "every minute of hour 00:00, hour 12:00 and hour 23:00", "", ""},
		[]string{"0 1 0,12,23 * * *", "at minute 1 of hour 00:00, hour 12:00 and hour 23:00", "", ""},
		[]string{"22,33 * 4 * * *", "at second 22 and second 33 of every minute of hour 04:00", "", ""},
		[]string{"22,33 8 4 * * *", "at second 22 and second 33 of minute 8 of hour 04:00", "", ""},

		[]string{"22,33 8 * * * *", "at second 22 and second 33 of minute 8 of every hour", "", ""},
		[]string{"* 8,9,10 2,3 * * *", "every second of minute 8, minute 9 and minute 10 of hour 02:00 and hour 03:00", "", ""},

		// Test multiple-of entries

		[]string{"*%20,*%21 * * * * *", "at every 20th second and every 21st second of every minute", "", ""},
		[]string{"* *%2,*%55 * * * *", "every second of every 2nd minute and every 55th minute of every hour", "", ""},
		[]string{"* * *%13,*%14 * * *", "every second of every 13th hour and every 14th hour", "", ""},

		// Test mix of normal and multiple-of entries

		[]string{"*%2,*%11,1,3,4,18 *%4,11,12 *%3,1,4 * * *", "at every 2nd second, every 11th second, second 1, second 3, second 4 and second 18 of every 4th minute, minute 11 and minute 12 of every 3rd hour, hour 01:00 and hour 04:00", "", ""},

		// Test errors

		[]string{"* 60 * * * *", "", "Cron minute entry must be smaller or equal than 59", ""},
		[]string{"* * 2,5,25 * * *", "", "Cron hour entry must be smaller or equal than 23", ""},
		[]string{"* * * 0 * *", "", "Cron day of month entry must be greater or equal than 1", ""},
		[]string{"* * * *%a * *", "", "Cron day of month entry needs a number after '*%'", ""},
		[]string{"* * * *%0 * *", "", "Cron day of month entry must be greater or equal than 1", ""},
		[]string{"* * * * * x", "", "Cron entries must be a number, '*' or *% and a number", ""},
	}

	for i, row := range testTimeTable {
		res, err := checkTimeSpec(row[0], row[3])

		if err != nil {

			if row[2] == "" || row[2] != err.Error() {
				t.Errorf("Unexpected error for row %v %v: %v", (i + 1), row, err)
				return
			}

		} else if res != row[1] {
			t.Errorf("Unexpected result for row %v %v\nexpected: %v\ngot: %v",
				(i + 1), row, row[1], res)
			return
		}
	}

	// Test combination

	if ss, err := NewCronSpec("* * * * * *"); ss.String() != "every second every day" {
		t.Error("Unexpected string:", ss, err)
		return
	}

	if ss, err := NewCronSpec("1 2 3 4 5 6"); ss.String() != "at second 1 of minute 2 of hour 03:00 on Saturday and 4th of May" {
		t.Error("Unexpected string:", ss, err)
		return
	}

	ss, err := NewCronSpec("1,2 3,4 5,6 7,8 1 6")

	if err != nil || ss.String() != "at second 1 and second 2 of minute 3 and minute 4 of hour 05:00 and hour 06:00 on Saturday and 7th and 8th of January" {
		t.Error("Unexpected string:", ss, err)
		return
	}

	if res := strings.Join(ss.Generate2000Examples(3), "\n"); res != `
2000-01-08 05:03:01 +0000 UTC
2000-01-08 05:03:02 +0000 UTC
2000-01-08 05:04:01 +0000 UTC`[1:] {
		t.Error("Unexpected result:", res)
		return
	}

	ss, err = NewCronSpec("* * * * * *")

	if err != nil || ss.String() != "every second every day" {
		t.Error("Unexpected string:", ss, err)
		return
	}

	if res := strings.Join(ss.Generate2000Examples(3), "\n"); res != `
2000-01-01 00:00:00 +0000 UTC
2000-01-01 00:00:01 +0000 UTC
2000-01-01 00:00:02 +0000 UTC`[1:] {
		t.Error("Unexpected result:", res)
		return
	}
	ss, err = NewCronSpec("0 0 12 *%2 *%2 *")

	if err != nil || ss.String() != "at the beginning of hour 12:00 on every 2nd day of every 2nd month" {
		t.Error("Unexpected string:", ss, err)
		return
	}

	if res := strings.Join(ss.Generate2000Examples(3), "\n"); res != `
2000-02-02 12:00:00 +0000 UTC
2000-02-04 12:00:00 +0000 UTC
2000-02-06 12:00:00 +0000 UTC`[1:] {
		t.Error("Unexpected result:", res)
		return
	}
}

func checkDaysSpec(spec, newspec string) (string, error) {
	cs, err := NewCronSpec(spec)
	if err != nil {
		return "", err
	}

	if newspec == "" {
		if spec != cs.SpecString() {
			return "", fmt.Errorf("Spec string is not expected: %v != %v",
				spec, cs.SpecString())
		}
	} else {
		if newspec != cs.SpecString() {
			return "", fmt.Errorf("New spec string is not expected: %v != %v",
				newspec, cs.SpecString())
		}
	}

	return cs.DaysString(), nil
}

func checkTimeSpec(spec, newspec string) (string, error) {
	cs, err := NewCronSpec(spec)
	if err != nil {
		return "", err
	}

	if newspec == "" {
		if spec != cs.SpecString() {
			return "", fmt.Errorf("Spec string is not expected: %v != %v",
				spec, cs.SpecString())
		}
	} else {
		if newspec != cs.SpecString() {
			return "", fmt.Errorf("New spec string is not expected: %v != %v",
				newspec, cs.SpecString())
		}
	}

	return cs.TimeString(), nil
}
