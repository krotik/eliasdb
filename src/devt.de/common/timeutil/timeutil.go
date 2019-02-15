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
	"fmt"
	"strconv"
	"time"
)

/*
MakeTimestamp creates a timestamp string based on the systems
epoch (January 1, 1970 UTC).
*/
func MakeTimestamp() string {
	return fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond))
}

/*
CompareTimestamp compares 2 given timestamps. Returns 0 if they are equal,
1 if the frist is older and -1 if the second is older.
*/
func CompareTimestamp(ts1, ts2 string) (int, error) {
	if ts1 == ts2 {
		return 0, nil
	}

	millis1, err := strconv.ParseInt(ts1, 10, 64)
	if err != nil {
		return 0, err
	}
	millis2, err := strconv.ParseInt(ts2, 10, 64)
	if err != nil {
		return 0, err
	}

	if millis1 < millis2 {
		return 1, nil
	}

	return -1, nil
}

/*
TimestampString prints a given timestamp as a human readable time in a given
Location (timezone).
*/
func TimestampString(ts, loc string) (string, error) {

	millis, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return "", err
	}

	tsTime := time.Unix(0, millis*1000000)

	l, err := time.LoadLocation(loc)
	if err != nil {
		return "", err
	}

	return tsTime.In(l).String(), nil
}
