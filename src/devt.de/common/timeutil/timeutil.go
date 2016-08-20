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
	"fmt"
	"strconv"
	"time"
)

/*
MakeTimestamp creates a timestamp string based on the systems epoch.
*/
func MakeTimestamp() string {
	return fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond))
}

/*
TimestampString prints a given timestamp as a human readable time.
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
