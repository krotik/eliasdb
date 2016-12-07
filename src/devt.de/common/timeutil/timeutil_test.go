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
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTimestamp(t *testing.T) {

	ts := MakeTimestamp()

	millis, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return
	}
	tsTime := time.Unix(0, millis*1000000)
	tsTime = tsTime.UTC()

	tss, err := TimestampString(ts, "UTC")

	if err != nil {
		t.Error("Unexpected error during timestamp printing:", err)
		return
	} else if tsTime.String() != tss {
		t.Error("Unexpected timestamp printing result:", tss)
		return
	}

	_, err = TimestampString("abc", "UTC")

	if err.Error() != "strconv.ParseInt: parsing \"abc\": invalid syntax" {
		t.Error("Unexpected error during timestamp printing:", err)
		return
	}

	_, err = TimestampString(ts, "U_B_C")

	if !strings.HasPrefix(err.Error(), "cannot find U_B_C") {
		t.Error("Unexpected error during timestamp printing:", err)
		return
	}

	// Test compare

	ts = MakeTimestamp()

	if res, err := CompareTimestamp("1475602478271", "1475615168232"); res != 1 || err != nil {
		t.Error("Unexpected compare result:", res, err)
		return
	}

	if res, err := CompareTimestamp("1475602478271", "1375615168232"); res != -1 || err != nil {
		t.Error("Unexpected compare result:", res, err)
		return
	}

	if _, err := CompareTimestamp("1475602478271", ""); err == nil {
		t.Error("Unexpected compare result:", err)
		return
	}

	if _, err := CompareTimestamp("", "1"); err == nil {
		t.Error("Unexpected compare result:", err)
		return
	}

	if res, err := CompareTimestamp("1", "1"); res != 0 || err != nil {
		t.Error("Unexpected compare result:", res, err)
		return
	}
}
