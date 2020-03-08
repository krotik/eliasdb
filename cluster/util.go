/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package cluster

import (
	"fmt"
	"strconv"

	"devt.de/krotik/common/errorutil"
)

/*
toUInt64 safely converts an interface{} to an uint64.
*/
func toUInt64(v interface{}) uint64 {
	if vu, ok := v.(uint64); ok {
		return vu
	}

	cloc, err := strconv.ParseInt(fmt.Sprint(v), 10, 64)
	errorutil.AssertOk(err)

	return uint64(cloc)
}
