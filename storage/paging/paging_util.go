/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package paging

/*
CountPages counts the number of pages of a certain type of a given PagedStorageFile.
*/
func CountPages(pager *PagedStorageFile, pagetype int16) (int, error) {

	var err error

	cursor := NewPageCursor(pager, pagetype, 0)

	page, _ := cursor.Next()
	counter := 0

	for page != 0 {
		counter++

		page, err = cursor.Next()
		if err != nil {
			return -1, err
		}
	}

	return counter, nil
}
