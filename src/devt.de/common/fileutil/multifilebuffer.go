/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package fileutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"devt.de/common/timeutil"
)

/*
MultiFileBuffer is a file-persitent buffer which can be split over multiple files.

A specified file is opened and used as backend storage for a byte buffer. By
default, the file grows indefinitely. It is possible to specify a rollover
condition to allow the file to rollover once the condition is satisfied.
If the condition is satisfied, the file is closed and a new file is silently
opened for output. The buffer will save old log files by appending the
extensions ‘.1’, ‘.2’ etc., to the file name. The rollover condition is only
checked once at the beginning of a write operation.

For example, with a base file name of app.log, the buffer would create
app.log, app.log.1, app.log.2, etc. The file being written to is always app.log.
When this file is filled, it is closed and renamed to app.log.1, and if files
app.log.1, app.log.2, etc. exist, then they are renamed to app.log.2, app.log.3
etc. respectively.
*/
type MultiFileBuffer struct {
	lock     *sync.Mutex       // Lock for reading and writing
	filename string            // File name for buffer
	basename string            // Base file name (file name + iterator decoration)
	iterator FilenameIterator  // Iterator for file names
	cond     RolloverCondition // Rollover condition
	fp       *os.File          // Current file handle
}

/*
NewMultiFileBuffer creates a new MultiFileBuffer with a given file name
iterator and rollover condition.
*/
func NewMultiFileBuffer(filename string, it FilenameIterator, cond RolloverCondition) (*MultiFileBuffer, error) {
	var err error

	mfb := &MultiFileBuffer{&sync.Mutex{}, filename, it.Basename(filename), it, cond, nil}

	if err = mfb.checkrollover(); err != nil {
		return nil, err
	}

	if mfb.fp == nil {

		// File existed and can be continued

		mfb.lock.Lock()
		mfb.fp, err = os.OpenFile(mfb.basename, os.O_APPEND|os.O_RDWR, 0660)
		mfb.lock.Unlock()

	}

	return mfb, nil
}

/*
Write writes len(p) bytes from p to the underlying data stream. It returns
the number of bytes written from p (0 <= n <= len(p)) and any error
encountered that caused the write to stop early.
*/
func (mfb *MultiFileBuffer) Write(output []byte) (int, error) {
	var b int

	err := mfb.checkrollover()

	if err == nil {

		if mfb.fp == nil {

			// File existed and can be continued

			mfb.lock.Lock()
			mfb.fp, err = os.OpenFile(mfb.basename, os.O_APPEND|os.O_RDWR, 0660)
			mfb.lock.Unlock()

		}

		mfb.lock.Lock()
		b, err = mfb.fp.Write(output)
		mfb.lock.Unlock()
	}

	return b, err
}

/*
checkrollover checks if the buffer files should be switched.
*/
func (mfb *MultiFileBuffer) checkrollover() error {
	mfb.lock.Lock()
	defer mfb.lock.Unlock()

	//  Update basename here

	mfb.basename = mfb.iterator.Basename(mfb.filename)

	// Rollover if the base file does not exist

	ex, err := PathExists(mfb.basename)

	if err == nil && (!ex || mfb.cond.CheckRollover(mfb.basename)) {

		// Rollover if either the base file does not exist or the
		// rollover condition is satisfied

		err = mfb.rollover()
	}

	return err
}

/*
Close closes the buffer.
*/
func (mfb *MultiFileBuffer) Close() error {
	var err error

	if mfb.fp != nil {
		err = mfb.fp.Close()
		mfb.fp = nil
	}

	return err
}

/*
rollover switches the buffer files.
*/
func (mfb *MultiFileBuffer) rollover() error {
	var err error

	// Recursive file renaming function

	var ensureFileSlot func(fn string) error

	ensureFileSlot = func(fn string) error {

		// Check if the file exists already

		ex, err := PathExists(fn)

		if ex && err == nil {

			// Determine new file name

			newfn := mfb.iterator.NextName(fn)

			if newfn == "" {

				// If it is the end of the iteration just delete the file

				err = os.Remove(fn)

			} else {

				// Ensure the new file name is usable

				err = ensureFileSlot(newfn)

				// Rename file according to iterator.NextName()

				if err == nil {
					err = os.Rename(fn, newfn)
				}
			}
		}

		return err
	}

	// Close existing file

	err = mfb.Close()

	// Create file handle

	if err == nil {

		err = ensureFileSlot(mfb.basename)

		if err == nil {

			// Overwrite existing base file

			mfb.fp, err = os.OpenFile(mfb.basename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
		}
	}

	return err
}

// Rollover conditions
// ===================

/*
RolloverCondition is used by the MultiFileBuffer to check if the buffer files
should be switched.
*/
type RolloverCondition interface {

	/*
	   CheckRollover checks if the buffer files should be switched.
	*/
	CheckRollover(basename string) bool
}

/*
EmptyRolloverCondition creates a rollover condition which is never true.
*/
func EmptyRolloverCondition() RolloverCondition {
	return &emptyRolloverCondition{}
}

/*
emptyRolloverCondition is a rollover condition which is never true.
*/
type emptyRolloverCondition struct {
}

/*
NextName returns the next file name based on the current file name.
An empty string means the end of the iteration.
*/
func (rc *emptyRolloverCondition) CheckRollover(basename string) bool {
	return false
}

/*
SizeBasedRolloverCondition creates a new rollover condition based on file
size. The condition is satisfied if the base file exceeds a certain file size.
*/
func SizeBasedRolloverCondition(maxSize int64) RolloverCondition {
	return &sizeBasedRolloverCondition{maxSize}
}

/*
sizeBasedRolloverCondition is the implementation of the size based rollover
condition.
*/
type sizeBasedRolloverCondition struct {
	maxSize int64
}

/*
NextName returns the next file name based on the current file name.
An empty string means the end of the iteration.
*/
func (rc *sizeBasedRolloverCondition) CheckRollover(basename string) bool {
	ret := false

	if info, err := os.Stat(basename); err == nil {
		ret = info.Size() >= rc.maxSize
	}

	return ret
}

// FilenameIterator
// ================

/*
FilenameIterator is used by the MultiFileBuffer to determine the new file name
when rotating the buffer files. Basename is called before doing any calculation.
This function should do general filename decoration. If the decoration changes
over time then the function needs to also handle the cleanup.
*/
type FilenameIterator interface {

	/*
		Basename decorades the initial file name.
	*/
	Basename(filename string) string

	/*
		NextName returns the next file name based on the current file name.
		An empty string means the end of the iteration.
	*/
	NextName(currentName string) string
}

/*
ConsecutiveNumberIterator creates a new file name iterator which adds numbers
at the end of files. Up to maxNum files will be created. A maxNum parameter
< 1 means there is no limit.
*/
func ConsecutiveNumberIterator(maxNum int) FilenameIterator {
	return &consecutiveNumberIterator{maxNum}
}

/*
consecutiveNumberIterator is the implementation of the consecutive number
file iterator.
*/
type consecutiveNumberIterator struct {
	maxNum int
}

/*
Basename decorades the initial file name.
*/
func (it *consecutiveNumberIterator) Basename(filename string) string {
	return filename
}

/*
NextName returns the next file name based on the current file name.
An empty string means the end of the iteration.
*/
func (it *consecutiveNumberIterator) NextName(currentName string) string {

	if i := strings.LastIndex(currentName, "."); i > 0 {

		if num, err := strconv.ParseInt(currentName[i+1:], 10, 64); err == nil {

			nextNum := int(num + 1)

			if it.maxNum > 0 && nextNum > it.maxNum {
				return ""
			}

			return fmt.Sprintf("%s.%v", currentName[:i], nextNum)
		}
	}

	return fmt.Sprintf("%s.1", currentName)
}

/*
DailyDateIterator creates a new file name iterator which adds dates at the
end of files. The log will be switched at least once every day. Up to maxNumPerDay
files will be created per day. A maxNumPerDay parameter < 1 means there is no limit.
Up to maxDays different days will be kept (oldest ones are deleted). A maxDays
parameter < 1 means everything is kept.
*/
func DailyDateIterator(maxNumPerDay int, maxDays int) FilenameIterator {
	return &dailyDateIterator{&consecutiveNumberIterator{maxNumPerDay}, maxDays, timeutil.MakeTimestamp}
}

/*
consecutiveNumberIterator is the implementation of the consecutive number
file iterator.
*/
type dailyDateIterator struct {
	*consecutiveNumberIterator
	maxDays int
	tsFunc  func() string // Timestamp function
}

/*
NextName returns the next file name based on the current file name.
An empty string means the end of the iteration.
*/
func (it *dailyDateIterator) Basename(filename string) string {

	// Get todays date

	ts := it.tsFunc()
	today, _ := timeutil.TimestampString(ts, "UTC")
	today = today[:10]

	// Cleanup old files

	if it.maxDays > 0 {

		prefix := path.Base(filename)
		dir := path.Dir(filename)

		if files, err := ioutil.ReadDir(dir); err == nil {
			var datesToConsider []string

			// Collect all relevant files

			foundToday := false

			for _, f := range files {

				if strings.HasPrefix(f.Name(), prefix) && len(f.Name()) > len(prefix) {

					dateString := f.Name()[len(prefix)+1:]
					if !strings.ContainsRune(dateString, '.') {
						datesToConsider = append(datesToConsider, dateString)
						if !foundToday {
							foundToday = dateString == today
						}
					}
				}
			}

			// Make sure today is one of the dates

			if !foundToday {
				datesToConsider = append(datesToConsider, today)
			}

			// Sort them so the newest ones are kept

			sort.Strings(datesToConsider)

			//  Check if files need to be removed

			if len(datesToConsider) > it.maxDays {
				datesToRemove := datesToConsider[:len(datesToConsider)-it.maxDays]

				for _, f := range files {
					for _, dateToRemove := range datesToRemove {

						if strings.HasPrefix(f.Name(), fmt.Sprintf("%s.%s", prefix, dateToRemove)) {

							os.Remove(path.Join(dir, f.Name()))
						}
					}
				}
			}
		}
	}

	return fmt.Sprintf("%s.%s", filename, today)
}
