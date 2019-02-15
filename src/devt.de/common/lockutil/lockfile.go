/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package lockutil contains a file based lock which can be used to lock file resources
across different processes. The lock file is monitored by a Go routine. Invalidating
the lock file (e.g. just writing a single character to it) causes the Go routine
to exit. A client can check if the lockfile is still being monitored by calling
WatcherRunning().
*/
package lockutil

import (
	"errors"
	"fmt"
	"os"
	"time"
)

/*
LockFile data structure
*/
type LockFile struct {
	filename  string        // Filename for LockFile
	timestamp int64         // Timestamp to uniquely indentify the lockfile
	interval  time.Duration // Interval with which the file should be watched
	errorChan chan error    // Error communication channel with watcher goroutine
	running   bool          // Flag to indicate that a lockfile is being watched
}

/*
NewLockFile creates a new LockFile which and watch it in given intervals.
*/
func NewLockFile(filename string, interval time.Duration) *LockFile {
	return &LockFile{filename, time.Now().UnixNano(), interval, nil, false}
}

/*
watch is the internal watcher goroutine function.
*/
func (lf *LockFile) watch() {

	// Attempt to read the lockfile - no error checking since the next write
	// lockfile call will catch any file related errors

	res, _ := lf.checkLockfile()

	if err := lf.writeLockfile(); err != nil {
		lf.errorChan <- err
		return
	}

	if res != 0 {

		time.Sleep(lf.interval * 10)

		// If we have overwritten an existing timestamp then check
		// if it was overwritten again by another process after some time

		res, err := lf.checkLockfile()

		if res != lf.timestamp || err != nil {

			lf.errorChan <- errors.New(fmt.Sprint(
				"Could not write lockfile - read result after writing: ", res,
				"(expected: ", lf.timestamp, ")", err))
			return
		}
	}

	// Signal that all is well

	lf.running = true
	lf.errorChan <- nil

	for lf.running {

		// Wakeup every interval and read the file

		time.Sleep(lf.interval)

		res, err := lf.checkLockfile()
		if err != nil {

			// Shut down if we get an error back

			lf.running = false
			lf.errorChan <- err

			return
		}

		if res != lf.timestamp {

			// Attempt to write the timestamp again - no error checking
			// if it fails we'll try again next time

			lf.writeLockfile()
		}
	}

	// At this point lf.running is false - remove lockfile and return

	lf.errorChan <- os.Remove(lf.filename)
}

/*
Write a timestamp to the lockfile
*/
func (lf *LockFile) writeLockfile() error {
	file, err := os.OpenFile(lf.filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
	if err != nil {
		return err
	}
	defer file.Close()

	data := make([]byte, 8)

	data[0] = byte(lf.timestamp >> 56)
	data[1] = byte(lf.timestamp >> 48)
	data[2] = byte(lf.timestamp >> 40)
	data[3] = byte(lf.timestamp >> 32)
	data[4] = byte(lf.timestamp >> 24)
	data[5] = byte(lf.timestamp >> 16)
	data[6] = byte(lf.timestamp >> 8)
	data[7] = byte(lf.timestamp >> 0)

	_, err = file.Write(data)

	return err
}

/*
Try to read a timestamp from a lockfile
*/
func (lf *LockFile) checkLockfile() (int64, error) {
	file, err := os.OpenFile(lf.filename, os.O_RDONLY, 0660)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	// Read timestamp
	timestamp := make([]byte, 8)
	i, err := file.Read(timestamp)

	if i != 8 {
		return 0, errors.New(fmt.Sprint("Unexpected timestamp value found in lockfile:", timestamp))
	}

	return (int64(timestamp[0]) << 56) |
		(int64(timestamp[1]) << 48) |
		(int64(timestamp[2]) << 40) |
		(int64(timestamp[3]) << 32) |
		(int64(timestamp[4]) << 24) |
		(int64(timestamp[5]) << 16) |
		(int64(timestamp[6]) << 8) |
		(int64(timestamp[7]) << 0), err
}

/*
Start creates the lockfile and starts watching it.
*/
func (lf *LockFile) Start() error {

	// Do nothing if the lockfile is already being watched

	if lf.running {
		return nil
	}

	// Set the running flag and kick off the watcher goroutine

	lf.errorChan = make(chan error)

	go lf.watch()

	return <-lf.errorChan
}

/*
WatcherRunning returns if the watcher goroutine is running.
*/
func (lf *LockFile) WatcherRunning() bool {
	return lf.running
}

/*
Finish watching a lockfile and return once the watcher goroutine has finished.
*/
func (lf *LockFile) Finish() error {
	var err error

	// Do nothing if the lockfile is not being watched

	if !lf.running {

		// Clean up if there is a channel still open

		if lf.errorChan != nil {
			err = <-lf.errorChan
			lf.errorChan = nil
		}

		return err
	}

	// Signale the watcher goroutine to stop

	lf.running = false

	// Wait for the goroutine to finish

	err = <-lf.errorChan
	lf.errorChan = nil

	return err
}
