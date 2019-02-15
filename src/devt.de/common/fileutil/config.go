/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package fileutil contains file based utilities and helper functions.
*/
package fileutil

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"devt.de/common/stringutil"
)

/*
LoadConfig loads or creates a JSON based configuration file. Missing settings
from the config file will be filled with default settings. This function provides
a simple mechanism for programs to handle user-defined configuration files which
should be loaded at start time.
*/
func LoadConfig(filename string, defaultConfig map[string]interface{}) (map[string]interface{}, error) {
	var mdata []byte
	var data map[string]interface{}
	var err error
	var ok bool

	if ok, err = PathExists(filename); err != nil {
		return nil, err

	} else if ok {

		// Load config

		mdata, err = ioutil.ReadFile(filename)
		if err == nil {

			err = json.Unmarshal(mdata, &data)
			if err == nil {

				// Make sure all required configuration values are set

				for k, v := range defaultConfig {
					if dv, ok := data[k]; !ok || dv == nil {
						data[k] = v
					}
				}
			}
		}

	} else if err == nil {

		// Write config

		data = defaultConfig

		mdata, err = json.MarshalIndent(data, "", "    ")
		if err == nil {

			err = ioutil.WriteFile(filename, mdata, 0644)
		}
	}

	if err != nil {
		return nil, err
	}

	return data, nil
}

/*
ConfStr reads a config value as a string value.
*/
func ConfStr(config map[string]interface{}, key string) string {
	return fmt.Sprint(config[key])
}

/*
ConfBool reads a config value as a boolean value.
*/
func ConfBool(config map[string]interface{}, key string) bool {
	return strings.ToLower(fmt.Sprint(config[key])) == "true"
}

// Watched Config
// ==============

/*
WatchedConfigErrRetries is the number of times the code will try to
read the disk configuration before overwriting it with the current
(working) configuration. Set to -1 if it should never attempt to overwrite.
*/
var WatchedConfigErrRetries = 10

/*
watchSleep is the sleep which is used by the watch thread
*/
var watchSleep = time.Sleep

/*
Defined error codes for WatchedConfig
*/
var (
	ErrClosed = errors.New("Config file was closed")
)

/*
WatchedConfig is a helper object which continuously watches a given config file.
The file and the memory config are kept in sync.
*/
type WatchedConfig struct {
	config     map[string]interface{} // Internal in memory config
	configLock *sync.RWMutex          // Lock for config
	interval   time.Duration          // Interval with which the file should be watched
	filename   string                 // File which stores the config
	SyncError  error                  // Synchronization errors
	shutdown   chan bool              // Signal channel for thread shutdown
}

/*
NewWatchedConfig returns a new watcher object for a given config file.
*/
func NewWatchedConfig(filename string, defaultConfig map[string]interface{},
	interval time.Duration) (*WatchedConfig, error) {

	var ret *WatchedConfig

	config, err := LoadConfig(filename, defaultConfig)

	if err == nil {
		wc := &WatchedConfig{config, &sync.RWMutex{}, interval, filename, nil, nil}

		err = wc.start()

		if err == nil {
			ret = wc
		}
	}

	return ret, err
}

/*
GetValue returns a single config value.
*/
func (wc *WatchedConfig) GetValue(k string) (interface{}, bool, error) {
	wc.configLock.Lock()
	defer wc.configLock.Unlock()

	if wc.SyncError != nil {
		return nil, false, wc.SyncError
	}

	val, ok := wc.config[k]

	return val, ok, nil
}

/*
GetConfig returns the current config.
*/
func (wc *WatchedConfig) GetConfig() (map[string]interface{}, error) {
	wc.configLock.Lock()
	defer wc.configLock.Unlock()

	if wc.SyncError != nil {
		return nil, wc.SyncError
	}

	cconfig := make(map[string]interface{})

	for k, v := range wc.config {
		cconfig[k] = v
	}

	return cconfig, nil
}

/*
start kicks off the file watcher background thread.
*/
func (wc *WatchedConfig) start() error {

	// Sync from file - if the file exists. No need to hold a lock since
	// we are in the startup

	err := wc.sync(true)

	if err == nil {

		// Kick off watcher

		wc.shutdown = make(chan bool)

		go wc.watch()
	}

	return err
}

/*
watch is the internal file watch goroutine function.
*/
func (wc *WatchedConfig) watch() {
	err := wc.SyncError
	errCnt := 0

	defer func() {
		wc.shutdown <- true
	}()

	for wc.SyncError != ErrClosed {

		// Wakeup every interval

		watchSleep(wc.interval)

		// Run the sync

		wc.configLock.Lock()

		// Sync from file

		if err = wc.sync(true); err != nil && wc.SyncError != ErrClosed {

			// Increase the error count

			err = fmt.Errorf("Could not sync config from disk: %v",
				err.Error())

			errCnt++

		} else {

			// Reset the error count

			errCnt = 0
		}

		// Update the sync error

		if wc.SyncError != ErrClosed {
			wc.SyncError = err
		}

		if errCnt == WatchedConfigErrRetries {

			// We can't read the disk configuration after
			// WatchedConfigErrRetries attempts - try to overwrite
			// it with the working memory configuration

			wc.sync(false)
		}

		wc.configLock.Unlock()
	}
}

/*
Close closes this config watcher.
*/
func (wc *WatchedConfig) Close() error {
	var err error

	wc.configLock.Lock()

	if wc.SyncError != nil {

		// Preserve any old error

		err = wc.SyncError
	}

	// Set the table into the closed state

	wc.SyncError = ErrClosed

	wc.configLock.Unlock()

	// Wait for watcher shutdown if it was started

	if wc.shutdown != nil {
		<-wc.shutdown
		wc.shutdown = nil
	}

	return err
}

/*
Attempt to synchronize the memory config with the file. Depending on the
checkFile flag either the file (true) or the memory config (false) is
regarded as up-to-date.

It is assumed that the configLock (write) is held before calling this
function.

The table is in an undefined state if an error is returned.
*/
func (wc *WatchedConfig) sync(checkFile bool) error {
	var checksumFile, checksumMemory string

	stringMemoryTable := func() ([]byte, error) {
		return json.MarshalIndent(wc.config, "", "  ")
	}

	writeMemoryTable := func() error {
		res, err := stringMemoryTable()

		if err == nil {
			err = ioutil.WriteFile(wc.filename, res, 0644)
		}

		return err
	}

	readMemoryTable := func() (map[string]interface{}, error) {
		var conf map[string]interface{}

		res, err := ioutil.ReadFile(wc.filename)

		if err == nil {
			err = json.Unmarshal(stringutil.StripCStyleComments(res), &conf)
		}

		return conf, err
	}

	// Check if the file can be opened

	file, err := os.OpenFile(wc.filename, os.O_RDONLY, 0660)

	if err != nil {

		if os.IsNotExist(err) {

			// Just ignore not found errors

			err = nil
		}

		// File does not exist - no checksum

		checksumFile = ""

	} else {

		hashFactory := sha256.New()

		if _, err = io.Copy(hashFactory, file); err == nil {

			// Create the checksum of the present file

			checksumFile = fmt.Sprintf("%x", hashFactory.Sum(nil))
		}

		file.Close()
	}

	if err == nil {

		// At this point we know everything about the file now check
		// the memory table

		var cString []byte

		if cString, err = stringMemoryTable(); err == nil {
			hashFactory := sha256.New()

			hashFactory.Write(cString)

			checksumMemory = fmt.Sprintf("%x", hashFactory.Sum(nil))
		}
	}

	if err == nil {

		// At this point we also know everything about the memory table

		if checkFile {

			// File is up-to-date - we should build the memory table

			if checksumFile != checksumMemory {
				var conf map[string]interface{}

				if conf, err = readMemoryTable(); err == nil {
					wc.config = conf
				}
			}

		} else {

			// Memory is up-to-date - we should write a new file

			if checksumFile != checksumMemory {

				err = writeMemoryTable()
			}
		}
	}

	return err
}
