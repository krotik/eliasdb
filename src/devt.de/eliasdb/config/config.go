/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package config

import (
	"fmt"
	"path"
	"strconv"

	"devt.de/common/errorutil"
	"devt.de/common/fileutil"
)

// Global variables
// ================

/*
DefaultConfigFile is the default config file which will be used to configure EliasDB
*/
var DefaultConfigFile = "eliasdb.config.json"

/*
Known configuration options for EliasDB
*/
const (
	MemoryOnlyStorage        = "MemoryOnlyStorage"
	LocationDatastore        = "LocationDatastore"
	LocationHTTPS            = "LocationHTTPS"
	LocationWebFolder        = "LocationWebFolder"
	LocationUserDB           = "LocationUserDB"
	LocationAccessDB         = "LocationAccessDB"
	HTTPSCertificate         = "HTTPSCertificate"
	HTTPSKey                 = "HTTPSKey"
	LockFile                 = "LockFile"
	HTTPSHost                = "HTTPSHost"
	HTTPSPort                = "HTTPSPort"
	CookieMaxAgeSeconds      = "CookieMaxAgeSeconds"
	EnableReadOnly           = "EnableReadOnly"
	EnableWebFolder          = "EnableWebFolder"
	EnableAccessControl      = "EnableAccessControl"
	EnableWebTerminal        = "EnableWebTerminal"
	EnableCluster            = "EnableCluster"
	EnableClusterTerminal    = "EnableClusterTerminal"
	ResultCacheMaxSize       = "ResultCacheMaxSize"
	ResultCacheMaxAgeSeconds = "ResultCacheMaxAgeSeconds"
	ClusterStateInfoFile     = "ClusterStateInfoFile"
	ClusterConfigFile        = "ClusterConfigFile"
	ClusterLogHistory        = "ClusterLogHistory"
)

/*
DefaultConfig is the defaut configuration
*/
var DefaultConfig = map[string]interface{}{
	MemoryOnlyStorage:        false,
	EnableReadOnly:           false,
	EnableWebFolder:          true,
	EnableAccessControl:      false,
	EnableWebTerminal:        true,
	EnableCluster:            false,
	EnableClusterTerminal:    false,
	LocationDatastore:        "db",
	LocationHTTPS:            "ssl",
	LocationWebFolder:        "web",
	LocationUserDB:           "users.db",
	LocationAccessDB:         "access.db",
	HTTPSHost:                "localhost",
	HTTPSPort:                "9090",
	CookieMaxAgeSeconds:      "86400",
	HTTPSCertificate:         "cert.pem",
	HTTPSKey:                 "key.pem",
	LockFile:                 "eliasdb.lck",
	ResultCacheMaxSize:       0,
	ResultCacheMaxAgeSeconds: 0,
	ClusterStateInfoFile:     "cluster.stateinfo",
	ClusterConfigFile:        "cluster.config.json",
	ClusterLogHistory:        100.0,
}

/*
Config is the actual config which is used
*/
var Config map[string]interface{}

/*
LoadConfigFile loads a given config file. If the config file does not exist it is
created with the default options.
*/
func LoadConfigFile(configfile string) error {
	var err error

	Config, err = fileutil.LoadConfig(configfile, DefaultConfig)

	return err
}

/*
LoadDefaultConfig loads the default configuration.
*/
func LoadDefaultConfig() {
	data := make(map[string]interface{})
	for k, v := range DefaultConfig {
		data[k] = v
	}

	Config = data
}

// Helper functions
// ================

/*
Str reads a config value as a string value.
*/
func Str(key string) string {
	return fmt.Sprint(Config[key])
}

/*
Int reads a config value as an int value.
*/
func Int(key string) int64 {
	ret, err := strconv.ParseInt(fmt.Sprint(Config[key]), 10, 64)

	errorutil.AssertTrue(err == nil,
		fmt.Sprintf("Could not parse config key %v: %v", key, err))

	return ret
}

/*
Bool reads a config value as a boolean value.
*/
func Bool(key string) bool {
	ret, err := strconv.ParseBool(fmt.Sprint(Config[key]))

	errorutil.AssertTrue(err == nil,
		fmt.Sprintf("Could not parse config key %v: %v", key, err))

	return ret
}

/*
WebPath returns a path relative to the web directory.
*/
func WebPath(parts ...string) string {
	return path.Join("web", path.Join(parts...))
}
