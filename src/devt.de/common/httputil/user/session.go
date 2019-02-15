/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package user contains user session management for webservers. Sessions are
identified via session cookies and stored in memory on the server side.
*/
package user

import "fmt"

/*
Session models a user session object.
*/
type Session interface {

	/*
		Id returns the session id.
	*/
	ID() string

	/*
	   User returns the user of the session.
	*/
	User() string

	/*
		GetAll returns all known session values.
	*/
	GetAll() map[string]interface{}

	/*
		Get returns a session.
	*/
	Get(key string) (interface{}, bool)

	/*
		Set sets a session value. A nil value deletes a value
		from the session.
	*/
	Set(key string, value interface{})

	/*
		String returns a string representation of the session.
	*/
	String() string
}

/*
NewDefaultSession creates a new default session object.
*/
func NewDefaultSession(id string, user string) Session {
	return &DefaultSession{id, user, make(map[string]interface{})}
}

/*
DefaultSession is the default manager for web sessions.
*/
type DefaultSession struct {
	id     string
	user   string
	values map[string]interface{}
}

/*
ID returns the session id.
*/
func (ds *DefaultSession) ID() string {
	return ds.id
}

/*
User returns the user of the session.
*/
func (ds *DefaultSession) User() string {
	return ds.user
}

/*
GetAll returns all known session values.
*/
func (ds *DefaultSession) GetAll() map[string]interface{} {
	return ds.values
}

/*
Get returns a session.
*/
func (ds *DefaultSession) Get(key string) (interface{}, bool) {
	ret, ok := ds.values[key]
	return ret, ok
}

/*
Set sets a session value. A nil value deletes a value
from the session.
*/
func (ds *DefaultSession) Set(key string, value interface{}) {
	ds.values[key] = value
}

/*
	String returns a string representation of the session.
*/
func (ds *DefaultSession) String() string {
	return fmt.Sprint("Session: ", ds.id, " (User:", ds.user, " Values:", ds.values, ")")
}
