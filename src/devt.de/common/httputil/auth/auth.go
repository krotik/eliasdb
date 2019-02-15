/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package auth contains user authentication code for webservers.

Basic access authentication requires a client to provide a user name and password
with each request. Most browsers will directly support this method.
See: https://en.wikipedia.org/wiki/Basic_access_authentication

Cookie based authentication requires the client to login once and create a unique
access token. The access token is then used to authenticate each request.
*/
package auth

import "net/http"

/*
HandleFuncWrapper is an abstract wrapper for handle functions to add authentication features.
*/
type HandleFuncWrapper interface {

	/*
		SetAuthFunc gives an authentication function which can be used by the
		wrapper to authenticate users.
	*/
	SetAuthFunc(authFunc func(user, pass string) bool)

	/*
	   HandleFunc is the new handle func which wraps an original handle functions to do an authentication check.
	*/
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))

	/*
	   CheckAuth checks the user authentication of an incomming request. Returns
	   if the authentication is correct and the given username.
	*/
	CheckAuth(r *http.Request) (string, bool)
}
