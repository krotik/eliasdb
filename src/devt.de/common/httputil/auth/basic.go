/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package auth

import (
	"encoding/base64"
	"net/http"
	"strings"

	"devt.de/common/httputil/user"
)

/*
Realm is the authentication realm
*/
var Realm = "RestrictedAccessRealm"

/*
BashicAuthHandleFuncWrapper datastructure. Wrapper for HandleFunc to add
basic authentication to all added endpoints.
*/
type BashicAuthHandleFuncWrapper struct {
	origHandleFunc func(pattern string, handler func(http.ResponseWriter, *http.Request))
	authFunc       func(user, pass string) bool
	accessFunc     func(http.ResponseWriter, *http.Request, string) bool

	// Callbacks

	CallbackSessionExpired func(w http.ResponseWriter, r *http.Request)
	CallbackUnauthorized   func(w http.ResponseWriter, r *http.Request)
}

/*
NewBashicAuthHandleFuncWrapper creates a new HandleFunc wrapper.
*/
func NewBashicAuthHandleFuncWrapper(origHandleFunc func(pattern string,
	handler func(http.ResponseWriter, *http.Request))) *BashicAuthHandleFuncWrapper {

	return &BashicAuthHandleFuncWrapper{
		origHandleFunc,
		nil,
		nil,

		// Session expired callback

		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("WWW-Authenticate", `Basic realm="`+Realm+`"`)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Session expired\n"))
		},
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("WWW-Authenticate", `Basic realm="`+Realm+`"`)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized\n"))
		},
	}
}

/*
SetAuthFunc gives an authentication function which can be used by the wrapper
to authenticate users.
*/
func (bw *BashicAuthHandleFuncWrapper) SetAuthFunc(authFunc func(user, pass string) bool) {
	bw.authFunc = authFunc
}

/*
SetAccessFunc sets an access function which can be used by the wrapper to
check the user access rights.
*/
func (bw *BashicAuthHandleFuncWrapper) SetAccessFunc(accessFunc func(http.ResponseWriter, *http.Request, string) bool) {
	bw.accessFunc = accessFunc
}

/*
HandleFunc is the new handle func which wraps an original handle functions to do an authentication check.
*/
func (bw *BashicAuthHandleFuncWrapper) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {

	bw.origHandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {

		if name, res := bw.CheckAuth(r); res {

			session, err := user.UserSessionManager.GetSession(name, w, r, true)

			if session != nil && err == nil {

				// Check authorization

				if bw.accessFunc == nil || bw.accessFunc(w, r, name) {

					// Handle the request

					handler(w, r)
				}

				return
			}

			bw.CallbackSessionExpired(w, r)

			return
		}

		bw.CallbackUnauthorized(w, r)
	})
}

/*
CheckAuth checks the user authentication of an incomming request. Returns
if the authentication is correct and the given username.
*/
func (bw *BashicAuthHandleFuncWrapper) CheckAuth(r *http.Request) (string, bool) {
	var user string
	var ok bool

	if s := strings.SplitN(r.Header.Get("Authorization"), " ", 2); len(s) == 2 {

		if b, err := base64.StdEncoding.DecodeString(s[1]); err == nil {

			if pair := strings.Split(string(b), ":"); len(pair) == 2 {

				user = pair[0]
				pass := pair[1]

				ok = bw.authFunc != nil && bw.authFunc(user, pass)
			}
		}
	}

	return user, ok
}
