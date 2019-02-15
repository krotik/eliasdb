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
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"devt.de/common/datautil"
	"devt.de/common/errorutil"
	"devt.de/common/httputil/user"
)

/*
cookieNameAuth defines the auth cookie name
*/
const cookieNameAuth = "~aid"

/*
CookieMaxLifetime is the max life time of an auth cookie in seconds
*/
var CookieMaxLifetime = 3600

/*
TestCookieAuthDisabled is a flag to disable cookie based authentication temporarily
(should only be used by unit tests)
*/
var TestCookieAuthDisabled = false

/*
CookieAuthHandleFuncWrapper datastructure. Wrapper for HandleFunc to add
cookie authentication to all added endpoints.
*/
type CookieAuthHandleFuncWrapper struct {
	origHandleFunc func(pattern string, handler func(http.ResponseWriter, *http.Request))
	authFunc       func(user, pass string) bool
	accessFunc     func(http.ResponseWriter, *http.Request, string) bool
	tokenMap       *datautil.MapCache
	expiry         int
	publicURL      map[string]func(http.ResponseWriter, *http.Request)

	// Callbacks

	CallbackSessionExpired func(w http.ResponseWriter, r *http.Request)
	CallbackUnauthorized   func(w http.ResponseWriter, r *http.Request)
}

/*
NewCookieAuthHandleFuncWrapper creates a new HandleFunc wrapper.
*/
func NewCookieAuthHandleFuncWrapper(origHandleFunc func(pattern string,
	handler func(http.ResponseWriter, *http.Request))) *CookieAuthHandleFuncWrapper {

	return &CookieAuthHandleFuncWrapper{
		origHandleFunc,
		nil,
		nil,
		datautil.NewMapCache(0, int64(CookieMaxLifetime)),
		CookieMaxLifetime,
		make(map[string]func(http.ResponseWriter, *http.Request)),

		// Session expired callback

		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Session expired\n"))
		},
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized\n"))
		},
	}
}

/*
AddPublicPage adds a page which should be accessible without authentication.
using a special handler.
*/
func (cw *CookieAuthHandleFuncWrapper) AddPublicPage(url string, handler func(http.ResponseWriter, *http.Request)) {
	cw.publicURL[url] = handler
}

/*
Expiry returns the current authentication expiry time in seconds.
*/
func (cw *CookieAuthHandleFuncWrapper) Expiry() int {
	return cw.expiry
}

/*
SetExpiry sets the authentication expiry time in seconds. All existing authentications
are retracted during this function call.
*/
func (cw *CookieAuthHandleFuncWrapper) SetExpiry(secs int) {
	cw.expiry = secs
	cw.tokenMap = datautil.NewMapCache(0, int64(secs))
}

/*
SetAuthFunc sets an authentication function which can be used by the wrapper
to authenticate users.
*/
func (cw *CookieAuthHandleFuncWrapper) SetAuthFunc(authFunc func(user, pass string) bool) {
	cw.authFunc = authFunc
}

/*
SetAccessFunc sets an access function which can be used by the wrapper to
check the user access rights.
*/
func (cw *CookieAuthHandleFuncWrapper) SetAccessFunc(accessFunc func(http.ResponseWriter, *http.Request, string) bool) {
	cw.accessFunc = accessFunc
}

/*
AuthUser authenticates a user and creates an auth token unless testOnly is true.
Returns an empty string if the authentication was not successful.
*/
func (cw *CookieAuthHandleFuncWrapper) AuthUser(user, pass string, testOnly bool) string {

	if cw.authFunc != nil && cw.authFunc(user, pass) {

		if !testOnly {

			// Generate a valid auth token

			aid := cw.newAuthID()

			cw.tokenMap.Put(aid, user)

			return aid
		}

		return "ok"
	}

	return ""
}

/*
CheckAuth checks the user authentication of an incomming request. Returns
if the authentication is correct and the given username.
*/
func (cw *CookieAuthHandleFuncWrapper) CheckAuth(r *http.Request) (string, bool) {
	var name string
	var ok bool

	cookie, _ := r.Cookie(cookieNameAuth)

	if cookie != nil && cookie.Value != "" {
		var user interface{}
		if user, ok = cw.tokenMap.Get(cookie.Value); ok {
			name = fmt.Sprint(user)
		}
	}

	return name, ok
}

/*
SetAuthCookie sets the auth cookie in a given response object.
*/
func (cw *CookieAuthHandleFuncWrapper) SetAuthCookie(yaid string, w http.ResponseWriter) {

	if yaid == "" {

		// Nothing to do if no auth id is given

		return
	}

	cookie := http.Cookie{
		Name:     cookieNameAuth,
		Value:    url.QueryEscape(yaid),
		Path:     "/",
		HttpOnly: true,
		MaxAge:   cw.expiry,
	}
	http.SetCookie(w, &cookie)
}

/*
RemoveAuthCookie removes the auth cookie in a given response object and invalidates
it.
*/
func (cw *CookieAuthHandleFuncWrapper) RemoveAuthCookie(w http.ResponseWriter) {

	cookie := http.Cookie{
		Name:     cookieNameAuth,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	}
	http.SetCookie(w, &cookie)
}

/*
InvalidateAuthCookie invalidates the authentication of an incomming request.
*/
func (cw *CookieAuthHandleFuncWrapper) InvalidateAuthCookie(r *http.Request) {
	cookie, _ := r.Cookie(cookieNameAuth)

	if cookie != nil && cookie.Value != "" {
		cw.tokenMap.Remove(cookie.Value)
	}
}

/*
newAuthID creates a new auth id.
*/
func (cw *CookieAuthHandleFuncWrapper) newAuthID() string {
	b := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, b)

	errorutil.AssertOk(err)

	return fmt.Sprintf("A-%x", b)
}

/*
HandleFunc is the new handle func which wraps an original handle functions to do an authentication check.
*/
func (cw *CookieAuthHandleFuncWrapper) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {

	cw.origHandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {

		// Check if this is a public URL

		if chandler, ok := cw.publicURL[r.URL.Path]; ok {
			chandler(w, r)
			return
		}

		// Check if authentication is disabled

		if TestCookieAuthDisabled {
			handler(w, r)
			return
		}

		// Retrieve the cookie value

		cookie, _ := r.Cookie(cookieNameAuth)

		if cookie != nil && cookie.Value != "" {

			// Check in the token map if the user was authenticated

			if name, ok := cw.tokenMap.Get(cookie.Value); ok {
				nameString := fmt.Sprint(name)

				// Create or retrieve the user session (this call sets the session
				// cookie in the response) - a session is considered expired if
				// a session cookie is found in the request but no corresponding
				// session can be found by the UserSessionManager

				session, err := user.UserSessionManager.GetSession(nameString, w, r, true)

				if session != nil && err == nil && session.User() == nameString {

					// Set the auth cookie in the response

					cw.SetAuthCookie(cookie.Value, w)

					// Check authorization

					if cw.accessFunc == nil || cw.accessFunc(w, r, nameString) {

						// Handle the request

						handler(w, r)
					}

					return
				}

				// Remove auth token entry since the session has expired

				defer cw.tokenMap.Remove(cookie.Value)

				cw.CallbackSessionExpired(w, r)

				return
			}
		}

		cw.CallbackUnauthorized(w, r)
	})
}
