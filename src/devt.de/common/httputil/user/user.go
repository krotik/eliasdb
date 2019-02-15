/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package user

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"devt.de/common/datautil"
	"devt.de/common/errorutil"
)

/*
cookieName defines the session cookie name
*/
const cookieNameSession = "~sid"

/*
CookieMaxLifetime is the max life time of a session cookie in seconds
*/
var CookieMaxLifetime = 3600

/*
UserSessionManager manages all user sessions.
*/
var UserSessionManager = &SessionManager{sync.Mutex{},
	NewMemorySessionProvider()}

/*
SessionManager manages web sessions.
*/
type SessionManager struct {
	Lock     sync.Mutex
	Provider SessionProvider
}

/*
newSessionId creates a new session id.
*/
func (manager *SessionManager) newSessionID() string {
	b := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, b)

	errorutil.AssertOk(err)

	return fmt.Sprintf("S-%x", b)
}

/*
CheckSessionCookie checks if a request contains a session cookie and if the
session is active. Returns has cookie and is active.
*/
func (manager *SessionManager) CheckSessionCookie(r *http.Request) (bool, bool) {
	var session Session

	cookie, _ := r.Cookie(cookieNameSession)

	if cookie != nil {
		sid, _ := url.QueryUnescape(cookie.Value)
		session, _ = manager.Provider.Get(sid)
	}

	return cookie != nil, session != nil
}

/*
RemoveSessionCookie removes the session cookie in a given response object.
*/
func (manager *SessionManager) RemoveSessionCookie(w http.ResponseWriter) {

	cookie := http.Cookie{
		Name:     cookieNameSession,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	}
	http.SetCookie(w, &cookie)
}

/*
GetSession retrieves an existing or creates a new session
*/
func (manager *SessionManager) GetSession(user string, w http.ResponseWriter,
	r *http.Request, create bool) (Session, error) {

	manager.Lock.Lock()
	defer manager.Lock.Unlock()

	var session Session
	var err error
	var sid string

	// Retrieve the cookie

	cookie, cerr := r.Cookie(cookieNameSession)

	if cookie == nil || cookie.Value == "" {

		if !create {

			// Session is not present and it should not be created

			return nil, nil
		}

		// Session is not created if no user is present

		if user == "" {
			return nil, errors.New("Cannot create a session without a user")
		}

		// No cookie present - create a new session

		sid = manager.newSessionID()

		session, _ = manager.Provider.Init(sid, user)

	} else {

		// Session should be available

		sid, _ = url.QueryUnescape(cookie.Value)
		session, err = manager.Provider.Get(sid)
	}

	if create {

		// Write the session cookie in the response

		cookie = &http.Cookie{
			Name:     cookieNameSession,
			Value:    url.QueryEscape(sid),
			Path:     "/",
			HttpOnly: true,
			MaxAge:   CookieMaxLifetime,
		}

		http.SetCookie(w, cookie)
	}

	if cerr == http.ErrNoCookie {

		// Also register the cookie in the request so the session can
		// can be found by subsequent calls

		r.AddCookie(cookie)
	}

	return session, err
}

/*
SessionProvider is a session storage provider. Sessions should expire
after a certain amount of time.
*/
type SessionProvider interface {

	/*
		Create a new session for a given user. The session has an explicit
		expiry time after which a get will fail.
	*/
	Init(sid string, user string) (Session, error)

	/*
		Get retrieves a session.
	*/
	Get(sid string) (Session, error)

	/*
		GetAll returns a list of all sessions.
	*/
	GetAll() ([]Session, error)

	/*
		Destroy destroys a session.
	*/
	Destroy(sid string) error
}

/*
MemorySessionProvider keeps all session related data in memory.
*/
type MemorySessionProvider struct {
	sessions *datautil.MapCache // Thread safe memory cache
}

/*
NewMemorySessionProvider creates a new memory session provider. By default
sessions have the same expiry time as cookies.
*/
func NewMemorySessionProvider() SessionProvider {
	ret := &MemorySessionProvider{}
	ret.SetExpiry(CookieMaxLifetime)
	return ret
}

/*
SetExpiry sets the session expiry time in seconds. All existing sessions
are deleted during this function call. This call is not thread safe - only
use it during initialisation!
*/
func (ms *MemorySessionProvider) SetExpiry(secs int) {
	ms.sessions = datautil.NewMapCache(0, int64(secs))
}

/*
Init creates a new session for a given user. The session has an explicit
expiry time after which a get will fail.
*/
func (ms *MemorySessionProvider) Init(sid string, user string) (Session, error) {
	session := NewDefaultSession(sid, user)
	ms.sessions.Put(sid, session)
	return session, nil
}

/*
Get retrieves a session.
*/
func (ms *MemorySessionProvider) Get(sid string) (Session, error) {
	if session, _ := ms.sessions.Get(sid); session != nil {
		return session.(Session), nil
	}
	return nil, nil
}

/*
GetAll returns a list of all sessions.
*/
func (ms *MemorySessionProvider) GetAll() ([]Session, error) {
	sessions := make([]Session, 0, ms.sessions.Size())

	for _, s := range ms.sessions.GetAll() {
		sessions = append(sessions, s.(Session))
	}

	return sessions, nil
}

/*
Destroy destroys a session.
*/
func (ms *MemorySessionProvider) Destroy(sid string) error {
	ms.sessions.Remove(sid)
	return nil
}
