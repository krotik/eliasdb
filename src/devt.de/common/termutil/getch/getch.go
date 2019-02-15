/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package getch implements a platform agnostic character-wise input capture.

Things which work on all platforms:

- Detection of special keys F1-F12, Esc, Enter, Cursor keys, etc.
- Key combinations: CTRL+<Letter>, Shift+<Letter>
- Normal character input via KeyEvent.Rune

Example code:

func main() {
	var err error
	var e *getch.KeyEvent

	if err = getch.Start(); err != nil {
		fmt.Println(err)
		return
	}
	defer getch.Stop()

	for e == nil || e.Code != getch.KeyTab {
		e, err = getch.Getch()

		fmt.Println("==>", e, err)
	}
}
*/
package getch

import (
	"errors"
	"fmt"
)

// Static errors

/*
ErrNotImplemented is returned if the platform is not supported by getch
*/
var ErrNotImplemented = errors.New("Not implemented")

// Detail specific (dynamic) errors

/*
ErrUnknownEscapeSequence is returned if an unknown escape sequence was retrieved.
The sequence in question is located in the Detail byte slice.
*/
type ErrUnknownEscapeSequence struct {
	Detail []byte
}

/*
Error returns a string representation of this error.
*/
func (e *ErrUnknownEscapeSequence) Error() string {
	return fmt.Sprintf("Unknown escape sequence: %v", e.Detail)
}

/*
KeyCode is a platform-independent key code
*/
type KeyCode string

/*
Key codes for the KeyEvent object
*/
const (

	// Function keys

	KeyF1  = "Key_F1"
	KeyF2  = "Key_F2"
	KeyF3  = "Key_F3"
	KeyF4  = "Key_F4"
	KeyF5  = "Key_F5"
	KeyF6  = "Key_F6"
	KeyF7  = "Key_F7"
	KeyF8  = "Key_F8"
	KeyF9  = "Key_F9"
	KeyF10 = "Key_F10"
	KeyF11 = "Key_F11"
	KeyF12 = "Key_F12"

	// Control keys

	KeyEnter      = "Key_ENTER"
	KeyBackspace  = "Key_BACKSPACE"
	KeyEsc        = "Key_ESC"
	KeyTab        = "Key_TAB"
	KeyInsert     = "Key_INSERT"
	KeyDelete     = "Key_DELETE"
	KeyHome       = "Key_HOME"
	KeyEnd        = "Key_END"
	KeyPgup       = "Key_PAGE_UP"
	KeyPgdn       = "Key_PAGE_DOWN"
	KeyArrowUp    = "Key_ARROW_UP"
	KeyArrowDown  = "Key_ARROW_DOWN"
	KeyArrowLeft  = "Key_ARROW_LEFT"
	KeyArrowRight = "Key_ARROW_RIGHT"
	KeyCommand    = "Key_CMD" // "Windows" key

	// Normal letters

	KeyA = "Key_A"
	KeyB = "Key_B"
	KeyC = "Key_C"
	KeyD = "Key_D"
	KeyE = "Key_E"
	KeyF = "Key_F"
	KeyG = "Key_G"
	KeyH = "Key_H"
	KeyI = "Key_I"
	KeyJ = "Key_J"
	KeyK = "Key_K"
	KeyL = "Key_L"
	KeyM = "Key_M"
	KeyN = "Key_N"
	KeyO = "Key_O"
	KeyP = "Key_P"
	KeyQ = "Key_Q"
	KeyR = "Key_R"
	KeyS = "Key_S"
	KeyT = "Key_T"
	KeyU = "Key_U"
	KeyV = "Key_V"
	KeyW = "Key_W"
	KeyX = "Key_X"
	KeyY = "Key_Y"
	KeyZ = "Key_Z"

	// Normal numbers

	Key1 = "Key_1"
	Key2 = "Key_2"
	Key3 = "Key_3"
	Key4 = "Key_4"
	Key5 = "Key_5"
	Key6 = "Key_6"
	Key7 = "Key_7"
	Key8 = "Key_8"
	Key9 = "Key_9"
	Key0 = "Key_0"

	// Normal Symbols

	KeyBacktick     = "Key_BACKTICK"
	KeyMinus        = "Key_MINUS"
	KeyEqual        = "Key_EQUAL"
	KeyBracketOpen  = "Key_BRACKET_OPEN"
	KeyBracketClose = "Key_BRACKET_CLOSE"
	KeySemiColon    = "Key_SEMICOLON"
	KeyQuote        = "Key_QUOTE"
	KeyHash         = "Key_HASH"
	KeyBackslash    = "Key_BACKSLASH"
	KeyComma        = "Key_COMMA"
	KeyDot          = "Key_DOT"
	KeySlash        = "Key_SLASH"

	// Special states

	KeyUnknown = "Key_UNKNOWN"
)

/*
KeyEvent objects are produced by an input reader.
*/
type KeyEvent struct {
	Code   KeyCode // Code of the pressed key
	Ctrl   bool    // Flag if the ctrl key is also pressed
	Alt    bool    // Flag if the alt key is also pressed
	Shift  bool    // Flag if the shift key is also pressed
	Rune   rune    // Produced rune if the key is printable
	RawBuf []byte  // Raw input buffer since the last key event
}

func (k *KeyEvent) String() string {
	ret := fmt.Sprintf("%v %c [%#v - 0x%x]", k.Code, k.Rune, k.Rune, k.Rune)
	if k.Shift {
		ret += " + SHIFT"
	}
	if k.Ctrl {
		ret += " + CTRL"
	}
	if k.Alt {
		ret += " + ALT"
	}
	return ret
}

/*
internalKeyEvent is used to pass additional information to the getch function
*/
type internalKeyEvent struct {
	*KeyEvent
	err error
}

/*
getch is a platform-native single character input reader object.
*/
type getch interface {

	/*
	   GetKey returns the next key event or an error. This function blocks if no key
	   event is available.
	*/
	GetKey() *internalKeyEvent

	/*
	   GetKeyAsync returns the next key event or an error. This function does not block if no key
	   event is available - in this case nil is returned.
	*/
	GetKeyAsync() *internalKeyEvent

	/*
		CursorPosition returns the current cursor position.
	*/
	CursorPosition() (int, int, error)

	/*
	   SetCursorPosition sets the current cursor position
	*/
	SetCursorPosition(int, int) error

	/*
		Close detaches the character reader.
	*/
	Close()
}

/*
Singleton getch instance.
*/
var g getch

/*
Start starts the character reader.
*/
func Start() error {
	var err error

	if g == nil {
		g, err = attachReader()
	}

	return err
}

/*
CursorPosition returns the current cursor position.
*/
func CursorPosition() (int, int, error) {
	var x, y int
	var err error

	if g != nil {
		x, y, err = g.CursorPosition()
	}

	return x, y, err
}

/*
SetCursorPosition sets the current cursor position.
*/
func SetCursorPosition(x, y int) error {
	var err error

	if g != nil {
		err = g.SetCursorPosition(x, y)
	}

	return err
}

/*
Getch reads a single character.
*/
func Getch() (*KeyEvent, error) {
	var ret *KeyEvent
	var err error

	if g != nil {
		ke := g.GetKey()

		if err = ke.err; err == nil {
			ret = ke.KeyEvent
		}
	}

	return ret, err
}

/*
Stop stops the character reader.
*/
func Stop() {
	if g != nil {
		g.Close()
		g = nil
	}
}
