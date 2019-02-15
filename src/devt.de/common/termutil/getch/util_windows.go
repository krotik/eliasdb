/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package getch

/*
The code in this file was inspired by:

http://code.google.com/p/termbox
Pure Go termbox implementation
*/

import (
	"sync"
	"syscall"
	"time"
	"unsafe"
)

/*
attachReader attaches a character reader on Windows.
*/
func attachReader() (getch, error) {

	// Create an event object which can receive input events

	eobj, err := createEvent()

	if err == nil {
		gw := &getchWindows{eobj, 0, 0, make(chan *internalKeyEvent), make(chan bool),
			&sync.WaitGroup{}}

		// Open the console input device

		if gw.in, err = syscall.Open("CONIN$", syscall.O_RDWR, 0); err == nil {

			if gw.out, err = syscall.Open("CONOUT$", syscall.O_RDWR, 0); err == nil {

				// All is well we can start listening for events

				go gw.eventListener()

				// Make sure the input buffer is clear

				time.Sleep(100 * time.Millisecond)
				for gw.GetKeyAsync() != nil {
					time.Sleep(100 * time.Millisecond)
				}

				return gw, nil
			}
		}

		gw.Close()
	}

	return nil, err
}

// Getch object implementation for windows
// =======================================

/*
getchWindows is the Windows based getch implementation.
*/
type getchWindows struct {
	eobj         syscall.Handle         // Event object handle
	in           syscall.Handle         // Console input device handler
	out          syscall.Handle         // Console output device handle
	keyEventChan chan *internalKeyEvent // Channel for input events
	stopChan     chan bool              // Channel for shutdown event
	wg           *sync.WaitGroup        // Waitgroup for shutdown process
}

/*
CursorPosition returns the current cursor position.
*/
func (gw *getchWindows) CursorPosition() (int, int, error) {
	var x, y int
	var err error

	info, err := getConsoleScreenBufferInfo(gw.out)

	if err == nil {
		x = int(info.cursorPosition.x)
		y = int(info.cursorPosition.y)
	}

	return x, y, err
}

/*
SetCursorPosition sets the current cursor position
*/
func (gw *getchWindows) SetCursorPosition(x, y int) error {
	return setConsoleCursorPosition(gw.out, &winCOORD{int16(x), int16(y)})
}

/*
GetKey returns the next key event or an error. This function blocks if no key
event is available.
*/
func (gw *getchWindows) GetKey() *internalKeyEvent {
	return <-gw.keyEventChan
}

/*
GetKeyAsync returns the next key event or an error. This function does not block if no key
event is available - in this case nil is returned.
*/
func (gw *getchWindows) GetKeyAsync() *internalKeyEvent {
	select {
	case e := <-gw.keyEventChan:
		return e
	default:
	}
	return nil
}

/*
Close detaches the character reader.
*/
func (gw *getchWindows) Close() {

	// Send stop command and wait for the eventListener thread to end

	gw.wg.Add(1)

	// Empty all pending keys

	for gw.GetKeyAsync() != nil {
		time.Sleep(10 * time.Millisecond)
	}

	gw.stopChan <- true

	gw.wg.Wait()

	// Ignoring errors here since we are closing

	syscall.Close(gw.in)
	syscall.Close(gw.out)
	syscall.Close(gw.eobj)
}

/*
eventListener is a thread which will listen for events.
*/
func (gw *getchWindows) eventListener() {
	var ir inputRecord

	// Loop until an exit is requested

loop:
	for true {

		ok, err := waitForMultipleObjects([]syscall.Handle{gw.in, gw.eobj})

		if err != nil {
			gw.keyEventChan <- &internalKeyEvent{err: err}
		}

		// Check if we should stop

		select {
		case <-gw.stopChan:
			break loop
		default:
		}

		if err == nil && ok {

			if err = readConsoleInput(gw.in, &ir); err != nil {

				gw.keyEventChan <- &internalKeyEvent{err: err}

			} else {

				// Only analyse actual keyboard events

				if ir.eventType == 0x1 {

					if kr := (*winKeyEvent)(unsafe.Pointer(&ir.event)); kr.keyDown == 1 {
						gw.keyEventChan <- gw.buildKeyEvent(kr)
					}
				}
			}
		}
	}

	gw.wg.Done()
}

/*
buildKeyEvent builds an internalKeyEvent from a windows key event.
*/
func (gw *getchWindows) buildKeyEvent(wke *winKeyEvent) *internalKeyEvent {
	ike := &internalKeyEvent{&KeyEvent{}, nil}

	ike.Alt = wke.controlKeyState&(0x0001|0x0002) != 0  // Check if right alt or left alt is pressed
	ike.Ctrl = wke.controlKeyState&(0x0004|0x0008) != 0 // Check if right ctrl or left ctrl is pressed
	ike.Shift = wke.controlKeyState&0x0010 != 0         // Check if shift is pressed

	ike.Rune = rune(wke.unicodeChar)

	// Check for printable character

	switch wke.virtualKeyCode {

	case 0x41: // Letters
		ike.Code = KeyA
	case 0x42:
		ike.Code = KeyB
	case 0x43:
		ike.Code = KeyC
	case 0x44:
		ike.Code = KeyD
	case 0x45:
		ike.Code = KeyE
	case 0x46:
		ike.Code = KeyF
	case 0x47:
		ike.Code = KeyG
	case 0x48:
		ike.Code = KeyH
	case 0x49:
		ike.Code = KeyI
	case 0x4a:
		ike.Code = KeyJ
	case 0x4b:
		ike.Code = KeyK
	case 0x4c:
		ike.Code = KeyL
	case 0x4d:
		ike.Code = KeyM
	case 0x4e:
		ike.Code = KeyN
	case 0x4f:
		ike.Code = KeyO
	case 0x50:
		ike.Code = KeyP
	case 0x51:
		ike.Code = KeyQ
	case 0x52:
		ike.Code = KeyR
	case 0x53:
		ike.Code = KeyS
	case 0x54:
		ike.Code = KeyT
	case 0x55:
		ike.Code = KeyU
	case 0x56:
		ike.Code = KeyV
	case 0x57:
		ike.Code = KeyW
	case 0x58:
		ike.Code = KeyX
	case 0x59:
		ike.Code = KeyY
	case 0x5a:
		ike.Code = KeyZ

	case 0x30: // Numbers
		ike.Code = Key0
	case 0x31:
		ike.Code = Key1
	case 0x32:
		ike.Code = Key2
	case 0x33:
		ike.Code = Key3
	case 0x34:
		ike.Code = Key4
	case 0x35:
		ike.Code = Key5
	case 0x36:
		ike.Code = Key6
	case 0x37:
		ike.Code = Key7
	case 0x38:
		ike.Code = Key8
	case 0x39:
		ike.Code = Key9

	case 0xdf: // Symbols
		ike.Code = KeyBacktick
	case 0xbd:
		ike.Code = KeyMinus
	case 0xbb:
		ike.Code = KeyEqual
	case 0xdc:
		ike.Code = KeyBackslash
	case 0xbc:
		ike.Code = KeyComma
	case 0xbe:
		ike.Code = KeyDot
	case 0xbf:
		ike.Code = KeySlash
	case 0xba:
		ike.Code = KeySemiColon
	case 0xc0:
		ike.Code = KeyQuote
	case 0xde:
		ike.Code = KeyHash
	case 0xdb:
		ike.Code = KeyBracketOpen
	case 0xdd:
		ike.Code = KeyBracketClose

	default:

		// Key pressed cannot be a printable character

		ike.Rune = 0x00
	}

	// Check for non-printable keys

	switch wke.virtualKeyCode {

	case 0x70:
		ike.Code = KeyF1
	case 0x71:
		ike.Code = KeyF2
	case 0x72:
		ike.Code = KeyF3
	case 0x73:
		ike.Code = KeyF4
	case 0x74:
		ike.Code = KeyF5
	case 0x75:
		ike.Code = KeyF6
	case 0x76:
		ike.Code = KeyF7
	case 0x77:
		ike.Code = KeyF8
	case 0x78:
		ike.Code = KeyF9
	case 0x79:
		ike.Code = KeyF10
	case 0x7a:
		ike.Code = KeyF11
	case 0x7b:
		ike.Code = KeyF12

	case 0x9:
		ike.Code = KeyTab
	case 0xd:
		ike.Code = KeyEnter
	case 0x8:
		ike.Code = KeyBackspace
	case 0x1b:
		ike.Code = KeyEsc
	case 0x2d:
		ike.Code = KeyInsert
	case 0x2e:
		ike.Code = KeyDelete
	case 0x24:
		ike.Code = KeyHome
	case 0x23:
		ike.Code = KeyEnd
	case 0x21:
		ike.Code = KeyPgup
	case 0x22:
		ike.Code = KeyPgdn
	case 0x26:
		ike.Code = KeyArrowUp
	case 0x28:
		ike.Code = KeyArrowDown
	case 0x25:
		ike.Code = KeyArrowLeft
	case 0x27:
		ike.Code = KeyArrowRight
	case 0x5b:
		ike.Code = KeyCommand
	}

	return ike
}

// OS specific magic
// =================

var kernel32 = syscall.NewLazyDLL("kernel32.dll")
var syscallFunc = syscall.Syscall6

/*
createEvent creates an event object.

https://msdn.microsoft.com/en-gb/library/windows/desktop/ms682396(v=vs.85).aspx
*/
func createEvent() (syscall.Handle, error) {
	var err error

	r0, _, e1 := syscallFunc(kernel32.NewProc("CreateEventW").Addr(),
		4, 0, 0, 0, 0, 0, 0)

	if int(r0) == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}

	return syscall.Handle(r0), err
}

/*
waitForMultipleObjects waits (100ms) for an input event. Returns if an event was
received and any encountered errors.

https://msdn.microsoft.com/en-us/library/windows/desktop/ms687025(v=vs.85).aspx
*/
func waitForMultipleObjects(objects []syscall.Handle) (bool, error) {
	var err error

	r0, _, e1 := syscall.Syscall6(kernel32.NewProc("WaitForMultipleObjects").Addr(), 4,
		uintptr(len(objects)), uintptr(unsafe.Pointer(&objects[0])), 0, 10, 0, 0)

	if uint32(r0) == 0xFFFFFFFF {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}

	return uint32(r0) != 0x00000102 && err == nil, err
}

/*
winConsoleScreenBufferInfo is a CONSOLE_SCREEN_BUFFER_INFO which contains
information about a console screen buffer.

https://docs.microsoft.com/en-us/windows/console/console-screen-buffer-info-str
*/
type winConsoleScreenBufferInfo struct {
	size           winCOORD
	cursorPosition winCOORD
	attributes     uint16
	window         struct {
		left   int16
		top    int16
		right  int16
		bottom int16
	}
	maximumWindowSize winCOORD
}

var tempWinConsoleScreenBufferInfo = &winConsoleScreenBufferInfo{} // Temp space to prevent unnecessary heap allocations

/*
getConsoleScreenBufferInfo retrieves information about the specified console
screen buffer.

https://docs.microsoft.com/en-us/windows/console/getconsolescreenbufferinfo
*/
func getConsoleScreenBufferInfo(h syscall.Handle) (*winConsoleScreenBufferInfo, error) {
	var err error
	var ret *winConsoleScreenBufferInfo

	r0, _, e1 := syscall.Syscall(kernel32.NewProc("GetConsoleScreenBufferInfo").Addr(), 2,
		uintptr(h), uintptr(unsafe.Pointer(tempWinConsoleScreenBufferInfo)), 0)

	if int(r0) == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	} else {
		ret = tempWinConsoleScreenBufferInfo
	}

	return ret, err
}

/*
winCOORD is COORD structure which defines the coordinates of a character cell
in a console screen buffer.

https://docs.microsoft.com/en-us/windows/console/coord-str
*/
type winCOORD struct {
	x int16
	y int16
}

/*
setConsoleCursorPosition sets the cursor position.

https://docs.microsoft.com/en-us/windows/console/setconsolecursorposition
*/
func setConsoleCursorPosition(h syscall.Handle, pos *winCOORD) error {
	var err error

	r0, _, e1 := syscall.Syscall(kernel32.NewProc("SetConsoleCursorPosition").Addr(), 2,
		uintptr(h), uintptr(*(*int32)(unsafe.Pointer(pos))), 0)

	if int(r0) == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}

	return err
}

/*
inputRecord is a record read with readConsoleInput

https://docs.microsoft.com/en-us/windows/console/input-record-str
*/
type inputRecord struct {
	eventType uint16
	_         [2]byte
	event     [16]byte
}

/*
keyEventWin interprets the event from inputRecord as a windows key event

https://docs.microsoft.com/en-us/windows/console/key-event-record-str
*/
type winKeyEvent struct {
	keyDown         int32  // key is pressed
	repeatCount     uint16 // repeat count, which indicates that a key is being held down
	virtualKeyCode  uint16 // identifies the given key in a device-independent manner
	virtualScanCode uint16 //  represents the device-dependent value generated by the keyboard hardware
	unicodeChar     uint16 // translated Unicode character
	controlKeyState uint32 // state of the control keys
}

var temp uint32 // Temp space to prevent unnecessary heap allocations

/*
readConsoleInput reads data from a console input buffer.

https://docs.microsoft.com/en-us/windows/console/readconsoleinput
*/
func readConsoleInput(h syscall.Handle, ir *inputRecord) error {
	var err error

	r0, _, e1 := syscall.Syscall6(kernel32.NewProc("ReadConsoleInputW").Addr(),
		4, uintptr(h), uintptr(unsafe.Pointer(ir)), 1, uintptr(unsafe.Pointer(&temp)), 0, 0)

	if int(r0) == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}

	return err
}
