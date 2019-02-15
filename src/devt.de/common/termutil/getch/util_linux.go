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
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
	"unsafe"
)

/*
attachReader attaches a character reader on Windows.
*/
func attachReader() (getch, error) {

	// Detect the current terminal

	keys, err := detectTerminal()

	if err == nil {
		var inFd int

		// Open the tty for reading and signals

		inFd, err = syscall.Open("/dev/tty", syscall.O_RDONLY, 0)

		if err == nil {
			var out *os.File

			// Open a file descriptor on tty to perform ioctl calls

			out, err = os.OpenFile("/dev/tty", syscall.O_WRONLY, 0)

			if err == nil {
				gl := &getchLinux{make(chan os.Signal), inFd, out, keys, syscall.Termios{},
					make(chan *internalKeyEvent), make(chan bool), &sync.WaitGroup{}}

				in := uintptr(gl.inFd)

				// Notify signal channel when I/O is possible
				// Signal: SIGIO	29	I/O now possible (4.2 BSD)

				signal.Notify(gl.sigio, syscall.SIGIO)

				// Set the file status flag for /dev/tty to
				// O_ASYNC Enable generation of signals on the file descriptor
				// O_NONBLOCK Do not wait for input

				err = fcntl(in, syscall.F_SETFL, syscall.O_ASYNC|syscall.O_NONBLOCK)

				if err == nil {

					// Set the process ID that will receive SIGIO signals for events
					// on the file descriptor in.

					err = fcntl(in, syscall.F_SETOWN, syscall.Getpid())

					if err == nil {

						// Read the current serial port settings (terminal attributes)

						err = ioctl(gl.out.Fd(), syscall.TCGETS, &gl.origTios)

						// Reconfigure terminal attributes - see Linux termios

						tios := gl.origTios

						// Unsetting the following input mode flags means:

						// IGNBRK Don't ignore BREAK condition on input
						// BRKINT Breaks don't cause SIGINT to be send and read as \0
						// PARMRK Bytes with parity or framing errors are not marked
						// ISTRIP Do not strip off the eighth bit
						// INLCR Do not translate newline to carriage return on input
						// IGNCR No not ignore carriage return on input
						// ICRNL Do not translate carriage return to newline on input
						// IXON Do not enable XON/XOFF flow control on input

						tios.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK |
							syscall.ISTRIP | syscall.INLCR | syscall.IGNCR |
							syscall.ICRNL | syscall.IXON

						// Unsetting the following local mode flags means:

						// ECHO Do not echo input characters
						// ECHONL Do not echo newline characters
						// ICANON Do not operate in canonical mode - i.e. no line buffering
						// ISIG Do not generate signals when receiving either INTR, QUIT,
						//      SUSP, or DSUSP characters
						// IEXTEN Do not enable implementation-defined input processing

						tios.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ICANON |
							syscall.ISIG | syscall.IEXTEN

						// Unsetting the following control mode flags means:

						// CSIZE Clear any character size mask
						// PARENB Do not enable parity generation on output and
						//        parity checking for input

						tios.Cflag &^= syscall.CSIZE | syscall.PARENB

						// Set character size mask 8 bit

						tios.Cflag |= syscall.CS8

						// Set minimum number of characters for noncanonical read

						tios.Cc[syscall.VMIN] = 1

						// Set timeout in deciseconds for noncanonical read

						tios.Cc[syscall.VTIME] = 0

						err = ioctl(gl.out.Fd(), syscall.TCSETS, &tios)

						if err == nil {

							// All is well we can start listening for events

							go gl.eventListener()

							return gl, nil
						}
					}
				}
			}
		}
	}

	return nil, err
}

// Getch object implementation for linux
// =====================================

/*
getchLinux is the Linux based getch implementation.
*/
type getchLinux struct {
	sigio        chan os.Signal         // Channel to receive input signals
	inFd         int                    // Open file descriptor of /dev/tty (reading / signals)
	out          *os.File               // Open file descriptor on /dev/tty (setting attributes)
	escKeys      []string               // Escape sequences for non-printable keys
	origTios     syscall.Termios        // Original terminal attributes
	keyEventChan chan *internalKeyEvent // Channel for input events
	stopChan     chan bool              // Channel for shutdown event
	wg           *sync.WaitGroup        // Waitgroup for shutdown process
}

var posBuf = make([]byte, 20)

/*
CursorPosition returns the current cursor position.
*/
func (gl *getchLinux) CursorPosition() (int, int, error) {
	var n, x, y int
	var err error

	// Do a normal terminal communication via stdin and stdout

	gl.out.WriteString("\033[6n") // Ask via ANSI escape code

	if n, err = os.Stdin.Read(posBuf); n > 2 && err == nil {

		if res := string(posBuf[:n]); res[:2] == "\033[" {

			// Parse the result

			posStr := strings.Split(res[2:n-1], ";")

			if len(posStr) == 2 {
				if y, err = strconv.Atoi(posStr[0]); err == nil {
					x, err = strconv.Atoi(posStr[1])
				}
			}
		}
	}

	return x - 1, y, err
}

/*
SetCursorPosition sets the current cursor position
*/
func (gl *getchLinux) SetCursorPosition(x, y int) error {

	// Set position via ANSI escape code

	gl.out.WriteString("\033[")
	gl.out.WriteString(fmt.Sprint(y))
	gl.out.WriteString(";")
	gl.out.WriteString(fmt.Sprint(x + 1))
	gl.out.WriteString("H")

	return nil
}

/*
GetKey returns the next key event or an error. This function blocks if no key
event is available.
*/
func (gl *getchLinux) GetKey() *internalKeyEvent {
	return <-gl.keyEventChan
}

/*
GetKeyAsync returns the next key event or an error. This function does not block if no key
event is available - in this case nil is returned.
*/
func (gl *getchLinux) GetKeyAsync() *internalKeyEvent {
	select {
	case e := <-gl.keyEventChan:
		return e
	default:
	}
	return nil
}

/*
Close detaches the character reader.
*/
func (gl *getchLinux) Close() {

	// Send stop command and wait for the eventListener thread to end

	gl.wg.Add(1)

	// Empty all pending keys

	for gl.GetKeyAsync() != nil {
		time.Sleep(10 * time.Millisecond)
	}

	gl.stopChan <- true

	gl.wg.Wait()

	// Reset the original terminal state

	ioctl(gl.out.Fd(), syscall.TCSETS, &gl.origTios)

	// Ignoring errors here since we are closing

	gl.out.Close()
	syscall.Close(gl.inFd)
}

/*
eventListener is a thread which will listen for events.
*/
func (gl *getchLinux) eventListener() {

	// Create input buffer

	buf := make([]byte, 128)

	// Loop until an exit is requested

loop:
	for true {

		select { // Wait for input event or shutdown - block until one is received

		case <-gl.sigio: // Check for input event

			for {
				n, err := syscall.Read(gl.inFd, buf)

				if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
					break // Nothing more to read - wait for more

				} else if err != nil {
					gl.keyEventChan <- &internalKeyEvent{err: err}
				}

				// Either stop or send the event - block if the channel is full

				select {
				case <-gl.stopChan:
					break loop
				case gl.keyEventChan <- gl.buildKeyEvent(buf[:n]):
					continue
				}
			}

		case <-gl.stopChan: // Check for shutdown event
			break loop
		}
	}

	gl.wg.Done()
}

/*
buildKeyEvent builds an internalKeyEvent from a terminal input.
*/
func (gl *getchLinux) buildKeyEvent(buf []byte) *internalKeyEvent {
	ike := &internalKeyEvent{&KeyEvent{}, nil}

	if len(buf) > 0 {
		bufstr := string(buf)

		// Check for escape sequence

		if buf[0] == '\033' && len(buf) > 1 {

			for i, ek := range gl.escKeys {

				if strings.HasPrefix(bufstr, ek) {

					// Found a known termkey - set the code

					termkey := termKey(0xFFFF - i)

					// Codes for non-printable keys

					ike.Rune = 0x00

					switch termkey {

					case termKeyF1:
						ike.Code = KeyF1
					case termKeyF2:
						ike.Code = KeyF2
					case termKeyF3:
						ike.Code = KeyF3
					case termKeyF4:
						ike.Code = KeyF4
					case termKeyF5:
						ike.Code = KeyF5
					case termKeyF6:
						ike.Code = KeyF6
					case termKeyF7:
						ike.Code = KeyF7
					case termKeyF8:
						ike.Code = KeyF8
					case termKeyF9:
						ike.Code = KeyF9
					case termKeyF10:
						ike.Code = KeyF10
					case termKeyF11:
						ike.Code = KeyF11
					case termKeyF12:
						ike.Code = KeyF12
					case termKeyInsert:
						ike.Code = KeyInsert
					case termKeyDelete:
						ike.Code = KeyDelete
					case termKeyHome:
						ike.Code = KeyHome
					case termKeyHome1:
						ike.Code = KeyHome
					case termKeyEnd:
						ike.Code = KeyEnd
					case termKeyEnd1:
						ike.Code = KeyEnd
					case termKeyPgup:
						ike.Code = KeyPgup
					case termKeyPgdn:
						ike.Code = KeyPgdn
					case termKeyArrowUp:
						ike.Code = KeyArrowUp
					case termKeyArrowDown:
						ike.Code = KeyArrowDown
					case termKeyArrowLeft:
						ike.Code = KeyArrowLeft
					case termKeyArrowRight:
						ike.Code = KeyArrowRight
					}
				}
			}

			if ike.KeyEvent.Code == "" {

				// Escape sequence was not detected

				ike.err = &ErrUnknownEscapeSequence{buf}
			}

		} else {

			// Not an escape sequence

			de, deSize := utf8.DecodeRune(buf)

			if len(buf) > deSize {

				// There was more than one character - append the raw input buffer

				ike.RawBuf = make([]byte, len(buf))
				copy(ike.RawBuf, buf)
			}

			// Check for printable character

			ike.Rune = de

			switch de {

			case 'a': // Letters:
				ike.Code = KeyA
			case 'b':
				ike.Code = KeyB
			case 'c':
				ike.Code = KeyC
			case 'd':
				ike.Code = KeyD
			case 'e':
				ike.Code = KeyE
			case 'f':
				ike.Code = KeyF
			case 'g':
				ike.Code = KeyG
			case 'h':
				ike.Code = KeyH
			case 'i':
				ike.Code = KeyI
			case 'j':
				ike.Code = KeyJ
			case 'k':
				ike.Code = KeyK
			case 'l':
				ike.Code = KeyL
			case 'm':
				ike.Code = KeyM
			case 'n':
				ike.Code = KeyN
			case 'o':
				ike.Code = KeyO
			case 'p':
				ike.Code = KeyP
			case 'q':
				ike.Code = KeyQ
			case 'r':
				ike.Code = KeyR
			case 's':
				ike.Code = KeyS
			case 't':
				ike.Code = KeyT
			case 'u':
				ike.Code = KeyU
			case 'v':
				ike.Code = KeyV
			case 'w':
				ike.Code = KeyW
			case 'x':
				ike.Code = KeyX
			case 'y':
				ike.Code = KeyY
			case 'z':
				ike.Code = KeyZ

			case '0': // Numbers
				ike.Code = Key0
			case '1':
				ike.Code = Key1
			case '2':
				ike.Code = Key2
			case '3':
				ike.Code = Key3
			case '4':
				ike.Code = Key4
			case '5':
				ike.Code = Key5
			case '6':
				ike.Code = Key6
			case '7':
				ike.Code = Key7
			case '8':
				ike.Code = Key8
			case '9':
				ike.Code = Key9

				/*

				   The following symbols are returned as an Key_UNKNOWN
				   as they depend on the actual keyboard layout

				   			case '`': // Symbols
				   				ike.Code = KeyBacktick
				   			case '-':
				   				ike.Code = KeyMinus
				   			case '=':
				   				ike.Code = KeyEqual
				   			case '\\':
				   				ike.Code = KeyBackslash
				   			case ',':
				   				ike.Code = KeyComma
				   			case '.':
				   				ike.Code = KeyDot
				   			case '/':
				   				ike.Code = KeySlash
				   			case ';':
				   				ike.Code = KeySemiColon
				   			case '\'':
				   				ike.Code = KeyQuote
				   			case '#':
				   				ike.Code = KeyHash
				   			case ']':
				   				ike.Code = KeyBracketOpen
				   			case '[':
				   				ike.Code = KeyBracketClose
				*/

			case 'A': // Letters:
				ike.Code = KeyA
				ike.Shift = true
			case 'B':
				ike.Code = KeyB
				ike.Shift = true
			case 'C':
				ike.Code = KeyC
				ike.Shift = true
			case 'D':
				ike.Code = KeyD
				ike.Shift = true
			case 'E':
				ike.Code = KeyE
				ike.Shift = true
			case 'F':
				ike.Code = KeyF
				ike.Shift = true
			case 'G':
				ike.Code = KeyG
				ike.Shift = true
			case 'H':
				ike.Code = KeyH
				ike.Shift = true
			case 'I':
				ike.Code = KeyI
				ike.Shift = true
			case 'J':
				ike.Code = KeyJ
				ike.Shift = true
			case 'K':
				ike.Code = KeyK
				ike.Shift = true
			case 'L':
				ike.Code = KeyL
				ike.Shift = true
			case 'M':
				ike.Code = KeyM
				ike.Shift = true
			case 'N':
				ike.Code = KeyN
				ike.Shift = true
			case 'O':
				ike.Code = KeyO
				ike.Shift = true
			case 'P':
				ike.Code = KeyP
				ike.Shift = true
			case 'Q':
				ike.Code = KeyQ
				ike.Shift = true
			case 'R':
				ike.Code = KeyR
				ike.Shift = true
			case 'S':
				ike.Code = KeyS
				ike.Shift = true
			case 'T':
				ike.Code = KeyT
				ike.Shift = true
			case 'U':
				ike.Code = KeyU
				ike.Shift = true
			case 'V':
				ike.Code = KeyV
				ike.Shift = true
			case 'W':
				ike.Code = KeyW
				ike.Shift = true
			case 'X':
				ike.Code = KeyX
				ike.Shift = true
			case 'Y':
				ike.Code = KeyY
				ike.Shift = true
			case 'Z':
				ike.Code = KeyZ
				ike.Shift = true

			case 0x01:
				ike.Code = KeyA
				ike.Ctrl = true
			case 0x02:
				ike.Code = KeyB
				ike.Ctrl = true
			case 0x03:
				ike.Code = KeyC
				ike.Ctrl = true
			case 0x04:
				ike.Code = KeyD
				ike.Ctrl = true
			case 0x05:
				ike.Code = KeyE
				ike.Ctrl = true
			case 0x06:
				ike.Code = KeyF
				ike.Ctrl = true
			case 0x07:
				ike.Code = KeyG
				ike.Ctrl = true
			case 0x08:
				ike.Code = KeyH
				ike.Ctrl = true
			case 0x09:
				ike.Code = KeyI
				ike.Ctrl = true
			case 0x0A:
				ike.Code = KeyJ
				ike.Ctrl = true
			case 0x0B:
				ike.Code = KeyK
				ike.Ctrl = true
			case 0x0C:
				ike.Code = KeyL
				ike.Ctrl = true
			case 0x0D:
				ike.Code = KeyM
				ike.Ctrl = true
			case 0x0E:
				ike.Code = KeyN
				ike.Ctrl = true
			case 0x0F:
				ike.Code = KeyO
				ike.Ctrl = true
			case 0x10:
				ike.Code = KeyP
				ike.Ctrl = true
			case 0x11:
				ike.Code = KeyQ
				ike.Ctrl = true
			case 0x12:
				ike.Code = KeyR
				ike.Ctrl = true
			case 0x13:
				ike.Code = KeyS
				ike.Ctrl = true
			case 0x14:
				ike.Code = KeyT
				ike.Ctrl = true
			case 0x15:
				ike.Code = KeyU
				ike.Ctrl = true
			case 0x16:
				ike.Code = KeyV
				ike.Ctrl = true
			case 0x17:
				ike.Code = KeyW
				ike.Ctrl = true
			case 0x18:
				ike.Code = KeyX
				ike.Ctrl = true
			case 0x19:
				ike.Code = KeyY
				ike.Ctrl = true
			case 0x1a:
				ike.Code = KeyZ
				ike.Ctrl = true
			default:

				ike.Code = KeyUnknown
			}

			// Check for non-printable keys
			// Note: KeyCommand was not recognised

			switch de {

			case '\t':
				ike.Code = KeyTab
				ike.Rune = 0x00
			case '\r':
				ike.Code = KeyEnter
				ike.Rune = 0x00
			case 127:
				ike.Code = KeyBackspace
				ike.Rune = 0x00
			case 27:
				ike.Code = KeyEsc
				ike.Rune = 0x00
			}

		}
	}

	return ike
}

// Escape sequences for common terminals
// =====================================

/*
termKey models a linux terminal key
*/
type termKey uint16

/*
Recognized escape sequences
*/
const (
	termKeyF1 termKey = 0xFFFF - iota
	termKeyF2
	termKeyF3
	termKeyF4
	termKeyF5
	termKeyF6
	termKeyF7
	termKeyF8
	termKeyF9
	termKeyF10
	termKeyF11
	termKeyF12
	termKeyInsert
	termKeyDelete
	termKeyHome
	termKeyHome1 // Alternative key
	termKeyEnd
	termKeyEnd1 // Alternative key
	termKeyPgup
	termKeyPgdn
	termKeyArrowUp
	termKeyArrowUp1 // Alternative key
	termKeyArrowDown
	termKeyArrowDown1 // Alternative key
	termKeyArrowLeft
	termKeyArrowLeft1 // Alternative key
	termKeyArrowRight
	termKeyArrowRight1 // Alternative key
)

/*
Mappings from terminal type to escape sequence for a particular termKey (see above).
*/
var (
	etermKeys = []string{
		"\x1b[11~", "\x1b[12~", "\x1b[13~", "\x1b[14~", "\x1b[15~",
		"\x1b[17~", "\x1b[18~", "\x1b[19~", "\x1b[20~", "\x1b[21~",
		"\x1b[23~", "\x1b[24~", "\x1b[2~", "\x1b[3~", "\x1b[7~",
		"\x1b[7~", "\x1b[8~", "\x1b[8~", "\x1b[5~", "\x1b[6~",
		"\x1b[A", "\x1bOA", "\x1b[B", "\x1bOB", "\x1b[D", "\x1bOD",
		"\x1b[C", "\x1bOC",
	}
	screenKeys = []string{
		"\x1bOP", "\x1bOQ", "\x1bOR", "\x1bOS", "\x1b[15~", "\x1b[17~",
		"\x1b[18~", "\x1b[19~", "\x1b[20~", "\x1b[21~", "\x1b[23~",
		"\x1b[24~", "\x1b[2~", "\x1b[3~", "\x1b[1~", "\x1b[1~", "\x1b[4~",
		"\x1b[4~", "\x1b[5~", "\x1b[6~", "\x1b[A", "\x1bOA", "\x1b[B",
		"\x1bOB", "\x1b[D", "\x1bOD", "\x1b[C", "\x1bOC",
	}
	xtermKeys = []string{
		"\x1bOP", "\x1bOQ", "\x1bOR", "\x1bOS", "\x1b[15~", "\x1b[17~",
		"\x1b[18~", "\x1b[19~", "\x1b[20~", "\x1b[21~", "\x1b[23~",
		"\x1b[24~", "\x1b[2~", "\x1b[3~", "\x1b[H", "\x1bOH", "\x1b[F",
		"\x1bOF", "\x1b[5~", "\x1b[6~", "\x1b[A", "\x1bOA", "\x1b[B",
		"\x1bOB", "\x1b[D", "\x1bOD", "\x1b[C", "\x1bOC",
	}
	rxvtKeys = []string{
		"\x1b[11~", "\x1b[12~", "\x1b[13~", "\x1b[14~", "\x1b[15~",
		"\x1b[17~", "\x1b[18~", "\x1b[19~", "\x1b[20~", "\x1b[21~",
		"\x1b[23~", "\x1b[24~", "\x1b[2~", "\x1b[3~", "\x1b[7~",
		"\x1b[7~", "\x1b[8~", "\x1b[8~", "\x1b[5~", "\x1b[6~",
		"\x1b[A", "\x1bOA", "\x1b[B", "\x1bOB", "\x1b[D", "\x1bOD",
		"\x1b[C", "\x1bOC",
	}
	linuxKeys = []string{
		"\x1b[[A", "\x1b[[B", "\x1b[[C", "\x1b[[D", "\x1b[[E",
		"\x1b[17~", "\x1b[18~", "\x1b[19~", "\x1b[20~", "\x1b[21~",
		"\x1b[23~", "\x1b[24~", "\x1b[2~", "\x1b[3~", "\x1b[1~",
		"\x1b[1~", "\x1b[4~", "\x1b[4~", "\x1b[5~", "\x1b[6~",
		"\x1b[A", "\x1bOA", "\x1b[B", "\x1bOB", "\x1b[D", "\x1bOD",
		"\x1b[C", "\x1bOC",
	}

	DefaultTermMappings = []struct {
		name string
		keys []string
	}{
		{"Eterm", etermKeys},
		{"screen", screenKeys},
		{"xterm", xtermKeys},
		{"xterm-256color", xtermKeys},
		{"rxvt-unicode", rxvtKeys},
		{"rxvt-256color", rxvtKeys},
		{"linux", linuxKeys},
	}

	CompatibilityTermMappings = []struct {
		namePart string
		keys     []string
	}{
		{"xterm", xtermKeys},
		{"rxvt", rxvtKeys},
		{"linux", linuxKeys},
		{"Eterm", etermKeys},
		{"screen", screenKeys},
		{"cygwin", xtermKeys},
		{"st", xtermKeys},
	}
)

/*
detectTerminal detects the current terminal and returns the appropriate escape
sequence mapping for termKeys.
*/
func detectTerminal() ([]string, error) {
	var keys []string

	name := os.Getenv("TERM")

	if name == "" {
		return nil, fmt.Errorf("Cannot determine terminal - TERM environment variable not set")
	}

	err := fmt.Errorf("Terminal %s is not supported", name)

	// Look for the right mapping for the current terminal

	for _, t := range DefaultTermMappings {
		if name == t.name {
			keys = t.keys
			err = nil
			break
		}
	}

	if keys == nil {

		// Try compatibility mappings if there was no direct match

		for _, t := range CompatibilityTermMappings {
			if strings.Contains(name, t.namePart) {
				keys = t.keys
				err = nil
				break
			}
		}
	}

	return keys, err
}

// Util functions
// ==============

/*
fcntl does an OS system call of the same name to manipulate a file descriptor.
*/
func fcntl(fd uintptr, cmd int, arg int) error {
	var err error

	_, _, e := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(cmd),
		uintptr(arg))

	// Follow convention of syscall.Errno to convert from Errno to error

	if e != 0 {
		err = os.NewSyscallError("SYS_FCNTL", e)
	}

	return err
}

/*
ioctl does an OS system call of the same name to manipulates the underlying
device parameters of special files such as terminals.
*/
func ioctl(fd uintptr, cmd int, termios *syscall.Termios) error {
	var err error

	r, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(cmd),
		uintptr(unsafe.Pointer(termios)))

	if r != 0 {
		err = os.NewSyscallError("SYS_IOCTL", e)
	}

	return err
}
