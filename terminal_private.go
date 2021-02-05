//
// ORIGINAL PACKAGE
// ( https://github.com/thoas/bokchoy )
//
//     Copyright © 2019. All rights reserved.
//     Author: Florent Messa
//     Contacts: florent.messa@gmail.com, https://github.com/thoas
//     License: https://opensource.org/licenses/MIT
//
// HAS BEEN FORKED, HIGHLY MODIFIED AND NOW IS AVAILABLE AS
// ( https://github.com/qioalice/bokchoy )
//
//     Copyright © 2020. All rights reserved.
//     Author: Ilya Stroy.
//     Contacts: qioalice@gmail.com, https://github.com/qioalice
//     License: https://opensource.org/licenses/MIT
//

package bokchoy

// Ported from Goji's middleware, source:
// https://github.com/zenazn/goji/tree/master/web/middleware

import (
	"bytes"
	"fmt"
	"os"
)

// Color is a terminal color representation.
type Color []byte

//goland:noinspection GoUnusedGlobalVariable
var (
	// Normal colors
	colorBlack   = Color{'\033', '[', '3', '0', 'm'}
	colorRed     = Color{'\033', '[', '3', '1', 'm'}
	colorGreen   = Color{'\033', '[', '3', '2', 'm'}
	colorYellow  = Color{'\033', '[', '3', '3', 'm'}
	colorBlue    = Color{'\033', '[', '3', '4', 'm'}
	colorMagenta = Color{'\033', '[', '3', '5', 'm'}
	colorCyan    = Color{'\033', '[', '3', '6', 'm'}
	colorWhite   = Color{'\033', '[', '3', '7', 'm'}

	// Bright colors
	colorBrightBlack   = Color{'\033', '[', '3', '0', ';', '1', 'm'}
	colorBrightRed     = Color{'\033', '[', '3', '1', ';', '1', 'm'}
	colorBrightGreen   = Color{'\033', '[', '3', '2', ';', '1', 'm'}
	colorBrightYellow  = Color{'\033', '[', '3', '3', ';', '1', 'm'}
	colorBrightBlue    = Color{'\033', '[', '3', '4', ';', '1', 'm'}
	colorBrightMagenta = Color{'\033', '[', '3', '5', ';', '1', 'm'}
	colorBrightCyan    = Color{'\033', '[', '3', '6', ';', '1', 'm'}
	colorBrightWhite   = Color{'\033', '[', '3', '7', ';', '1', 'm'}

	colorReset = Color{'\033', '[', '0', 'm'}
)

var isTTY bool

func initIsTTY() {
	// This is sort of cheating: if stdout is a character device, we assume
	// that means it's a TTY. Unfortunately, there are many non-TTY
	// character devices, but fortunately stdout is rarely set to any of
	// them.
	//
	// We could solve this properly by pulling in a dependency on
	// code.google.com/p/go.crypto/ssh/terminal, for instance, but as a
	// heuristic for whether to print in color or in black-and-white, I'd
	// really rather not.
	fi, err := os.Stdout.Stat()
	if err == nil {
		m := os.ModeDevice | os.ModeCharDevice
		isTTY = fi.Mode()&m == m
	}
}

// ColorWriter is a bytes buffer with color.
type colorWriter struct {
	*bytes.Buffer
	color Color
}

// WithColor returns a new ColorWriter with a new color.
func (c colorWriter) WithColor(color Color) *colorWriter {
	c.color = color
	return &c
}

// newColorWriter initializes a new ColorWriter.
func newColorWriter(color Color) *colorWriter {
	return &colorWriter{
		&bytes.Buffer{},
		color,
	}
}

// Write writes an output to stdout.
func (c *colorWriter) Write(s string, args ...interface{}) {
	if isTTY && c.color != nil {
		c.Buffer.Write(c.color)
	}

	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(c.Buffer, s, args...)
	if isTTY && c.color != nil {
		c.Buffer.Write(colorReset)
	}
}
