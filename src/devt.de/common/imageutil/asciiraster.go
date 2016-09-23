/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package imageutil contains utility function to create/manipulate images.

Asciiraster contains support for raster fonts for images. Using RenderSymbols you
can add text and symbols to an image. By specifying a symbol map containing ASCII art
it is possible to define how each rune should be rendered.
*/
package imageutil

import (
	"bufio"
	"bytes"
	"fmt"
	"image"
	"image/color"
	"unicode"
)

/*
SymbolSpacing defines the spacing in pixels between two symbols
*/
var SymbolSpacing = 1

/*
SpaceSymbolSpacing defines the space in pixels of a space character if the
character is not defined in the font map
*/
var SpaceSymbolSpacing = 5

/*
RenderSymbols renders the symbols in the given string str at the given point p in the
given Image img in the color col using smap as symbol mapping.
*/
func RenderSymbols(img image.Image, p image.Point, str string,
	col color.Color, smap map[rune]string) (image.Image, error) {

	var offset int

	imgc := wrapImage(img)

	// Iterate over the string

	for _, r := range str {

		sym, ok := smap[r]
		if !ok {

			if unicode.IsSpace(r) {

				// If a space character is encountered and it is not defined in the map
				// then just move the offset and continue

				offset += SpaceSymbolSpacing
				continue
			}

			return nil, fmt.Errorf("Cannot find mapping for rune: %q", r)
		}

		sline := 0
		rwidth := 0

		// Go through the symbold line by line

		scanner := bufio.NewScanner(bytes.NewBufferString(sym))
		for scanner.Scan() {

			line := scanner.Text()

			// Set max width of symbol

			if l := len(line); rwidth < l {
				rwidth = l
			}

			soffset := 0

			for _, sr := range line {

				// Draw each pixel

				if !(unicode.IsSpace(sr) || unicode.IsControl(sr)) {
					imgc.Set(offset+soffset+p.X, sline+p.Y, col)
				}

				soffset++
			}

			sline++
		}

		// Advance the offset

		offset += rwidth + SymbolSpacing
	}

	return imgc, nil
}

/*
wrapImage wraps a given image.
*/
func wrapImage(img image.Image) *imageWrapper {
	return &imageWrapper{img, make(map[image.Point]color.Color)}
}

/*
imageWrapper is a wrapper class for images which allows setting single pixels.
*/
type imageWrapper struct {
	image.Image                             // Original image
	pixMap      map[image.Point]color.Color // Modified pixels
}

/*
Set sets the color of the pixel at (x, y).
*/
func (m *imageWrapper) Set(x, y int, c color.Color) {
	m.pixMap[image.Point{x, y}] = c
}

/*
At returns the color of the pixel at (x, y).
*/
func (m *imageWrapper) At(x, y int) color.Color {

	if c := m.pixMap[image.Point{x, y}]; c != nil {
		return c
	}

	return m.Image.At(x, y)
}
