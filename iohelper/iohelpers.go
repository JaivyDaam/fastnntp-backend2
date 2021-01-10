/*
Copyright (c) 2021 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/*
IO Helper routines and structures.

*/
package iohelper

import "io"
import "errors"

var eContinue = errors.New("internal! Continue")

func positive(n int) int {
	if n<0 { return 0 }
	return n
}

/*
Splitter is a writer, that splits a RFC-822 style message into Header and Body.
The algorithm is oppinionated, as it assumes that every '\r' is followed by
an '\n'.

Note that the body starts with a newline ("\n" or "\r\n")!
*/
type Splitter struct{
	Head io.Writer
	Body io.Writer
	
	shift uint8
	toBo bool
}

func (s *Splitter) iWrite(p []byte) (n int, err error) {
	if s.toBo {
		if s.Body==nil { return 0,io.ErrShortWrite }
		return s.Body.Write(p)
	}
	
	shift := uint16(s.shift)
	for i,b := range p {
		shift = (shift<<8) | uint16(b)
		switch shift&0xffff {
		case 0x0a0a,0x0a0d: // "\n\n" | "\n\r"
			s.toBo = true
			p = p[:i]
			break
		}
	}
	s.shift = uint8(shift&0xff)
	
	n,err = 0,nil
	if len(p)>0 { n,err = s.Head.Write(p) }
	return
}
func (s *Splitter) Write(p []byte) (n int, err error) {
	var nn int
	for len(p)>0 {
		nn,err = s.iWrite(p)
		nn = positive(nn)
		n += nn
		p = p[nn:]
		if err==eContinue { continue }
		if err!=nil { break }
	}
	return
}

// Chopper chops off the first leading "\r\n" or "\n"
type Chopper struct {
	Rest io.Writer
	
	consumedCRLF bool
}
func (c *Chopper) Write(p []byte) (n int, err error) {
	if c.consumedCRLF { return c.Rest.Write(p) }
	
	m := len(p)
	for i,b := range p {
		if b=='\r' { continue }
		m = i
		if b=='\n' { m++ }
		c.consumedCRLF = true
		break
	}
	
	n,err = c.Rest.Write(p[m:])
	n += m
	return
}


//
