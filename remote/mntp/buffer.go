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


package mntpc


import (
	"io"
	"sync"
	"bytes"
	"github.com/byte-mug/fastnntp"
)


type membuffer struct {
	bytes.Buffer
	writeToObject func(w *fastnntp.DotWriter)
}

var pool_membuffer = sync.Pool{ New: func() interface{} {
	return new(membuffer)
} }

func getMembuf() *membuffer {
	m := pool_membuffer.Get().(*membuffer)
	if m.writeToObject==nil { m.writeToObject = m.writeTo }
	return m
}

func (m *membuffer) release() {
	m.Reset()
	pool_membuffer.Put(m)
}
func (m *membuffer) writeTo(w *fastnntp.DotWriter) {
	io.Copy(w,m)
	m.release()
}


