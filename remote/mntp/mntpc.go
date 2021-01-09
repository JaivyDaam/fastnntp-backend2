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
MNTP: Minimalistic News Transfer Protocol.
MNTP is a wire protocol that is kind of a sibling to NNTP. It is meant to offer
an efficient means of communication between the Frontend server and the backend.
*/
package mntpc

import (
	"io"
	pipeline "net/textproto" // For Pipeline
	"github.com/byte-mug/fastnntp"
	"sync"
	"fmt"
)

func errsel(errs ...error) error {
	for _,err := range errs { if err!=nil { return err } }
	return nil
}

var strmap_bool = map[bool]byte {
	false: '0',
	true: '1',
}

func lsplit(line []byte,targ [][]byte) [][]byte {
	i,j,n := 0,0,len(line)
	for i<n {
		switch line[i] {
		case '\r','\n':
			targ = append(targ,line[j:i])
			return targ
		case '\t':
			targ = append(targ,line[j:i])
			j = i+1
		}
		i++
	}
	return targ
}
func ljoin(targ []byte, elems... interface{}) []byte {
	for i,e := range elems {
		if i!=0 { targ = append(targ,'\t') }
		switch v := e.(type) {
		case []byte: targ = append(targ,v...)
		case string: targ = append(targ,v...)
		case bool: targ = append(targ,strmap_bool[v])
		case int64: if v<0 { v = 0 }; targ = append(targ,fmt.Sprint(v)...)
		}
	}
	return append(targ,'\n')
}

func getarg(args [][]byte,i int) []byte {
	if len(args)<=i { return args[i] }
	return nil
}

func argtrue(args [][]byte,i int) bool { return string(getarg(args,i))=="1" }

func argtoi64(args [][]byte,j int) (i int64) {
	a := getarg(args,j)
	for _,b := range a {
		i = (i*10)+int64(b-'0')
	}
	return
}

type iobuffer struct {
	c io.ReadWriteCloser
	r *fastnntp.Reader
	
	lineBuffer  []byte
	outBuffer   []byte
	splitbuf    [][]byte
}
var pool_iobuffer = sync.Pool{ New: func() interface{} {
	return &iobuffer{
		lineBuffer : make([]byte,0,1<<13),
		outBuffer  : make([]byte,0,1<<13),
		splitbuf   : make([][]byte,0,10),
	}
} }


func (i *iobuffer) free() {
	if i==nil { return }
	i.r.Release()
	i.c = nil
	i.r = nil
	pool_iobuffer.Put(i)
}
func wrapiob(c io.ReadWriteCloser) (i *iobuffer) {
	i = pool_iobuffer.Get().(*iobuffer)
	i.c = c
	i.r = fastnntp.AcquireReader().Init(c)
	return i
}

func (i *iobuffer) readLine() ([]byte,error) {
	if i==nil { return nil,io.EOF }
	return i.r.ReadLineB(i.lineBuffer)
}
func (i *iobuffer) readSplit() ([][]byte,error) {
	line,err := i.readLine()
	return lsplit(line,i.splitbuf),err
}
func (i *iobuffer) writeSplit(args... interface{}) error {
	_,err := i.c.Write(ljoin(i.outBuffer,args...))
	return err
}
func (i *iobuffer) readDot() (r *fastnntp.DotReader) {
	r = i.r.DotReader()
	return
}
func (i *iobuffer) writeDot() (w *fastnntp.DotWriter) {
	w = fastnntp.AcquireDotWriter()
	w.Reset(i.c)
	return
}

func ConsumeRelease(r *fastnntp.DotReader) {
	r.Consume()
	r.Release()
}

type Client struct {
	p pipeline.Pipeline
	b *iobuffer
}
type lock struct {
	c *Client
	id,state uint
}
func (l *lock) upd(state uint) {
	if state>2 { state = 2 }
	for l.state<state {
		l.state++
		switch l.state {
		case 1:
			l.c.p.EndRequest(l.id)
			l.c.p.StartResponse(l.id)
		case 2:
			l.c.p.EndResponse(l.id)
		}
	}
}
func (l *lock) resp() { l.upd(1) }
func (l *lock) release() { l.upd(2) }
func (c *Client) req() *lock {
	id := c.p.Next()
	c.p.StartRequest(id)
	return &lock{c,id,0}
}
func NewClient(conn io.ReadWriteCloser) (c *Client) {
	c = new(Client)
	c.b = wrapiob(conn)
	return
}

type servergls struct {
	ID [128]byte
	AR fastnntp.ArticleRange
}

type server struct {
	b *iobuffer
	rh fastnntp.Handler
	gls servergls
}

func ServeConn(conn io.ReadWriteCloser, rh fastnntp.Handler) {
	s := new(server)
	s.b = wrapiob(conn)
	s.rh = rh
	s.serve()
}

type handlerFunc func(s *server,args [][]byte) error
var mntpCommands = make(map[string]handlerFunc)
var noop = []byte("\n")
func (s *server) serve() {
	for {
		args,err := s.b.readSplit()
		if err!=nil { return }
		if len(args)==0 { s.b.c.Write(noop); continue }
		// MNTP is case sensitive!
		handler,ok := mntpCommands[string(args[0])]
		if !ok  { s.b.c.Write(noop); continue }
		err = handler(s,args)
		if err!=nil { return }
	}
}

