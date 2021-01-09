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
Minimalistic Pipelined NNTP client.
*/
package nntpclient

import "io"
import "github.com/byte-mug/fastnntp"
import "regexp"
import "fmt"

var respline_Rex = regexp.MustCompile(`^(\d\d\d)\s+(.*)`)
var listActive_Rex = regexp.MustCompile(`^(\S+)\s+(\d+)\s+(\d+)\s+(.)`)
var listNewsgoups_Rex = regexp.MustCompile(`^(\S+)\s+([^\r\n]+)`)

var group_Rex = regexp.MustCompile(`(\d+)\s+(\d+)\s+(\d+)`)

var xover_Rex = regexp.MustCompile(`^([^\r\n\t]*)\t([^\r\n\t]*)\t([^\r\n\t]*)\t([^\r\n\t]*)\t([^\r\n\t]*)\t([^\r\n\t]*)\t([^\r\n\t]*)\t([^\r\n\t]*)`)
var listgrp_Rex = regexp.MustCompile(`^(\d+)`)

/* 22X 0|n message-id   Body follows*/
var article_Rex = regexp.MustCompile(`^\d+\s+(\S+)`)

type ErrNum int
const (
	EMalformedResponse ErrNum = iota
	EInvalidArgs
)
func (e ErrNum) Error() (s string) {
	switch e{
	case EMalformedResponse: s = "Malformed Response"
	case EInvalidArgs: s = "Invalid arguments"
	default: s = "Unknown ErrNum"
	}
	return
}

var eMalformedResponse error = EMalformedResponse

func nvatoi64(a []byte) (i int64) {
	for _,b := range a {
		i = (i*10)+int64(b-'0')
	}
	return
}
func nvatoi(a []byte) (i int) {
	for _,b := range a {
		i = (i*10)+int(b-'0')
	}
	return
}

type UnexpectedCode struct {
	Expected string
	Got string
}
func (u *UnexpectedCode) Error() string {
	return fmt.Sprintf("Expected %s got %s",u.Expected,u.Got)
}

func expectReturn(perr *error,exp string,code []byte) {
	if (*perr)==nil && string(code)!=exp {
		*perr = &UnexpectedCode{exp,string(code)}
	}
}
func errsel(errs ...error) error {
	for _,err := range errs { if err!=nil { return err } }
	return nil
}

func concatb(pref []byte, rest ...interface{}) []byte {
	for _,elem := range rest {
		switch v := elem.(type) {
		case []byte: pref = append(pref,v...)
		case string: pref = append(pref,v...)
		}
	}
	return pref
}

func FinishDR(dr *fastnntp.DotReader) {
	dr.Consume()
	dr.Release()
}
func finishDW(dw *fastnntp.DotWriter) {
	dw.Close()
	dw.Release()
}

type Connection struct {
	pl lPipeline
	rwc io.ReadWriteCloser
	r *fastnntp.Reader
	
	lineBuffer []byte
	outBuffer  []byte
}
func (c *Connection) init() {
	c.r = fastnntp.AcquireReader().Init(c.rwc)
	c.lineBuffer = make([]byte,0,1<<10)
	c.outBuffer  = make([]byte,0,1<<10)
}
func (c *Connection) release() {
	if c.r!=nil { c.r.Release(); c.r = nil }
	if c.rwc!=nil { c.rwc.Close(); c.rwc = nil }
}
func (c *Connection) readResponse() ([]byte,[]byte,error) {
	line,err := c.r.ReadLineB(c.lineBuffer)
	if err!=nil { return nil,nil,err }
	elem := respline_Rex.FindSubmatch(line)
	if len(elem)==0 { return nil,nil,eMalformedResponse }
	return elem[1],elem[2],nil
}
func NewConnection(rwc io.ReadWriteCloser) (c *Connection,err error) {
	var code []byte
	c = new(Connection)
	c.rwc = rwc
	c.init()
	code,_,err = c.readResponse()
	expectReturn(&err,"200",code)
	return
}

// Closes the connection.
// Handle with care: Any further attempt to perform a request panics
// after the connection is closed using Close().
func (c *Connection) Close() (err error) {
	defer c.pl.exclusive()()
	
	var code []byte
	_,err2 := c.rwc.Write([]byte("QUIT\r\n"))
	code,_,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,"205",code)
	c.release()
	return
}

// List all newsgroups.
func (c *Connection) ListActive(f func(group []byte, high, low int64, status byte)) (err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	var code []byte
	_,err2 := c.rwc.Write([]byte("LIST ACTIVE\r\n"))
	
	L.resp()
	
	code,_,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,"215",code)
	if err!=nil { return }
	dr := c.r.DotReader()
	defer FinishDR(dr)
	lr := fastnntp.AcquireReader().Init(dr)
	
	for {
		line,err3 := lr.ReadLineB(c.lineBuffer)
		if err3!=nil { break }
		elem := listActive_Rex.FindSubmatch(line)
		if len(elem)==0 { continue }
		group := elem[1]
		high := nvatoi64(elem[2])
		low := nvatoi64(elem[3])
		status := elem[4][0]
		f(group,high,low,status)
	}
	return
}

// List all newsgroups.
func (c *Connection) ListNewsgroups(f func(group []byte, descr []byte)) (err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	
	var code []byte
	_,err2 := c.rwc.Write([]byte("LIST NEWSGROUPS\r\n"))
	
	L.resp()
	
	code,_,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,"215",code)
	if err!=nil { return }
	dr := c.r.DotReader()
	defer FinishDR(dr)
	lr := fastnntp.AcquireReader().Init(dr)
	
	for {
		line,err3 := lr.ReadLineB(c.lineBuffer)
		if err3!=nil { break }
		elem := listNewsgoups_Rex.FindSubmatch(line)
		if len(elem)==0 { continue }
		group := elem[1]
		descr := elem[2]
		f(group,descr)
	}
	return
}

// Selects a newsgroup.
func (c *Connection) Group(grp []byte) (num,low,high int64,err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	var code []byte
	var data []byte
	_,err2 := c.rwc.Write(concatb(c.outBuffer,"GROUP ",grp,"\r\n"))
	
	L.resp()
	
	code,data,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,"211",code)
	elems := group_Rex.FindSubmatch(data)
	if len(elems)>0 {
		num = nvatoi64(elems[1])
		low = nvatoi64(elems[2])
		high = nvatoi64(elems[3])
	}
	//fmt.Printf("GROUP %s -> %s \n",grp,data)
	return
}

type XoverResp func(num int64, subject, from, date, msgId, refs []byte, lng, lines int64)

// Performs the XOVER command.
func (c *Connection) Xover(f XoverResp,msgno []byte) (err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	var code []byte
	_,err2 := c.rwc.Write(concatb(c.outBuffer,"XOVER ",msgno,"\r\n"))
	
	L.resp()
	
	code,_,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,"224",code)
	if err!=nil { return }
	dr := c.r.DotReader()
	defer FinishDR(dr)
	lr := fastnntp.AcquireReader().Init(dr)
	
	for {
		line,err3 := lr.ReadLineB(c.lineBuffer)
		if err3!=nil { break }
		elem := xover_Rex.FindSubmatch(line)
		if len(elem)==0 { continue }
		num := nvatoi64(elem[1])
		subject := elem[2]
		from := elem[3]
		date := elem[4]
		msgId := elem[5]
		refs := elem[6]
		lng := nvatoi64(elem[7])
		lines := nvatoi64(elem[8])
		f(num,subject,from,date,msgId,refs,lng,lines)
	}
	return
}

// EXPERIMENTAL, MAY CHANGE!
// Performs the LISTGROUP command.
func (c *Connection) Listgroup(f func(num int64),args interface{}) (err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	var code []byte
	_,err2 := c.rwc.Write(concatb(c.outBuffer,"LISTGROUP ",args,"\r\n"))
	
	L.resp()
	
	code,_,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,"211",code)
	if err!=nil { return }
	dr := c.r.DotReader()
	defer FinishDR(dr)
	lr := fastnntp.AcquireReader().Init(dr)
	
	for {
		line,err3 := lr.ReadLineB(c.lineBuffer)
		if err3!=nil { break }
		elem := listgrp_Rex.FindSubmatch(line)
		if len(elem)==0 { continue }
		num := nvatoi64(elem[1])
		f(num)
	}
	return
}

// Submits one of the following commands: ARTICLE, HEAD, BODY or STAT.
func (c *Connection) Article(args []byte, head, body bool, f func(dr *fastnntp.DotReader, msgid []byte)) (err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	var code,data []byte
	cmd := ""
	resp := ""
	reader := true
	if head && body {
		cmd,resp = "ARTICLE","220"
	} else if head {
		cmd,resp = "HEAD","221"
	} else if body {
		cmd,resp = "BODY","222"
	} else {
		cmd,resp,reader = "STAT","223",false
	}
	_,err2 := c.rwc.Write(concatb(c.outBuffer,cmd," ",args,"\r\n"))
	
	L.resp()
	
	code,data,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,resp,code)
	var dr *fastnntp.DotReader
	var msgid []byte
	if err==nil && reader { dr = c.r.DotReader(); defer FinishDR(dr) }
	elems := article_Rex.FindSubmatch(data)
	if len(elems)!=0 { msgid = elems[1] }
	f(dr,msgid)
	return
}

// Submits one of the following commands: ARTICLE, HEAD, BODY or STAT.
func (c *Connection) ArticleInto(args []byte, head, body bool, t io.Writer) (msgid []byte, err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	var code,data []byte
	cmd := ""
	resp := ""
	reader := true
	if head && body {
		cmd,resp = "ARTICLE","220"
	} else if head {
		cmd,resp = "HEAD","221"
	} else if body {
		cmd,resp = "BODY","222"
	} else {
		cmd,resp,reader = "STAT","223",false
	}
	_,err2 := c.rwc.Write(concatb(c.outBuffer,cmd," ",args,"\r\n"))
	
	L.resp()
	
	code,data,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,resp,code)
	var dr *fastnntp.DotReader
	
	if err==nil && reader { dr = c.r.DotReader(); defer FinishDR(dr) }
	elems := article_Rex.FindSubmatch(data)
	if len(elems)!=0 { msgid = elems[1] }
	if dr!=nil { io.Copy(t,dr) }
	return
}

type Posting func(dw *fastnntp.DotWriter)

func (c *Connection) PostOrIhave(msgid []byte,f Posting) (err error) {
	defer c.pl.exclusive()()
	
	var code,req []byte
	resp := ""
	subp := ""
	if len(msgid)==0 {
		req,resp,subp = append(c.outBuffer,"POST\r\n"...),"340","240"
	} else {
		req,resp,subp = concatb(c.outBuffer,"IHAVE ",msgid,"\r\n"),"335","235"
	}
	_,err2 := c.rwc.Write(req)
	code,_,err = c.readResponse()
	err = errsel(err,err2)
	expectReturn(&err,resp,code)
	if err!=nil { return }
	dw := fastnntp.AcquireDotWriter()
	dw.Reset(c.rwc)
	f(dw)
	finishDW(dw)
	code,_,err = c.readResponse()
	expectReturn(&err,subp,code)
	return
}

const (
	CheckOK = 238 // Send article to be transferred
	CheckNotPossible = 431 // Transfer not possible; try again later
	CheckNotWanted = 438 // Article not wanted
	TakethisOK = 239
	TakethisRejected = 439
)

// Checks whether or not an article is wanted.
// Requires RFC-4644 (The STREAMING Extension)
func (c *Connection) Check(msgid []byte) (code int,err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	var bcode []byte
	_,err2 := c.rwc.Write(concatb(c.outBuffer,"CHECK ",msgid,"\r\n"))
	
	L.resp()
	
	bcode,_,err = c.readResponse()
	err = errsel(err,err2)
	code = nvatoi(bcode)
	return
}

// Posts an article using the TAKETHIS command.
// Requires RFC-4644 (The STREAMING Extension)
func (c *Connection) Takethis(msgid []byte,f Posting) (code int,err error) {
	L := c.pl.ackquire(); defer L.release(); L.req()
	
	var bcode []byte
	_,err = c.rwc.Write(concatb(c.outBuffer,"TAKETHIS ",msgid,"\r\n"))
	if err!=nil { return }
	dw := fastnntp.AcquireDotWriter()
	dw.Reset(c.rwc)
	f(dw)
	finishDW(dw)
	
	L.resp()
	
	bcode,_,err = c.readResponse()
	code = nvatoi(bcode)
	return
}

//
