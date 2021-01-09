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

import "io"
import "github.com/byte-mug/fastnntp"


func handleSTAT(s *server,args [][]byte) error {
	a := &s.gls.AR.Article
	a.MessageId = append(s.gls.ID[:0],getarg(args,1)...)
	a.Group     = getarg(args,2)
	a.Number    = argtoi64(args,3)
	a.HasId     = argtrue(args,4)
	a.HasNum    = argtrue(args,5)
	var ok bool
	if s.rh.ArticleCaps==nil {
		ok = false
	} else {
		ok = s.rh.StatArticle(a)
	}
	return s.b.writeSplit(ok,a.MessageId)
}

func handleGET(s *server,args [][]byte) error {
	a := &s.gls.AR.Article
	a.MessageId = append(s.gls.ID[:0],getarg(args,1)...)
	a.Group     = getarg(args,2)
	a.Number    = argtoi64(args,3)
	a.HasId     = argtrue(args,4)
	a.HasNum    = argtrue(args,5)
	head := argtrue(args,6)
	body := argtrue(args,7)
	
	if s.rh.ArticleCaps==nil || !(head||body) {
		return s.b.writeSplit(false,"")
	}
	
	f := s.rh.GetArticle(a,head,body)
	
	err := s.b.writeSplit(f!=nil,a.MessageId)
	
	if f!=nil {
		w := s.b.writeDot()
		f(w)
		err2 := w.Close()
		w.Release()
		return errsel(err,err2)
	}
	
	return err
}
func (s *server) WriteEntry(num int64, subject, from, date, msgId, refs []byte, lng, lines int64) error {
	return s.b.writeSplit(true,num,subject,from,date,msgId,refs,lng,lines)
}
func handleOVER(s *server,args [][]byte) error {
	a := &s.gls.AR
	a.MessageId  = append(s.gls.ID[:0],getarg(args,1)...)
	a.Group      = getarg(args,2)
	a.Number     = argtoi64(args,3)
	a.HasId      = argtrue(args,4)
	a.HasNum     = argtrue(args,5)
	a.LastNumber = argtoi64(args,6)
	if s.rh.ArticleCaps!=nil {
		f := s.rh.WriteOverview(a)
		if f!=nil { f(s) }
	}
	return s.b.writeSplit(false)
}

func (c *Client) StatArticle(a *fastnntp.Article) bool {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("STAT",a.MessageId,a.Group,a.Number,a.HasId,a.HasNum)
	
	L.resp()
	
	args,_ := c.b.readSplit()
	ok := argtrue(args,0)
	if !a.HasId {
		a.MessageId = append(a.MessageId,getarg(args,1)...)
		a.HasId = true
	}
	return ok
}
func (c *Client) GetArticleInto(a *fastnntp.Article, head, body bool, targ io.Writer) bool {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("GET",a.MessageId,a.Group,a.Number,a.HasId,a.HasNum)
	
	L.resp()
	
	args,_ := c.b.readSplit()
	ok := argtrue(args,0)
	if !a.HasId {
		a.MessageId = append(a.MessageId,getarg(args,1)...)
		a.HasId = true
	}
	if ok {
		r := c.b.readDot()
		defer ConsumeRelease(r)
		io.Copy(targ,r)
	}
	return ok
}

func (c *Client) GetArticle(a *fastnntp.Article, head, body bool) func(w *fastnntp.DotWriter) {
	buf := getMembuf()
	if c.GetArticleInto(a,head,body,buf) {
		return buf.writeToObject
	} else {
		buf.release()
		return nil
	}
}

func (c *Client) WriteOverviewInto(a *fastnntp.ArticleRange, w fastnntp.IOverview) {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("OVER",a.MessageId,a.Group,a.Number,a.HasId,a.HasNum,a.LastNumber)
	
	L.resp()
	
	for {
		args,err := c.b.readSplit()
		if err!=nil { return }
		if !argtrue(args,0) { break }
		
		num     := argtoi64(args,1)
		subject := getarg(args,2)
		from    := getarg(args,3)
		date    := getarg(args,4)
		msgId   := getarg(args,5)
		refs    := getarg(args,6)
		lng     := argtoi64(args,7)
		lines   := argtoi64(args,8)
		w.WriteEntry(num,subject,from,date,msgId,refs,lng,lines)
	}
}
func (c *Client) WriteOverview(ar *fastnntp.ArticleRange) func(w fastnntp.IOverview) {
	return func(w fastnntp.IOverview) { c.WriteOverviewInto(ar,w) }
}

func init() {
	mntpCommands["STAT"] = handleSTAT
	mntpCommands["GET"]  = handleGET
	mntpCommands["OVER"] = handleOVER
}
