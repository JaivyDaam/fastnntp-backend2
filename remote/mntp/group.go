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

func handleGROUP(s *server,args [][]byte) error {
	var ok bool
	grp := &s.gls.GR
	grp.Group = getarg(args,1)
	if s.rh.GroupCaps==nil {
		ok = false
	} else {
		ok = s.rh.GetGroup(grp)
	}
	return s.b.writeSplit(ok, grp.Number, grp.Low, grp.High)
}

func handleSOVER(s *server,args [][]byte) error {
	grp := &s.gls.GR
	var first,last int64
	grp.Group = getarg(args,1)
	first = argtoi64(args,2)
	last = argtoi64(args,3)
	if s.rh.GroupCaps==nil {
		return s.b.writeSplit(false)
	} else {
		s.b.writeSplit(true)
		w := s.b.writeDot()
		s.rh.ListGroup(grp, w, first, last)
		err2 := w.Close()
		w.Release()
		return err2
	}
	panic("...")
}

func handleMOVEG(s *server,args [][]byte) error {
	var ok,backward bool
	var i int64
	grp := &s.gls.GR
	id := s.gls.ID[:0]
	
	grp.Group = getarg(args,1)
	i = argtoi64(args,2)
	backward = argtrue(args,3)
	
	if s.rh.GroupCaps==nil {
		ok = false
	} else {
		i, id, ok = s.rh.CursorMoveGroup(grp, i, backward, id)
	}
	
	return s.b.writeSplit(ok, i, id)
}

type listActive struct {
	*server
	lam fastnntp.ListActiveMode
}

func (s *listActive) GetListActiveMode() fastnntp.ListActiveMode {
	return s.lam
}
func (s *listActive) WriteFullInfo(group []byte, high, low int64, status byte, description []byte) error {
	return s.b.writeSplit(true, group, high, low, status, description)
}
func (s *listActive) WriteActive(group []byte, high, low int64, status byte) error {
	return s.b.writeSplit(true, group, high, low, status, "")
}
func (s *listActive) WriteNewsgroups(group []byte, description []byte) error {
	var high, low int64
	return s.b.writeSplit(true, group, high, low, byte('y'), description)
}

func handleLISTN(s *server,args [][]byte) error {
	var lam = fastnntp.ListActiveMode(argtoi64(args, 1))
	
	if s.rh.GroupListingCaps!=nil {
		ila := &listActive{s,lam}
		s.rh.ListGroups(nil,ila)
	}
	return s.b.writeSplit(false)
}


/*

*/

func (c *Client) GetGroup(g *fastnntp.Group) bool {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("GROUP",g.Group)
	
	L.resp()
	
	args,_ := c.b.readSplit()
	ok := argtrue(args,0)
	g.Number = argtoi64(args,1)
	g.Low    = argtoi64(args,2)
	g.High   = argtoi64(args,3)
	return ok
}

func (c *Client) ListGroup(g *fastnntp.Group, w *fastnntp.DotWriter, first, last int64) {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("SOVER",g.Group,first,last)
	
	L.resp()
	
	args,_ := c.b.readSplit()
	ok := argtrue(args,0)
	if ok {
		r := c.b.readDot()
		defer ConsumeRelease(r)
		io.Copy(w,r)
	}
}
func (c *Client) CursorMoveGroup(g *fastnntp.Group, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("MOVEG", g.Group, i, backward)
	
	L.resp()
	
	args,_ := c.b.readSplit()
	
	ok = argtrue(args, 0)
	ni = argtoi64(args, 1)
	id = append(id_buf[:0], getarg(args, 2)...)
	return
}
func (c *Client) ListGroups(wm *fastnntp.WildMat, ila fastnntp.IListActive) (ok bool) {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("LISTN", int64(ila.GetListActiveMode()))
	
	L.resp()
	
	args,_ := c.b.readSplit()
	
	for argtrue(args, 0) {
		ok = true
		group       := getarg(args, 1)
		high        := argtoi64(args, 2)
		low         := argtoi64(args, 3)
		status      := argtob(args, 4)
		description := getarg(args, 5)
		ila.WriteFullInfo(group, high, low, status, description)
		
		args,_ = c.b.readSplit()
	}
	return ok
}


func init() {
	mntpCommands["GROUP"] = handleGROUP
	mntpCommands["SOVER"] = handleSOVER
	mntpCommands["MOVEG"] = handleMOVEG
	mntpCommands["LISTN"] = handleLISTN
}
