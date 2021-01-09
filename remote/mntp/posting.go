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


func handleCHECK(s *server,args [][]byte) error {
	var wanted,possible bool
	id := getarg(args,1)
	if s.rh.PostingCaps==nil {
		wanted,possible = false,false
	} else if len(id)>1 {
		wanted,possible = s.rh.CheckPostId(id)
	} else {
		wanted,possible = true,s.rh.CheckPost()
	}
	return s.b.writeSplit(wanted,possible)
}

func handlePOST(s *server,args [][]byte) error {
	id := getarg(args,1)
	r := s.b.readDot()
	if s.rh.PostingCaps==nil {
		ConsumeRelease(r)
		return s.b.writeSplit(false,false)
	}
	defer ConsumeRelease(r)
	rejected,failed := s.rh.PerformPost(id,r)
	r.Consume()
	return s.b.writeSplit(!rejected,!failed)
}

func (c *Client) CheckPostId(id []byte) (wanted bool, possible bool) {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("CHECK",id)
	
	L.resp()
	
	args,_ := c.b.readSplit()
	wanted = argtrue(args,0)
	possible = argtrue(args,1)
	return
}
func (c *Client) CheckPost() (possible bool) {
	_,possible = c.CheckPostId(nil)
	return
}
func (c *Client) PerformPost(id []byte, r *fastnntp.DotReader) (rejected bool, failed bool) {
	L := c.req(); defer L.release()
	
	c.b.writeSplit("POST",id)
	w := c.b.writeDot()
	io.Copy(w,r)
	w.Close()
	w.Release()
	
	L.resp()
	
	args,_ := c.b.readSplit()
	rejected = !argtrue(args,0)
	failed = !argtrue(args,1)
	return
}


func init() {
	mntpCommands["CHECK"] = handleCHECK
	mntpCommands["POST"]  = handlePOST
}
