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


package newscaps

import (
	"github.com/byte-mug/fastnntp"
	"github.com/byte-mug/fastnntp-backend2/storage"
	//"github.com/byte-mug/fastnntp-backend2/iohelper"
	//"io/ioutil"
	"fmt"
)

func lam2bool(lam fastnntp.ListActiveMode) (active bool, descr bool) {
	switch lam {
	case fastnntp.LAM_Full: active = true; descr = true
	case fastnntp.LAM_Active: active = true;
	case fastnntp.LAM_Newsgroups: descr = true
	}
	return
}

type GroupReader struct {
	GM storage.GroupMethod
	OV storage.OverviewMethod
}

var _ fastnntp.GroupCaps = (*GroupReader)(nil)

func (gr *GroupReader) GetGroup(g *fastnntp.Group) bool {
	if gr.OV==nil { return false }
	num,lo,hi,err := gr.OV.GroupStat(g.Group)
	if err!=nil { return false }
	g.Number = num
	g.Low = lo
	g.High = hi
	return true
}
func (gr *GroupReader) ListGroup(g *fastnntp.Group, w *fastnntp.DotWriter, first, last int64) {
	if gr.OV==nil { return }
	pt := new(storage.TOKEN)
	pove := new(storage.OverviewElement)
	cur,err := gr.OV.FetchAll(g.Group,first,last,pt,pove)
	if err!=nil { return }
	defer cur.Release()
	
	for cur.Next() {
		fmt.Fprintf(w,"%v\r\n",pove.Num)
	}
	
	return
}
func (gr *GroupReader) CursorMoveGroup(g *fastnntp.Group, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	if gr.OV==nil { return }
	pt := new(storage.TOKEN)
	pove := new(storage.OverviewElement)
	rel,err := gr.OV.SeekOne(g.Group,i,backward,pt,pove)
	if err!=nil { return }
	if rel!=nil { rel.Release() }
	ni = pove.Num
	id = append(id_buf[:0],pove.MsgId...)
	ok = true
	return
}

var _ fastnntp.GroupListingCaps = (*GroupReader)(nil)
func (gr *GroupReader) ListGroups(wm *fastnntp.WildMat, ila fastnntp.IListActive) bool {
	active,descr := lam2bool(ila.GetListActiveMode())
	if active && gr.OV!=nil { return false }
	
	ge := new(storage.GroupElement)
	cur,err := gr.GM.FetchGroups(active,descr,ge)
	if err!=nil { return false }
	defer cur.Release()
	
	if active {
		for cur.Next() {
			if wm!=nil && !wm.Match(ge.Group) { continue }
			_,lo,hi,err := gr.OV.GroupStat(ge.Group)
			err = ila.WriteFullInfo(ge.Group,hi,lo,ge.Status,ge.Description)
			if err!=nil { break } /* Network-IO-Error. */
		}
	} else {
		for cur.Next() {
			if wm!=nil && !wm.Match(ge.Group) { continue }
			err = ila.WriteNewsgroups(ge.Group,ge.Description)
			if err!=nil { break } /* Network-IO-Error. */
		}
	}
	
	return true
}

