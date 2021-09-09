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


package poster

import (
	"io"
	"github.com/byte-mug/fastnntp"
	"github.com/byte-mug/fastnntp/posting"
	"github.com/byte-mug/fastnntp-backend2/storage"
	"github.com/byte-mug/fastnntp-backend2/iohelper"
	
	"bytes"
	"time"
	
	// Temp
	"github.com/byte-mug/fastnntp-backend2/storage/ovldb"
	"fmt"
)

func debug(i ...interface{}) {
	fmt.Println(i...)
}

type OverviewMethodEx interface{
	GroupWrite(grp []byte, num int64, tk *storage.TOKEN, ove *storage.OverviewElement) (err error)
	InitGroup(grp []byte) (err error)
}

type noopstamp int
func (noopstamp) GetId(id_buf []byte) []byte { return nil }
func (noopstamp) PathSeg(buf []byte) []byte { return nil }

var noopstamp_inst posting.Stamper = noopstamp(0)

type rBuffer struct{
	bytes.Buffer
}
func (*rBuffer) Release() {}

type StorageWriter struct {
	Stamp posting.Stamper
	SM    *storage.StorageManager
	OV    *ovldb.OvLDB
	HIS   storage.HisMethod
	RI    storage.RiMethod
}

const day = time.Hour*24

func (c *StorageWriter) article_md() *storage.Article_MD {
	a := new(storage.Article_MD)
	a.Arrival = time.Now()
	a.Expires = a.Arrival.Add(day) // TODO: make this configurable.
	return a
}
func (c *StorageWriter) findStorageClass(ngrps [][]byte, size int64) int {
	for i,sm := range c.SM.Classes {
		if sm==nil { continue }
		mt := c.SM.Methods[i]
		if mt!=nil {
			if size < mt.Size { continue }
			if size > mt.MaxSize && mt.MaxSize > 0 { continue }
			if mt.Newsgroups!="" {
				wm,_ := c.SM.Wildmat[i].(*fastnntp.WildMat)
				if wm==nil {
					wm = fastnntp.ParseWildMat(mt.Newsgroups)
					if wm.Compile()!=nil { continue } /* We can't compile. Match failed! */
					c.SM.Wildmat[i] = wm
				}
				fail,succ := false,false
				for _,ngrp := range ngrps {
					if wm.Match(ngrp) {
						succ = true
					} else {
						fail = true
					}
				}
				if !succ { continue } /* One must match! */
				if mt.ExactMatch && fail { continue } /* All must match! */
			}
		}
		return i
	}
	return -1
}
func (c *StorageWriter) CheckPost() (possible bool) { return true }
func (c *StorageWriter) CheckPostId(id []byte) (wanted bool, possible bool) {
	tk := new(storage.TOKEN)
	if c.HIS.HisLookup(id,tk)!=nil { return true,true }
	ar,_,err := c.SM.Retrieve(tk, storage.SM_Stat)
	if err!=nil { return true,true }
	if ar!=nil  { ar.Release() }
	return false,true
}
func (c *StorageWriter) PerformPost(id []byte, r *fastnntp.DotReader) (rejected bool, failed bool) {
	hb := new(bytes.Buffer)
	bb := new(bytes.Buffer)
	ab := new(rBuffer)
	sp := &iohelper.Splitter{
		Head:hb,
		Body:bb,
	}
	_,err := io.Copy(sp,r)
	if err!=nil { return false,true } /* Failed receiving the article. */
	
	stamp := c.Stamp
	if stamp==nil { stamp = noopstamp_inst }
	
	hi := posting.ParseAndProcessHeaderWithBuffer(id,stamp,hb.Bytes(),&ab.Buffer)
	if hi==nil { return false,true }
	
	if len(hi.MessageId)==0 { return true,false } /* No article-id: rejected! */
	
	ngrps := posting.SplitNewsgroups(hi.Newsgroups)
	
	// TODO: filter the groups for allowed/disallowed groups.
	
	if len(ngrps)==0 { return true,false } /* We need to be in at least one newsgroup. */
	
	//nums := make([]int64,len(ngrps))
	
	tk := new(storage.TOKEN)
	ove := new(storage.OverviewElement)
	ove.Num = 0
	ove.Subject = hi.Subject
	ove.From    = hi.From
	ove.Date    = hi.Date
	ove.MsgId   = hi.MessageId
	ove.Refs    = hi.References
	ove.Lng     = int64(len(hi.RAW)+bb.Len())
	ove.Lines   = posting.CountLines(bb.Bytes())
	
	bb.WriteTo(ab)
	
	if c.HIS.HisLookup(hi.MessageId,tk)==nil { return true,false } /* Prevent Message-ID overwrites. */
	
	cls := c.findStorageClass(ngrps,ove.Lng)
	if cls<0 { return false,true /* We didn't found a storage class: posting failed! */ }
	
	amd := c.article_md()
	
	tk[0] = byte(cls)
	err = c.SM.Classes[cls].Store(amd,ab,tk)
	if err!=nil { return false,true /* Storing the article failed with some IO error. Fail. */ }
	
	err = c.HIS.HisWrite(hi.MessageId,amd,tk)
	if err!=nil { return false,true }
	
	first := true
	ri := c.RI
	var rie *storage.RiElement
	var riw storage.RiWriter
	if ri!=nil {
		rie = new(storage.RiElement)
		riw = ri.RiBegin(hi.MessageId)
	}
	
	for _,ngrp := range ngrps {
		err = c.OV.GroupWriteOv(ngrp,true,amd,tk,ove)
		
		if riw==nil { continue } /* Short cut, if ri==nil, we don't need the further code. */
		
		rie.Group = ngrp
		rie.Num   = ove.Num
		if first {
			riw.RiWrite(amd, rie)
		} else {
			riw.RiWriteMore(amd, rie)
		}
		first = false
	}
	err = riw.RiCommit()
	
	/* Success! */
	return false,false
}



