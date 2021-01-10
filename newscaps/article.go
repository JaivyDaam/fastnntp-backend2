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
	"github.com/byte-mug/fastnntp-backend2/iohelper"
	"io/ioutil"
)

type ArticleReader struct {
	SM *storage.StorageManager
	OV storage.OverviewMethod
	HIS storage.HisMethod
}

var _ fastnntp.ArticleCaps = (*ArticleReader)(nil)

// TODO optimize-away the (implicit) memory allocations (var t TOKEN and var ove OverviewElement)


func (ar *ArticleReader) tstat(t *storage.TOKEN) bool {
	obj,_,err := ar.SM.Retrieve(t,storage.SM_Stat)
	if obj!=nil { obj.Release() }
	return err==nil
}

func (ar *ArticleReader) StatArticle(a *fastnntp.Article) bool {
	var t storage.TOKEN
	var ove storage.OverviewElement
	if a.HasId && ar.HIS!=nil {
		err := ar.HIS.HisLookup(a.MessageId,&t)
		if err!=nil { return false }
		return true
	}
	if a.HasNum && ar.OV!=nil {
		rel,err := ar.OV.FetchOne(a.Group,a.Number,&t,&ove)
		if err!=nil { return false }
		a.MessageId = append(a.MessageId[:0],ove.MsgId...)
		if rel!=nil { rel.Release() }
		return ar.tstat(&t)
	}
	return false
}

func writerFrom(obj storage.Article_R, smt storage.SMLevel, head, body bool) func(w *fastnntp.DotWriter) {
	return func(w *fastnntp.DotWriter) {
		defer obj.Release()
		if smt==storage.SM_Head {
			obj.WriteTo(w)
		} else if head && body {
			obj.WriteTo(w)
		} else {
			var split iohelper.Splitter
			if head {
				split.Head = w
			} else {
				split.Head = ioutil.Discard
				split.Body = &iohelper.Chopper{Rest: w}
			}
			obj.WriteTo(&split)
		}
	}
}

func (ar *ArticleReader) tContent(t *storage.TOKEN, head, body bool) func(w *fastnntp.DotWriter) {
	smt := storage.SM_All
	if !body { smt = storage.SM_Head } 
	obj,smt,err := ar.SM.Retrieve(t,smt)
	if err!=nil { return nil }
	
	return writerFrom(obj,smt,head,body)
}
func (ar *ArticleReader) GetArticle(a *fastnntp.Article, head, body bool) func(w *fastnntp.DotWriter) {
	var t storage.TOKEN
	var ove storage.OverviewElement
	if a.HasId && ar.HIS!=nil {
		err := ar.HIS.HisLookup(a.MessageId,&t)
		if err!=nil { return nil }
		return ar.tContent(&t,head,body)
	}
	if a.HasNum && ar.OV!=nil {
		rel,err := ar.OV.FetchOne(a.Group,a.Number,&t,&ove)
		if err!=nil { return nil }
		a.MessageId = append(a.MessageId[:0],ove.MsgId...)
		if rel!=nil { rel.Release() }
		a.HasId = true
		return ar.tContent(&t,head,body)
	}
	return nil
}


func (ar *ArticleReader) WriteOverview(a *fastnntp.ArticleRange) func(w fastnntp.IOverview) {
	pt := new(storage.TOKEN)
	pove := new(storage.OverviewElement)
	if a.HasId {
		// TODO: we need a way to perform a lookup.
		return nil
	}
	if a.HasNum && ar.OV!=nil {
		cur,err := ar.OV.FetchAll(a.Group,a.Number,a.LastNumber,pt,pove)
		if err!=nil { return nil }
		
		return func(w fastnntp.IOverview) {
			defer cur.Release()
			for cur.Next() {
				err := w.WriteEntry(
					pove.Num,
					pove.Subject,
					pove.From,
					pove.Date,
					pove.MsgId,
					pove.Refs,
					pove.Lng,
					pove.Lines)
				/*
				 * At this point, an error indicates an IO error.
				 * We need to be gentle at this point.
				 */
				if err!=nil { break }
			}
		}
	}
	return nil
}

