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
Expires old articles.
*/
package expire

import "github.com/byte-mug/fastnntp-backend2/storage"
import "context"
import "time"
import "errors"

var ECouldNotQuery = errors.New("CouldNotQuery")

type Expirer struct{
	SM  *storage.StorageManager
	OV  storage.OverviewMethod
	HIS storage.HisMethod
	RI  storage.RiMethod
}


/*
Expires old articles.

ow = NOW.
*/
func(e *Expirer) ExpireProcess(ctx context.Context, ow *time.Time) error {
	if e.RI==nil && ow==nil { return ECouldNotQuery }
	
	hist := new(storage.RiHistory)
	tok := new(storage.TOKEN)
	ove := new(storage.OverviewElement)
	
	cur,err := e.RI.RiQueryExpired(ow,hist)
	if err!=nil { return err }
	defer cur.Release()
	
	hasNoTok := true
	for cur.Next() {
		if hist.Group!=nil && e.OV!=nil {
			if hasNoTok {
				if rel,err := e.OV.FetchOne(hist.Group,hist.Num,tok,ove); err==nil {
					hasNoTok = false
					if rel!=nil { rel.Release() }
				}
			}
			e.OV.CancelOv(hist.Group,hist.Num)
		}
		if hist.MessageId!=nil && e.HIS!=nil {
			if hasNoTok {
				if e.HIS.HisLookup(hist.MessageId,tok)==nil {
					hasNoTok = false
				}
			}
			e.HIS.HisCancel(hist.MessageId)
			e.RI.RiExpire(hist.MessageId)
			if !hasNoTok && e.SM!=nil {
				e.SM.Cancel(tok)
			}
			hasNoTok = true
			
			// We receive a cancellation signal only after the whole article has been deleted.
			if err = ctx.Err(); err!=nil { return err }
		}
	}
	return nil
}

