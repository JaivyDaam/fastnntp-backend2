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
Stores overview data into a LevelDB database.
*/
package ovldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	
	"github.com/byte-mug/fastnntp-backend2/storage"
	"encoding/binary"
	"io"
	"errors"
)

var eRecShort = io.ErrUnexpectedEOF
var eNoEnt = errors.New("No Entry")
var bin = binary.BigEndian

func tsplit(p []byte) ([]byte,[]byte) {
	for i,b := range p {
		if b=='\t' { return p[:i],p[i+1:] }
	}
	return p,nil
}

type OvLDB struct {
	DB *leveldb.DB
}

var _ storage.OverviewMethod = (*OvLDB)(nil)

func (ov *OvLDB) gstatid(grp []byte) []byte {
	rid := make([]byte,len(grp)+1)
	copy(rid,grp)
	rid[len(grp)] = 0xff
	return rid
}

func (ov *OvLDB) recid(grp []byte, num int64) []byte {
	rid := make([]byte,len(grp)+9)
	copy(rid,grp)
	bin.PutUint64(rid[len(grp)+1:],uint64(num))
	return rid
}
func (ov *OvLDB) recid_incr(rid []byte) {
	for i := len(rid)-1; i>=0; i-- {
		rid[i]++
		if rid[i]!=0 { return }
	}
}

func (ov *OvLDB) recid2num(recid []byte) int64 {
	if len(recid)<8 { return 0 }
	return int64(bin.Uint64(recid[len(recid)-8:]))
}

/* should be called explodeRecord */
func (ov *OvLDB) splitRecord(rec []byte, tk *storage.TOKEN, ove *storage.OverviewElement) (err error) {
	if len(rec)<len(tk) { return eRecShort }
	rec = rec[copy(tk[:],rec):]
	ove.Subject,rec = tsplit(rec)
	ove.From   ,rec = tsplit(rec)
	ove.Date   ,rec = tsplit(rec)
	ove.MsgId  ,rec = tsplit(rec)
	ove.Refs   ,rec = tsplit(rec)
	if len(rec)<16 { return eRecShort }
	ove.Lng   = int64(bin.Uint64(rec))
	ove.Lines = int64(bin.Uint64(rec[8:]))
	return
}

func (ov *OvLDB) splitGstat(rec []byte) (num, low, high int64, err error) {
	if len(rec)<24 { err = eRecShort; return }
	num  = int64(bin.Uint64(rec[ 0:]))
	low  = int64(bin.Uint64(rec[ 8:]))
	high = int64(bin.Uint64(rec[16:]))
	return
}

func (ov *OvLDB) FetchOne(grp []byte, num int64, tk *storage.TOKEN, ove *storage.OverviewElement) (rel storage.Releaser,err error) {
	var rid,rec []byte
	rid = ov.recid(grp,num)
	rec,err = ov.DB.Get(rid,nil)
	ove.Num = num
	if err!=nil { err = ov.splitRecord(rec,tk,ove) }
	return
}

type cursor struct{
	iterator.Iterator
	tk *storage.TOKEN
	ove *storage.OverviewElement
	ov *OvLDB
	next bool
}
func (c *cursor) Next() (ok bool) {
restart:
	if c.next {
		ok = c.Next()
	} else {
		ok = c.First()
		c.next = true
	}
	if !ok { return }
	
	c.ove.Num = c.ov.recid2num(c.Key())
	err := c.ov.splitRecord(c.Value(),c.tk,c.ove)
	if err!=nil { goto restart }
	
	return
}

func (ov *OvLDB) FetchAll(grp []byte, num, lastnum int64, tk *storage.TOKEN, ove *storage.OverviewElement) (cur storage.Cursor,err error) {
	var rid,lid []byte
	rid = ov.recid(grp,num)
	lid = ov.recid(grp,lastnum)
	ov.recid_incr(lid)
	iter := ov.DB.NewIterator(&util.Range{rid,lid},nil)
	cur = &cursor{iter,tk,ove,ov,false}
	return
}
func (ov *OvLDB) SeekOne(grp []byte, num int64, back bool, tk *storage.TOKEN, ove *storage.OverviewElement) (rel storage.Releaser,err error) {
	var rid,rec []byte
	rid = ov.recid(grp,num)
	
	iter := ov.DB.NewIterator(nil,nil)
	ok := iter.Seek(rid)
	if !ok { iter.Release(); err = eNoEnt; return }
	if back {
		ok = iter.Prev()
	} else {
		if string(iter.Key())==string(rid) { ok = iter.Next() }
	}
	if !ok { iter.Release(); err = eNoEnt; return }
	rec = iter.Value()
	
	ove.Num = ov.recid2num(iter.Key())
	if err!=nil { iter.Release(); err = ov.splitRecord(rec,tk,ove) }
	
	rel = iter
	
	return
}
func (ov *OvLDB) GroupStat(grp []byte) (num, low, high int64, err error) {
	var rec []byte
	rec,err = ov.DB.Get(ov.gstatid(grp),nil)
	if err!=nil { return }
	return ov.splitGstat(rec)
}


