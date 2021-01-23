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
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	
	"github.com/byte-mug/fastnntp-backend2/storage"
	"github.com/byte-mug/fastnntp-backend2/utils/minihash"
	"sync"
	"encoding/binary"
	"io"
	"errors"
	
	"fmt"
	"path/filepath"
)

func debug(i ...interface{}) { fmt.Println(i...) }
func debugf(f string, i ...interface{}) { fmt.Printf(f+"\n",i...) }

var eRecShort = io.ErrUnexpectedEOF
var eNoEnt = errors.New("No Entry")
var bin = binary.BigEndian

const m_locks_size = 1<<12
const m_locks_mask = m_locks_size-1
var m_locks [m_locks_size]sync.Mutex

func tsplit(p []byte) ([]byte,[]byte) {
	for i,b := range p {
		if b=='\t' { return p[:i],p[i+1:] }
	}
	return p,nil
}

/*
Handles the format of the LevelDB keys.
*/
type OvKeyFormat interface {
	gstatid(grp []byte) []byte
	recid_prefix_eq(rid1, rid2 []byte) bool
	recid(grp []byte, num int64) []byte
	recid_incr(rid []byte)
	recid2num(recid []byte) int64
}

type OvValFormat interface {
	explodeRecord(rec []byte, tk *storage.TOKEN, ove *storage.OverviewElement) (err error)
	joinRecord(buf []byte, tk *storage.TOKEN, ove *storage.OverviewElement) (rec []byte)
	explodeGstat(rec []byte) (num, low, high int64, err error)
	joinGstat(buf []byte, num, low, high int64) (rec []byte)
}

type OvLDB struct {
	OvKeyFormat
	OvValFormat
	DB *leveldb.DB
}

var _ storage.OverviewMethod = (*OvLDB)(nil)

type ovf1 int

var i_ovkf1 OvKeyFormat = ovf1(0)

func GetKF_V1() OvKeyFormat { return i_ovkf1 }

var i_ovvf1 OvValFormat = ovf1(0)

func GetVF_V1() OvValFormat { return i_ovvf1 }


func (ovf1) gstatid(grp []byte) []byte {
	rid := make([]byte,len(grp)+1)
	copy(rid,grp)
	rid[len(grp)] = 0xff
	return rid
}


func (ovf1) recid_prefix_eq(rid1, rid2 []byte) bool {
	if len(rid1)!=len(rid2) { return false }
	l := len(rid1)-8
	if l<0 { return false } // Safety first.
	return string(rid1[:l])==string(rid2[:l])
}
func (ovf1) recid(grp []byte, num int64) []byte {
	rid := make([]byte,len(grp)+9)
	copy(rid,grp)
	bin.PutUint64(rid[len(grp)+1:],uint64(num))
	return rid
}
func (ovf1) recid_incr(rid []byte) {
	for i := len(rid)-1; i>=0; i-- {
		rid[i]++
		if rid[i]!=0 { return }
	}
}

func (ovf1) recid2num(recid []byte) int64 {
	if len(recid)<8 { return 0 }
	return int64(bin.Uint64(recid[len(recid)-8:]))
}


func (ovf1) explodeRecord(rec []byte, tk *storage.TOKEN, ove *storage.OverviewElement) (err error) {
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
func (ovf1) joinRecord(buf []byte, tk *storage.TOKEN, ove *storage.OverviewElement) (rec []byte) {
	rec = buf[:0]
	rec = append(rec,tk[:]...)
	rec = append(rec,ove.Subject...)
	rec = append(rec,'\t')
	rec = append(rec,ove.From...)
	rec = append(rec,'\t')
	rec = append(rec,ove.Date...)
	rec = append(rec,'\t')
	rec = append(rec,ove.MsgId...)
	rec = append(rec,'\t')
	rec = append(rec,ove.Refs...)
	rec = append(rec,'\t')
	var b16 [16]byte
	bin.PutUint64(b16[:8],uint64(ove.Lng))
	bin.PutUint64(b16[8:],uint64(ove.Lines))
	rec = append(rec,b16[:]...)
	return
}


func (ovf1) explodeGstat(rec []byte) (num, low, high int64, err error) {
	if len(rec)<24 { err = eRecShort; return }
	num  = int64(bin.Uint64(rec[ 0:]))
	low  = int64(bin.Uint64(rec[ 8:]))
	high = int64(bin.Uint64(rec[16:]))
	return
}
func (ovf1) joinGstat(buf []byte, num, low, high int64) (rec []byte) {
	if cap(buf)<24 { buf = make([]byte,24) }
	rec = buf[:24]
	bin.PutUint64(rec[ 0:],uint64(num ))
	bin.PutUint64(rec[ 8:],uint64(low ))
	bin.PutUint64(rec[16:],uint64(high))
	return rec
}

func (ov *OvLDB) lock_group(grp []byte) func() {
	l := &m_locks[minihash.HashBytes(grp)&m_locks_mask]
	l.Lock()
	return l.Unlock
}


func (ov *OvLDB) FetchOne(grp []byte, num int64, tk *storage.TOKEN, ove *storage.OverviewElement) (rel storage.Releaser,err error) {
	var rid,rec []byte
	rid = ov.recid(grp,num)
	rec,err = ov.DB.Get(rid,nil)
	ove.Num = num
	if err==nil { err = ov.explodeRecord(rec,tk,ove) }
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
		ok = c.Iterator.Next()
	} else {
		ok = c.Iterator.First()
		c.next = true
	}
	if !ok { return }
	
	//debugf("ITER %q %q",c.Key(),c.Value())
	c.ove.Num = c.ov.recid2num(c.Key())
	err := c.ov.explodeRecord(c.Value(),c.tk,c.ove)
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
	if back && !ok { ok = iter.Last(); back = false }
	if !ok { iter.Release(); err = eNoEnt; return }
	//debugf("FND %q %q",iter.Key(),iter.Value())
	if back {
		ok = iter.Prev()
	} else {
		if string(iter.Key())==string(rid) { ok = iter.Next() }
	}
	
	/* Check, that we did't went into the next newsgroup group accidentially. */
	if ok { ok = ov.recid_prefix_eq(rid,iter.Key()) }
	
	if !ok { iter.Release(); err = eNoEnt; return }
	rec = iter.Value()
	
	ove.Num = ov.recid2num(iter.Key())
	if err==nil { err = ov.explodeRecord(rec,tk,ove) }
	
	if err!=nil { iter.Release(); return }
	
	rel = iter
	
	return
}
func (ov *OvLDB) GroupStat(grp []byte) (num, low, high int64, err error) {
	var rec []byte
	rec,err = ov.DB.Get(ov.gstatid(grp),nil)
	if err!=nil { return }
	return ov.explodeGstat(rec)
}

func (ov *OvLDB) GroupWriteOv(grp []byte, autonum bool, md *storage.Article_MD, tk *storage.TOKEN, ove *storage.OverviewElement) (err error) {
	defer ov.lock_group(grp)()
	var mrid,mrec,rid,rec []byte
	var num int64
	
	mrid = ov.gstatid(grp)
	{
		omrec,err1 := ov.DB.Get(mrid,nil)
		if err1!=nil { return err1 }
		anum,low,high,err1 := ov.explodeGstat(omrec)
		if err1!=nil { return err1 }
		anum++
		if autonum {
			high++
			num = high
			ove.Num = num
		} else {
			num = ove.Num
			if high<num { high = num}
		}
		mrec = ov.joinGstat(make([]byte,32),anum,low,high)
	}
	
	rid = ov.recid(grp,num)
	ove.Num = num
	rec = ov.joinRecord(make([]byte,0,1<<10),tk,ove)
	
	
	bat := leveldb.MakeBatch(1<<10)
	bat.Put(rid,rec)
	bat.Put(mrid,mrec)
	
	err = ov.DB.Write(bat,nil)
	return
}

func (ov *OvLDB) InitGroup(grp []byte) (err error) {
	defer ov.lock_group(grp)()
	rid := ov.gstatid(grp)
	rec,err1 := ov.DB.Get(ov.gstatid(grp),nil)
	num,low,high,err2 := ov.explodeGstat(rec)
	
	if err1!=nil || err2!=nil {
		num,low,high = 0,1,0
	}
	rec = ov.joinGstat(make([]byte,32),num,low,high)
	
	return ov.DB.Put(rid,rec,nil)
}


func OpenOvLDB(path string) (*OvLDB,error) {
	db,err := leveldb.OpenFile(path, nil)
	if err!=nil { return nil,err }
	return &OvLDB{
		OvKeyFormat: i_ovkf1,
		OvValFormat: i_ovvf1,
		DB: db,
	},nil
}

func OpenSpoolOvLDB(spool string, o *opt.Options) (*OvLDB,error) {
	db,err := leveldb.OpenFile(filepath.Join(spool,"ovldb"), o)
	if err!=nil { return nil,err }
	return &OvLDB{
		OvKeyFormat: i_ovkf1,
		OvValFormat: i_ovvf1,
		DB: db,
	},nil
}


func loader_ovldb(cfg *storage.CfgMaster) (storage.OverviewMethod,error) {
	return OpenSpoolOvLDB(cfg.Spool,nil)
}

func init() {
	storage.RegisterOverviewLoader("ovldb",loader_ovldb)
}
