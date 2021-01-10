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
Storage methods, modeled after the venerable INN usenet server.

NOTE: All interfaces are WIP. Expect breaking changes.
*/
package storage

import (
	"io"
	"time"
	"errors"
)

var ENotInitialized = errors.New("SM not Initialized")

type SMFlags uint
const (
	SM_Expensivestat SMFlags = 1<<iota
	SM_Selfexpire
)

type SMLevel uint
const (
	SM_Stat SMLevel = iota
	SM_Head
	SM_All
)

type TOKEN [34]byte
func (t *TOKEN) Bytes() []byte { return t[2:] }
func (t *TOKEN) Class() byte { return t[0] }
func (t *TOKEN) reserved() byte { return t[1] }

func Bzero(bs []byte) {
	for i := range bs { bs[i] = 0 }
}

type Releaser interface{
	Release()
}
type Cursor interface{
	Releaser
	Next() bool
}

type Article_W interface{
	Releaser
	io.Reader
	io.WriterTo
}

type Article_R interface{
	Releaser
	io.WriterTo
}

type Article_MD struct{
	Arrival time.Time
}


type StorageMethod interface {
	io.Closer
	
	Store(md *Article_MD, a Article_W,t *TOKEN) (err error)
	Retrieve(t *TOKEN, s SMLevel) (a Article_R, rs SMLevel,err error)
	Cancel(t *TOKEN) (err error)
}

type OverviewElement struct{
	Num int64
	Subject, From, Date, MsgId, Refs []byte
	Lng, Lines int64
}

type OverviewMethod interface {
	FetchOne(grp []byte, num int64, tk *TOKEN, ove *OverviewElement) (rel Releaser,err error)
	FetchAll(grp []byte, num, lastnum int64, tk *TOKEN, ove *OverviewElement) (cur Cursor,err error)
	SeekOne(grp []byte, num int64, back bool, tk *TOKEN, ove *OverviewElement) (rel Releaser,err error)
	GroupStat(grp []byte) (num, low, high int64, err error)
}

type GroupElement struct {
	Group []byte
	Status byte
	Description []byte
}
type GroupMethod interface {
	FetchGroups(descr bool, ge *GroupElement) (cur Cursor,err error)
}

type HisMethod interface {
	// HisWrite(msgid []byte,md *Article_MD, t *TOKEN) (err error)
	HisLookup(msgid []byte, t *TOKEN) (err error)
}


type StorageManager struct {
	Classes [256]StorageMethod
}

func (s *StorageManager) Retrieve(t *TOKEN, sl SMLevel) (a Article_R, rs SMLevel,err error) {
	sm := s.Classes[t.Class()]
	if sm==nil { err = ENotInitialized; return }
	a,rs,err = sm.Retrieve(t,sl)
	return
}



