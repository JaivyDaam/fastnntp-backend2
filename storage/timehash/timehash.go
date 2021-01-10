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
Implements a traditional storage method, where each article is stored in a
separate file.

in the TIMEHASH method, each file is named:

	<spoolpath>/time-nn/zzbb/cc/yyyy-aadd

Where "nn" is the hexadecimal value of the storage-class value, and "zzbb",
"cc" and "aadd" are components of the arrival time in hexadecimal. The arrival
time, in seconds since the epoch is converted to hexadecimal and interpreted as
0xzzaabbccdd. "yyyy" is the serial number.

*/
package timehash

import (
	"github.com/byte-mug/fastnntp-backend2/storage"
	"os"
	"io"
	"path/filepath"
	
	"fmt"
	"encoding/binary"
	"sync/atomic"
)

func str2os(s string) string {
	if filepath.Separator=='/' { return s }
	bs := []byte(s)
	for i,b := range bs {
		if b=='/' { bs[i] = filepath.Separator }
	}
	return string(bs)
}

var bin = binary.BigEndian

type TimeHashSpool struct {
	serial uint32
	SpoolPath string
}
func (sm *TimeHashSpool) Close() error { return nil }
func (sm *TimeHashSpool) timehash(md *storage.Article_MD, t *storage.TOKEN) {
	b := t.Bytes()
	storage.Bzero(b)
	bin.PutUint64(b,uint64(md.Arrival.Unix()))
	bin.PutUint32(b[8:],atomic.AddUint32(&sm.serial,1))
}
func (sm *TimeHashSpool) thpath(t *storage.TOKEN) string {
	b := t.Bytes()
	tm := bin.Uint64(b)
	ser := bin.Uint32(b[8:])
	// time-nn/zzbb/cc/yyyy-aadd  <- 0xzzaabbccdd, 0xyyyy
	
	s := fmt.Sprintf("time-%02x/%02x%02x/%02x/%04x-%02x%02x",
		t.Class(),
		(tm>>32)&0xff,(tm>>16)&0xff,(tm>>8)&0xff,
		ser&0xffff,(tm>>24)&0xff,tm&0xff)
	return filepath.Join(sm.SpoolPath,str2os(s))
}
func (sm *TimeHashSpool) Store(md *storage.Article_MD, a storage.Article_W,t *storage.TOKEN) (err error) {
	var f *os.File
	defer a.Release()
	
	sm.timehash(md,t)
	s := sm.thpath(t)
	s_del := s+".del"
	os.MkdirAll(filepath.Dir(s),0750)
	f,err = os.OpenFile(s,os.O_WRONLY|os.O_CREATE|os.O_EXCL,0600)
	if err!=nil { return }
	defer os.Remove(s_del)
	defer f.Close()
	_,err = a.WriteTo(f)
	if err!=nil { os.Rename(s,s_del) }
	return
}
func (sm *TimeHashSpool) Retrieve(t *storage.TOKEN, s storage.SMLevel) (a storage.Article_R, rs storage.SMLevel,err error) {
	var f *os.File
	name := sm.thpath(t)
	
	if s==storage.SM_Stat {
		_,err = os.Stat(name)
		rs = storage.SM_Stat
		return
	}
	f,err = os.Open(name)
	if err!=nil { return }
	
	a = &articleFile{f}
	
	rs = storage.SM_All
	return
}
func (sm *TimeHashSpool) Cancel(t *storage.TOKEN) (err error) {
	name := sm.thpath(t)
	return os.Remove(name)
}

var _ storage.StorageMethod = (*TimeHashSpool)(nil)

type articleFile struct {
	*os.File
}
func (a *articleFile) Release() { a.Close() }
func (a *articleFile) WriteTo(w io.Writer) (n int64, err error) { return io.Copy(w,a.File) }

