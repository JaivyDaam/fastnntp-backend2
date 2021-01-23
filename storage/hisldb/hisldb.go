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
*/
package hisldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	//"github.com/syndtr/goleveldb/leveldb/iterator"
	
	"github.com/byte-mug/fastnntp-backend2/storage"
	"errors"
	"path/filepath"
)

var eTokenMismatch = errors.New("internal Error: Token mismatch.")

type HisLdb struct {
	DB *leveldb.DB
}

var _ storage.HisMethod = (*HisLdb)(nil)

func (s *HisLdb) HisWrite(msgid []byte,md *storage.Article_MD, t *storage.TOKEN) (err error) {
	return s.DB.Put(msgid,t[:],nil)
}
func (s *HisLdb) HisLookup(msgid []byte, t *storage.TOKEN) (err error) {
	var rec []byte
	rec,err = s.DB.Get(msgid,nil)
	copy(t[:],rec)
	if err==nil && len(t)!=len(rec) { err = eTokenMismatch }
	return
}
func (s *HisLdb) HisCancel(msgid []byte) (err error) {
	err = s.DB.Delete(msgid,nil)
	return
}

func OpenSpoolHisLdb(spool string, o *opt.Options) (*HisLdb,error) {
	db,err := leveldb.OpenFile(filepath.Join(spool,"hisldb"), o)
	if err!=nil { return nil,err }
	return &HisLdb{
		DB: db,
	},nil
}

func loader_hisldb(cfg *storage.CfgMaster) (storage.HisMethod,error) {
	return OpenSpoolHisLdb(cfg.Spool,nil)
}

func init() {
	storage.RegisterHisLoader("hisldb",loader_hisldb)
}
