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
	
	// Debug!
	"fmt"
	"strings"
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
func (t *TOKEN) Debug() string {
	s := fmt.Sprintf("(%d)-(%d)-%x",t[0],t[1],t[2:])
	n := strings.TrimRight(s,"0-")
	if n==s { return s }
	return n+"::"
}

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
func (ove *OverviewElement) Debug() string {
	return fmt.Sprintf("{%d %q %q %q %q %q %d %d}",ove.Num,ove.Subject,ove.From,ove.Date,ove.MsgId,ove.Refs,ove.Lng,ove.Lines)
}

type OverviewMethod interface {
	FetchOne(grp []byte, num int64, tk *TOKEN, ove *OverviewElement) (rel Releaser,err error)
	FetchAll(grp []byte, num, lastnum int64, tk *TOKEN, ove *OverviewElement) (cur Cursor,err error)
	SeekOne(grp []byte, num int64, back bool, tk *TOKEN, ove *OverviewElement) (rel Releaser,err error)
	GroupStat(grp []byte) (num, low, high int64, err error)
	
	// Write methods:
	
	// Writes a new Overview line into the database.
	GroupWriteOv(grp []byte, autonum bool, md *Article_MD, tk *TOKEN, ove *OverviewElement) (err error)
	
	// Initializes a group in the overview-database.
	InitGroup(grp []byte) (err error)
}

type GroupElement struct {
	Group []byte
	Status byte
	Description []byte
}
type GroupMethod interface {
	FetchGroups(status, descr bool, ge *GroupElement) (cur Cursor,err error)
}

/*
Inspired by INN's HIS(history) database.
Maps message-ids to storage tokens.
*/
type HisMethod interface {
	HisWrite(msgid []byte,md *Article_MD, t *TOKEN) (err error)
	HisLookup(msgid []byte, t *TOKEN) (err error)
	HisCancel(msgid []byte) (err error)
}

type RiElement struct{
	Group []byte
	Num   int64
}

type RiHistory struct{
	// IF Group != nil THEN expire a group.
	Group []byte
	Num   int64
	
	// IF MessageID != nil THEN expire an article.
	MessageId []byte
}

/*
Reverse Index. Maps message-ids to group/number-pairs.
*/
type RiMethod interface {
	// Called for the first group/number-pair associated to the article
	RiWrite(msgid []byte,md *Article_MD, rie *RiElement) (err error)
	
	// Called for the remaining group/number-pair associated to the article
	RiWriteMore(msgid []byte,md *Article_MD, rie *RiElement) (err error)
	
	// Performs a reverse index lookup: message-id to the first group/number pair.
	RiLookup(msgid []byte,rie *RiElement) (rel Releaser,err error)
	
	// Query Expired articles. SHOULD return message-ids after their group/number counterparts.
	RiQueryExpired(ow *time.Time, rih *RiHistory) (cur Cursor, err error)
	
	// Expires an article using the message-id.
	RiExpire(msgid []byte) (err error)
}

type CfgBaseInfo struct{
	Spool      string `inn:"$spool"`
}

/*
General-Config.
*/
type CfgMaster struct{
	OvMethod  string `inn:"$ovmethod"`
	HisMethod string `inn:"$hismethod"`
	Spool     string `inn:"$pathspool"`
}
func (cfg *CfgMaster) BaseInfo() *CfgBaseInfo {
	return &CfgBaseInfo{
		Spool: cfg.Spool,
	}
}

type CfgStorageMethod struct {
	Method     string `inn:"$method"`
	Class      int    `inn:"$class"`
	Newsgroups string `inn:"$newsgroup"`
	Size       int64  `inn:"$size"`
	MaxSize    int64  `inn:"$max-size" json:"max-size"`
	Options    string `inn:"$options"`
	ExactMatch bool   `inn:"$exactmatch"`
}

/*
Storage-Config.
*/
type CfgStorage struct {
	Methods []*CfgStorageMethod `inn:"@method"`
}

type StorageManager struct {
	Classes [256]StorageMethod
	Methods [256]*CfgStorageMethod
	
	/*
	Used by the Posting backend.
	Will hold *fastnntp.WildMat objects.
	*/
	Wildmat [256]interface{}
}
func (s *StorageManager) SetMethods(cfg *CfgStorage) {
	for _,m := range cfg.Methods {
		if m==nil { continue }
		if m.Class<0 || m.Class>=256 { continue }
		s.Methods[m.Class] = m
	}
}

func (s *StorageManager) Retrieve(t *TOKEN, sl SMLevel) (a Article_R, rs SMLevel,err error) {
	sm := s.Classes[t.Class()]
	if sm==nil { err = ENotInitialized; return }
	a,rs,err = sm.Retrieve(t,sl)
	return
}

type CfgStorageLoader func(cfg *CfgStorageMethod, bi *CfgBaseInfo) (StorageMethod,error)

var storage_methods = make(map[string]CfgStorageLoader)

func RegisterStorageLoader(name string, ldr CfgStorageLoader) {
	storage_methods[name] = ldr
}

func (s *StorageManager) Open(bi *CfgBaseInfo) (err error) {
	for i,smc := range s.Methods {
		if smc==nil { continue }
		smf := storage_methods[smc.Method]
		if smf==nil { return fmt.Errorf("Unknown method %q",smc.Method) }
		s.Classes[i],err = smf(smc,bi)
		if err!=nil { return }
	}
	return
}

type CfgHisLoader func(cfg *CfgMaster) (HisMethod,error)

var his_methods = make(map[string]CfgHisLoader)

func RegisterHisLoader(name string, ldr CfgHisLoader) {
	his_methods[name] = ldr
}

func OpenHisMethod(cfg *CfgMaster) (HisMethod, error) {
	m := his_methods[cfg.HisMethod]
	if m==nil { return nil,fmt.Errorf("Unknown his-method %q",cfg.HisMethod) }
	return m(cfg)
}

type CfgOverviewLoader func(cfg *CfgMaster) (OverviewMethod,error)

var overview_methods = make(map[string]CfgOverviewLoader)

func RegisterOverviewLoader(name string, ldr CfgOverviewLoader) {
	overview_methods[name] = ldr
}

func OpenOverviewMethod(cfg *CfgMaster) (OverviewMethod, error) {
	m := overview_methods[cfg.OvMethod]
	if m==nil { return nil,fmt.Errorf("Unknown overview-method %q",cfg.OvMethod) }
	return m(cfg)
}
