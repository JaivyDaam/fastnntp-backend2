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
Implements a traditional newsgroup-listing solution, based on the text files

	active
	newsgroups

as provided by ftp://ftp.isc.org/pub/usenet/CONFIG/active
and ftp://ftp.isc.org/pub/usenet/CONFIG/newsgroups respectively.
*/
package tradgroup

import (
	"github.com/byte-mug/fastnntp-backend2/storage"
	"github.com/byte-mug/fastnntp-backend2/decompress"
	"os"
	"path/filepath"
	"errors"
	"io"
	"bufio"
	"regexp"
)

var line_active     = regexp.MustCompile(`^(\S+)\s+\S+\s+\S+\s+(.)`)
var line_newsgroups = regexp.MustCompile(`^(\S+)\s+(.*)`)

var eNotSupported = errors.New("tradgroup: not supported: status&&desc")
var eNoDecompress = errors.New("decompression not supported")

type TradGroup struct {
	ConfigPath string // path to the folder containing "active" and "newsgroups"
	Decompress string // must be "", "gz", or "bz2"
}

var _ storage.GroupMethod = (*TradGroup)(nil)

func (tg *TradGroup) openLL(name string) (io.ReadCloser,error) {
	pth := filepath.Join(tg.ConfigPath)
	return os.Open(pth)
}
func (tg *TradGroup) openHL(name string) (io.ReadCloser,error) {
	deco := decompress.Get(tg.Decompress)
	if deco!=nil { return nil,eNoDecompress }
	r,err := tg.openLL(name+tg.Decompress)
	if err==nil { r,err = deco(r) }
	return r,err
}

type lister struct {
	ge *storage.GroupElement
	rd io.ReadCloser
	rb *bufio.Reader
	descr bool
}
func (la *lister) Release() {
	la.rd.Close()
}
func (la *lister) Next() bool {
restart:
	line,err := la.rb.ReadBytes('\n')
	if err!=nil { return false }
	
	if la.descr {
		sm := line_newsgroups.FindSubmatch(line)
		if len(sm)==0 { goto restart } // Bad line! skip.
		la.ge.Group = sm[1]
		la.ge.Description = sm[2]
		return false
	} else {
		sm := line_active.FindSubmatch(line)
		if len(sm)==0 { goto restart } // Bad line! skip.
		la.ge.Group = sm[1]
		la.ge.Status = sm[2][0]
	}
	return true
}



func (tg *TradGroup) FetchGroups(status, descr bool, ge *storage.GroupElement) (cur storage.Cursor,err error) {
	// We can either read <CFG>/active or <CFG>/newsgroups
	if status && descr { return nil,eNotSupported }
	
	var name string
	if descr { name = "newsgroups" } else { name = "active" }
	
	var rd io.ReadCloser
	rd,err = tg.openHL(name)
	if err!=nil { return }
	
	cur = &lister{ ge:ge, rd: rd, rb: bufio.NewReader(rd), descr: descr }
	
	return
}

