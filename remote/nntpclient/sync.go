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


package nntpclient

import (
	"sync"
	"net/textproto"
)


type lPipeline struct {
	textproto.Pipeline
	rw sync.RWMutex
}

func (p *lPipeline) exclusive() func() {
	p.rw.Lock()
	return p.rw.Unlock
}

type lRequest struct {
	*lPipeline
	id uint
	state uint
}
func (p *lPipeline) ackquire() (*lRequest) {
	p.rw.RLock()
	id := p.Next()
	return &lRequest{p,id,0}
}
func (r *lRequest) update(state uint) {
	if state>3 { state = 3 }
	for r.state<state {
		r.state++
		switch r.state {
		case 1: r.StartRequest(r.id)
		case 2:
			r.EndRequest(r.id)
			r.StartResponse(r.id)
		case 3:
			r.EndResponse(r.id)
			r.rw.RUnlock()
		}
	}
}
func (r *lRequest) req() { r.update(1) }
func (r *lRequest) resp() { r.update(2) }
func (r *lRequest) release() { r.update(3) }

