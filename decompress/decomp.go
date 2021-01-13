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
A decompression plugin system. This is used for the TRADGROUP code, because
<CONFIG>/active and <CONFIG>/newsgroups have compressed counterparts,
one in the ".bz2" format and one in ".gz", which should be supported.

Also, i prefer to use github.com/klauspost/compress for more speed.
*/
package decompress

import "io"
import "strings"

type Decoder func(io.ReadCloser) (io.ReadCloser,error)

type prioDecoder struct{
	prio int
	deco Decoder
}

func empty(r io.ReadCloser) (io.ReadCloser,error) { return r, nil }

var registry = make(map[string]prioDecoder)

func Register(ext string,deco Decoder, prio int) {
	dec := registry[ext]
	if dec.prio>=prio { return }
	dec.prio = prio
	dec.deco = deco
	registry[ext] = dec
}

/*
Returns the matching Decoder or nil if not supported.

ext is the file extension like ".gz", ".bz2", or, if no compression is used, "".
*/
func Get(ext string) Decoder {  return registry[strings.ToLower(ext)].deco }

func init() {
	Register("",empty,1<<20)
}

