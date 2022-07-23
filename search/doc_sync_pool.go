//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"sync"
)

// DocumentMatchSyncPool manages use/re-use of DocumentMatch instances
// for searchers that are concurrent, avoiding reuse of the SortValue backing array
// as seen in DocumentMatchSlicePool
// it pre-allocates space in a sync.Pool with the expected number of instances.
// It is thread-safe.
type DocumentMatchSyncPool struct {
	size int64
	pool *sync.Pool
}

// NewDocumentMatchSyncPool will build a DocumentMatchSyncPool with memory
// pre-allocated to accommodate the requested number of DocumentMatch
// instances
func NewDocumentMatchSyncPool(size, sortSize int) *DocumentMatchSyncPool {
	pool := &sync.Pool{
		New: func() interface{} {
			return &DocumentMatch{
				SortValue: make([][]byte, 0, sortSize),
			}
		},
	}

	var poolSize int64
	d := &DocumentMatch{SortValue: make([][]byte, 0, sortSize)}
	poolSize = int64(d.Size() * size) // calculate pool size once
	pool.Put(d)

	// pre-allocate the expected number of instances
	// make these initial instances available in the pool
	for i := 1; i < size; i++ { // start from i := 1, since we have added one above
		pool.Put(&DocumentMatch{
			SortValue: make([][]byte, 0, sortSize),
		})
	}

	return &DocumentMatchSyncPool{pool: pool, size: poolSize}
}

// Get returns an available DocumentMatch from the pool
// if the pool was not allocated with sufficient size, an allocation will
// occur to satisfy this request.  As a side-effect this will grow the size
// of the pool.
func (p *DocumentMatchSyncPool) Get() *DocumentMatch {
	return p.pool.Get().(*DocumentMatch)
}

// Size returns the size of the DocumentMatchSyncPool
func (p *DocumentMatchSyncPool) Size() int64 {
	return p.size
}

// Put returns a DocumentMatch to the pool
func (p *DocumentMatchSyncPool) Put(d *DocumentMatch) {
	if d == nil {
		return
	}

	d.Reset() // reset DocumentMatch before returning it to available pool
	p.pool.Put(d)
}
