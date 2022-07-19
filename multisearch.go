//  Copyright (c) 2020 The Bluge Authors.
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

package bluge

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/blugelabs/bluge/search"
)

type MultiSearcherList struct {
	index              int
	searchers          []search.Searcher
	docs               []*search.DocumentMatch
	docChan            chan *search.DocumentMatch
	finishedCollecting chan struct{}
	once               sync.Once
}

func NewMultiSearcherList(searchers []search.Searcher) *MultiSearcherList {
	return &MultiSearcherList{
		searchers:          searchers,
		docs:               make([]*search.DocumentMatch, 0, len(searchers)*10),
		docChan:            make(chan *search.DocumentMatch, len(searchers)*10),
		finishedCollecting: make(chan struct{}),
	}
}

// if one searcher fails, should stop all the rest and exit?
func (m *MultiSearcherList) collectAllDocuments(ctx search.Context) {
	errs := &multierror.Group{}

	var numFailed int64 = 0
	for _, searcher := range m.searchers {
		s := searcher
		errs.Go(func() error {
			dm, err := s.Next(ctx)

			for err == nil && dm != nil {
				m.docChan <- dm
				dm, err = s.Next(ctx)
			}

			if err != nil {
				atomic.AddInt64(&numFailed, 1)
				return err
			}

			return nil
		})
	}

	multiErr := errs.Wait().ErrorOrNil()
	close(m.docChan)

	if multiErr != nil {
		log.Printf("%d searchers failed errored, errors: %v", numFailed, multiErr.Error())
	}
}

// A dilemma here, should MultiSearcherList.Next listen on finishedCollecting and then begin dispersing documents?
// or just return documents directly from docChan?
func (m *MultiSearcherList) storeDocs() {
	for {
		match, ok := <-m.docChan
		if !ok {
			close(m.finishedCollecting)
			return
		}

		m.docs = append(m.docs, match)
	}
}

func (m *MultiSearcherList) Next(ctx search.Context) (*search.DocumentMatch, error) {
	m.once.Do(func() {
		go m.collectAllDocuments(ctx)
		go m.storeDocs() // see comment on storeDocs
		<-m.finishedCollecting
	})

	if m.index < len(m.docs) {
		dm := m.docs[m.index]
		m.index++
		return dm, nil
	}

	return nil, nil
}

func (m *MultiSearcherList) DocumentMatchPoolSize() int {
	// we search sequentially, so just use largest
	var rv int
	for _, searcher := range m.searchers {
		ps := searcher.DocumentMatchPoolSize()
		if ps > rv {
			rv = ps
		}
	}
	return rv
}

func (m *MultiSearcherList) Close() (err error) {
	for _, searcher := range m.searchers {
		cerr := searcher.Close()
		if err == nil {
			err = cerr
		}
	}
	return err
}

func MultiSearch(ctx context.Context, req SearchRequest, readers ...*Reader) (search.DocumentMatchIterator, error) {
	collector := req.Collector()

	var searchers []search.Searcher
	start := time.Now()
	for _, reader := range readers {
		searcher, err := req.Searcher(reader.reader, reader.config)
		if err != nil {
			return nil, err
		}
		searchers = append(searchers, searcher)
	}
	fmt.Println("time spent arranging readers: ", time.Since(start).Milliseconds())

	msl := NewMultiSearcherList(searchers)
	dmItr, err := collector.Collect(ctx, req.Aggregations(), msl)
	if err != nil {
		return nil, err
	}

	return dmItr, nil
}
