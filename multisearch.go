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
	"log"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/blugelabs/bluge/search"
)

type MultiSearcherList struct {
	searchers []search.Searcher
	docChan   chan *search.DocumentMatch
	once      sync.Once
}

func NewMultiSearcherList(searchers []search.Searcher) *MultiSearcherList {
	return &MultiSearcherList{
		searchers: searchers,
		docChan:   make(chan *search.DocumentMatch, len(searchers)*2),
	}
}

// if one searcher fails, should stop all the rest and exit?
func (m *MultiSearcherList) collectAllDocuments(ctx search.Context) {
	errs := errgroup.Group{}
	errs.SetLimit(1000)
	for i := range m.searchers {
		j := i
		errs.Go(func() error {
			dm, err := m.searchers[j].Next(ctx)

			for err == nil && dm != nil {
				m.docChan <- dm
				dm, err = m.searchers[j].Next(ctx)
			}

			return err
		})
	}

	err := errs.Wait()
	if err != nil {
		log.Printf("multisearcher failed: %s", err.Error())
	}

	close(m.docChan)
}

func (m *MultiSearcherList) Next(ctx search.Context) (*search.DocumentMatch, error) {
	m.once.Do(func() {
		go m.collectAllDocuments(ctx)
	})

	return <-m.docChan, nil
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
	searchers := make([]search.Searcher, 0, len(readers))
	for _, reader := range readers {
		searcher, err := req.Searcher(reader.reader, reader.config)
		if err != nil {
			return nil, err
		}
		searchers = append(searchers, searcher)
	}

	collector := req.Collector()
	msl := NewMultiSearcherList(searchers)

	start := time.Now()
	dmItr, err := collector.Collect(ctx, req.Aggregations(), msl, search.PoolTypeSyncPool)
	if err != nil {
		return nil, err
	}
	log.Printf("multisearch query time: %dms", time.Since(start).Microseconds())

	return dmItr, nil
}
