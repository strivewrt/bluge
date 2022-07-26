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

package collector

import (
	"context"

	"github.com/blugelabs/bluge/search"
)

type CollectorConfig struct {
	Sort         search.SortOrder
	SearchAfter  *search.DocumentMatch
	NeededFields []string
	BackingSize  int
}

// MultiSearchCollector collects the top N hits, optionally skipping some results
type MultiSearchCollector struct {
	CC *CollectorConfig
	*TopNCollector
}

// Collect goes to the index to find the matching documents
func (hc *MultiSearchCollector) Collect(ctx context.Context, aggs search.Aggregations,
	searcher search.Collectible) (search.DocumentMatchIterator, error) {
	var err error
	var next *search.DocumentMatch

	// ensure that we always close the searcher
	defer func() {
		_ = searcher.Close()
	}()

	bucket := search.NewBucket("", aggs)

	var hitNumber int
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		next, err = searcher.Next(nil)
	}
	for err == nil && next != nil {
		if hitNumber%CheckDoneEvery == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		hitNumber++
		next.HitNumber = hitNumber

		err = hc.collectSingle(next, bucket)
		if err != nil {
			return nil, err
		}

		next, err = searcher.Next(nil)
	}
	if err != nil {
		return nil, err
	}

	bucket.Finish()

	// finalize actual results
	err = hc.finalizeResults()
	if err != nil {
		return nil, err
	}

	rv := &TopNIterator{
		results: hc.results,
		bucket:  bucket,
		index:   0,
		err:     nil,
	}
	return rv, nil
}

func (hc *MultiSearchCollector) collectSingle(d *search.DocumentMatch, bucket *search.Bucket) error {
	//var err error

	//if len(hc.CC.NeededFields) > 0 {
	//	err = d.LoadDocumentValues(d.Context, hc.CC.NeededFields)
	//	if err != nil {
	//		return err
	//	}
	//}

	// compute this hits sort value
	hc.sort.Compute(d)

	// calculate aggregations
	bucket.Consume(d)

	// support search after based pagination,
	// if this hit is <= the search after sort key
	// we should skip it
	if hc.searchAfter != nil {
		if hc.sort.CompareSearchAfter(d, hc.searchAfter) <= 0 {
			return nil
		}
	}

	// optimization, we track lowest sorting hit already removed from heap
	// with this one comparison, we can avoid all heap operations if
	// this hit would have been added and then immediately removed
	if hc.lowestMatchOutsideResults != nil {
		cmp := hc.sort.Compare(d, hc.lowestMatchOutsideResults)
		if cmp >= 0 {
			// this hit can't possibly be in the result set, so avoid heap ops
			d.Context.DocumentMatchPool.Put(d)
			return nil
		}
	}

	removed := hc.store.AddNotExceedingSize(d, hc.size+hc.skip)
	if removed != nil {
		if hc.lowestMatchOutsideResults == nil {
			hc.lowestMatchOutsideResults = removed
		} else {
			cmp := hc.sort.Compare(removed, hc.lowestMatchOutsideResults)
			if cmp < 0 {
				tmp := hc.lowestMatchOutsideResults
				hc.lowestMatchOutsideResults = removed
				d.Context.DocumentMatchPool.Put(tmp)
			}
		}
	}
	return nil
}
