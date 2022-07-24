package search

import (
	segment "github.com/blugelabs/bluge_segment_api"
)

// syncPoolContext represents the context around a single search
// It is safe for concurrent use.
type syncPoolContext struct {
	DocumentMatchPool *DocumentMatchSyncPool
	dvReaders         map[DocumentValueReadable]segment.DocumentValueReader
}

func newSearchSyncPoolContext(size, sortSize int) Context {
	return &syncPoolContext{
		DocumentMatchPool: NewDocumentMatchSyncPool(size, sortSize),
		dvReaders:         make(map[DocumentValueReadable]segment.DocumentValueReader),
	}
}

func (sc *syncPoolContext) DocValueReaderForReader(r DocumentValueReadable, fields []string) (segment.DocumentValueReader, error) {
	dvReader := sc.dvReaders[r]

	if dvReader == nil {
		var err error
		dvReader, err = r.DocumentValueReader(fields)
		if err != nil {
			return nil, err
		}

		sc.dvReaders[r] = dvReader
	}
	return dvReader, nil
}

// Size returns the search syncPoolContext's size in memory.
func (sc *syncPoolContext) Size() int64 {
	sizeInBytes := reflectStaticSizeSyncPoolSearchContext + sizeOfPtr +
		reflectStaticSizeDocumentMatchSyncPool + sizeOfPtr

	var dpSize int64
	if sc.DocumentMatchPool != nil {
		dpSize = sc.DocumentMatchPool.Size()
	}

	totalSize := dpSize + int64(sizeInBytes)
	return totalSize
}

func (sc *syncPoolContext) GetDocumentMatchFromPool() *DocumentMatch {
	return sc.DocumentMatchPool.Get()
}

func (sc *syncPoolContext) PutDocumentMatchInPool(d *DocumentMatch) {
	sc.DocumentMatchPool.Put(d)
}
