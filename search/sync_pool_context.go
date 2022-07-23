package search

import (
	"sync"

	segment "github.com/blugelabs/bluge_segment_api"
)

// syncPoolContext represents the context around a single search
// It is safe for concurrent use.
type syncPoolContext struct {
	lock              sync.RWMutex // prevent data race on dvReaders
	DocumentMatchPool *DocumentMatchSyncPool
	dvReaders         map[DocumentValueReadable]segment.DocumentValueReader
}

func newSearchSyncPoolContext(size, sortSize int) Context {
	return &syncPoolContext{
		DocumentMatchPool: NewDocumentMatchSyncPool(size, sortSize),
		dvReaders:         make(map[DocumentValueReadable]segment.DocumentValueReader, size/2),
	}
}

func (sc *syncPoolContext) DocValueReaderForReader(r DocumentValueReadable, fields []string) (segment.DocumentValueReader, error) {
	sc.lock.RLock()
	dvReader := sc.dvReaders[r]
	sc.lock.RUnlock()

	if dvReader == nil {
		var err error
		dvReader, err = r.DocumentValueReader(fields)
		if err != nil {
			return nil, err
		}

		sc.lock.Lock()
		sc.dvReaders[r] = dvReader
		sc.lock.Unlock()
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
