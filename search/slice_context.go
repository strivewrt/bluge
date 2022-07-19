package search

import (
	segment "github.com/blugelabs/bluge_segment_api"
)

// sliceContext represents the context around a single search
// It is not safe for concurrent use.
type sliceContext struct {
	DocumentMatchPool *DocumentMatchSlicePool
	dvReaders         map[DocumentValueReadable]segment.DocumentValueReader
}

func newSliceContext(size, sortSize int) *sliceContext {
	return &sliceContext{
		DocumentMatchPool: NewDocumentMatchSlicePool(size, sortSize),
		dvReaders:         make(map[DocumentValueReadable]segment.DocumentValueReader),
	}
}

type PoolType string

const (
	PoolTypeSlice    PoolType = "slice"
	PoolTypeSyncPool PoolType = "sync_pool"
)

func NewSearchContext(size, sortSize int, poolType PoolType) Context {
	switch poolType {
	case PoolTypeSlice:
		return newSliceContext(size, sortSize)
	case PoolTypeSyncPool:
		return newSearchSyncPoolContext(size, sortSize)
	default:
		panic("unknown pool type: " + poolType)
	}
}

func (sc *sliceContext) DocValueReaderForReader(r DocumentValueReadable, fields []string) (segment.DocumentValueReader, error) {
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

// Size returns the sliceContext 's size in memory.
func (sc *sliceContext) Size() int64 {
	sizeInBytes := reflectStaticSizeSliceSearchContext + sizeOfPtr +
		reflectStaticSizeDocumentMatchSlicePool + sizeOfPtr

	if sc.DocumentMatchPool != nil {
		for _, entry := range sc.DocumentMatchPool.avail {
			if entry != nil {
				sizeInBytes += entry.Size()
			}
		}
	}

	return int64(sizeInBytes)
}

func (sc *sliceContext) GetDocumentMatchFromPool() *DocumentMatch {
	return sc.DocumentMatchPool.Get()
}

func (sc *sliceContext) PutDocumentMatchInPool(d *DocumentMatch) {
	sc.DocumentMatchPool.Put(d)
}
