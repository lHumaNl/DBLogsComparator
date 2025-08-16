package pkg

import (
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
)

// ClientPool - HTTP client pool for connection reuse in querier
type ClientPool struct {
	clients     []*http.Client
	atomicIndex uint64
}

// NewClientPool creates a new HTTP client pool with optimized configuration for querier
func NewClientPool(size int, timeout time.Duration) *ClientPool {
	// If size is 0 or negative, use CPU-based allocation
	size = runtime.NumCPU() / 2 // Conservative approach for queries

	if size < 16 {
		size = 16
	}

	pool := &ClientPool{
		clients: make([]*http.Client, size),
	}

	// Optimize connection pool settings for query workload
	for i := 0; i < size; i++ {
		pool.clients[i] = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				MaxConnsPerHost:       100,
				IdleConnTimeout:       30 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				DisableKeepAlives:     false,
			},
			Timeout: timeout,
		}
	}

	return pool
}

// Get returns an HTTP client from the pool using round-robin
func (p *ClientPool) Get() *http.Client {
	index := atomic.AddUint64(&p.atomicIndex, 1)
	return p.clients[index%uint64(len(p.clients))]
}

// Size returns the size of the pool
func (p *ClientPool) Size() int {
	return len(p.clients)
}
