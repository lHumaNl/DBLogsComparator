package pkg

import (
	"bytes"
	"net/http"
	"sync"
	"time"
)

// ClientPool - HTTP client pool for connection reuse
type ClientPool struct {
	clients []*http.Client
	mutex   sync.Mutex
	index   int
}

// NewClientPool creates a new HTTP client pool
func NewClientPool(size int) *ClientPool {
	pool := &ClientPool{
		clients: make([]*http.Client, size),
	}

	for i := 0; i < size; i++ {
		pool.clients[i] = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: 10 * time.Second,
		}
	}

	return pool
}

// Get returns an HTTP client from the pool
func (p *ClientPool) Get() *http.Client {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	client := p.clients[p.index]
	p.index = (p.index + 1) % len(p.clients)
	return client
}

// BufferPool - buffer pool to minimize memory allocations
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a new buffer pool
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get returns a buffer from the pool
func (p *BufferPool) Get() *bytes.Buffer {
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// Put returns a buffer to the pool
func (p *BufferPool) Put(buf *bytes.Buffer) {
	p.pool.Put(buf)
}
