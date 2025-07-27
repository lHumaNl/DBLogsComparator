package pkg

import (
	"bytes"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// ClientPool - HTTP client pool for connection reuse
type ClientPool struct {
	clients []*http.Client
	mutex   sync.Mutex
	index   int
}

// NewClientPool creates a new HTTP client pool with optimized configuration
func NewClientPool(size int) *ClientPool {
	// If size is 0 or negative, use CPU-based allocation
	if size <= 0 {
		size = runtime.NumCPU() * 8 // More clients for better connection reuse
	}

	pool := &ClientPool{
		clients: make([]*http.Client, size),
	}

	// Optimize connection pool settings for high throughput
	maxConns := size * 10 // More connections per client
	for i := 0; i < size; i++ {
		pool.clients[i] = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:          maxConns,
				MaxIdleConnsPerHost:   maxConns,
				MaxConnsPerHost:       maxConns,
				IdleConnTimeout:       120 * time.Second, // Longer keep-alive
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				DisableKeepAlives:     false, // Ensure keep-alive is enabled
				DisableCompression:    true,  // Disable compression for better performance
			},
			Timeout: 30 * time.Second, // Longer timeout for async operations
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

// BufferPool - optimized buffer pool to minimize memory allocations
type BufferPool struct {
	smallPool sync.Pool // For small buffers (< 1KB)
	largePool sync.Pool // For large buffers (>= 1KB)
}

// NewBufferPool creates a new optimized buffer pool with size-based allocation
func NewBufferPool() *BufferPool {
	return &BufferPool{
		smallPool: sync.Pool{
			New: func() interface{} {
				// Pre-allocate small buffer with 512 bytes capacity
				buf := bytes.NewBuffer(make([]byte, 0, 512))
				return buf
			},
		},
		largePool: sync.Pool{
			New: func() interface{} {
				// Pre-allocate large buffer with 4KB capacity
				buf := bytes.NewBuffer(make([]byte, 0, 4096))
				return buf
			},
		},
	}
}

// Get returns an appropriately sized buffer from the pool
func (p *BufferPool) Get() *bytes.Buffer {
	// Start with small buffer - will grow if needed
	buf := p.smallPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// GetLarge returns a large buffer from the pool for bulk operations
func (p *BufferPool) GetLarge() *bytes.Buffer {
	buf := p.largePool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// Put returns a buffer to the appropriate pool based on its capacity
func (p *BufferPool) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}

	// Reset the buffer and return to appropriate pool
	buf.Reset()

	// If buffer grew large, put it in large pool
	if buf.Cap() >= 1024 {
		// Limit buffer size to prevent memory bloat
		if buf.Cap() > 64*1024 { // 64KB limit
			return // Don't reuse overly large buffers
		}
		p.largePool.Put(buf)
	} else {
		p.smallPool.Put(buf)
	}
}
