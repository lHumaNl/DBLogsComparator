package pkg

import (
	"bytes"
	"net/http"
	"sync"
	"time"
)

// ClientPool - пул HTTP-клиентов для переиспользования соединений
type ClientPool struct {
	clients []*http.Client
	mutex   sync.Mutex
	index   int
}

// NewClientPool создает новый пул HTTP-клиентов
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

// Get возвращает HTTP-клиент из пула
func (p *ClientPool) Get() *http.Client {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	client := p.clients[p.index]
	p.index = (p.index + 1) % len(p.clients)
	return client
}

// BufferPool - пул буферов для минимизации аллокаций памяти
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool создает новый пул буферов
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get возвращает буфер из пула
func (p *BufferPool) Get() *bytes.Buffer {
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// Put возвращает буфер в пул
func (p *BufferPool) Put(buf *bytes.Buffer) {
	p.pool.Put(buf)
}
