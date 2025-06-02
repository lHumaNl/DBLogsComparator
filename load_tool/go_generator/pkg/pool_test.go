package pkg

import (
	"bytes"
	"sync"
	"testing"
)

func TestBufferPool(t *testing.T) {
	// Initialize buffer pool
	pool := NewBufferPool()

	// Get buffer from pool
	buf := pool.Get()
	if buf == nil {
		t.Fatal("BufferPool.Get() returned nil buffer")
	}

	// Check that buffer is empty
	if buf.Len() != 0 {
		t.Errorf("New buffer from pool should be empty, size: %d", buf.Len())
	}

	// Write data to buffer
	testData := "Test data for buffer"
	_, err := buf.WriteString(testData)
	if err != nil {
		t.Fatalf("Error writing to buffer: %v", err)
	}

	// Check buffer content
	if buf.String() != testData {
		t.Errorf("Expected buffer content '%s', got '%s'", testData, buf.String())
	}

	// Return buffer to pool
	pool.Put(buf)

	// Get another buffer
	buf2 := pool.Get()
	if buf2 == nil {
		t.Fatal("BufferPool.Get() returned nil buffer on second call")
	}

	// Check that buffer was reset
	if buf2.Len() != 0 {
		t.Errorf("Buffer after return to pool should be reset, size: %d", buf2.Len())
	}
}

func TestBufferPoolConcurrent(t *testing.T) {
	// Test buffer pool with concurrent usage
	pool := NewBufferPool()
	const goroutines = 100
	const iterations = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				// Get buffer
				buf := pool.Get()
				if buf == nil {
					t.Errorf("Goroutine %d: BufferPool.Get() returned nil in iteration %d", id, j)
					continue
				}

				// Write data
				data := []byte("Test data from goroutine")
				_, err := buf.Write(data)
				if err != nil {
					t.Errorf("Goroutine %d: Error writing data: %v", id, err)
				}

				// Check data
				if !bytes.Contains(buf.Bytes(), data) {
					t.Errorf("Goroutine %d: Data was not written correctly", id)
				}

				// Return buffer
				pool.Put(buf)
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkBufferPool(b *testing.B) {
	pool := NewBufferPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf.WriteString("Benchmark test data that simulates a typical log entry with some realistic content")
		pool.Put(buf)
	}
}

// Comparative test to evaluate buffer pool performance
func BenchmarkBufferWithoutPool(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := &bytes.Buffer{}
		buf.WriteString("Benchmark test data that simulates a typical log entry with some realistic content")
	}
}
