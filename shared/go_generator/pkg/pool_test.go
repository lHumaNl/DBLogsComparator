package pkg

import (
	"bytes"
	"sync"
	"testing"
)

func TestBufferPool(t *testing.T) {
	// Инициализация пула буферов
	pool := NewBufferPool()
	
	// Получение буфера из пула
	buf := pool.Get()
	if buf == nil {
		t.Fatal("BufferPool.Get() returned nil buffer")
	}
	
	// Проверка, что буфер пуст
	if buf.Len() != 0 {
		t.Errorf("Новый буфер из пула должен быть пуст, размер: %d", buf.Len())
	}
	
	// Запись данных в буфер
	testData := "Test data for buffer"
	_, err := buf.WriteString(testData)
	if err != nil {
		t.Fatalf("Ошибка записи в буфер: %v", err)
	}
	
	// Проверка содержимого буфера
	if buf.String() != testData {
		t.Errorf("Ожидалось содержимое буфера '%s', получено '%s'", testData, buf.String())
	}
	
	// Возврат буфера в пул
	pool.Put(buf)
	
	// Получение другого буфера
	buf2 := pool.Get()
	if buf2 == nil {
		t.Fatal("BufferPool.Get() returned nil buffer on second call")
	}
	
	// Проверка, что буфер был сброшен
	if buf2.Len() != 0 {
		t.Errorf("Буфер после возврата в пул должен быть сброшен, размер: %d", buf2.Len())
	}
}

func TestBufferPoolConcurrent(t *testing.T) {
	// Тестирование пула буферов при конкурентном использовании
	pool := NewBufferPool()
	const goroutines = 100
	const iterations = 10
	
	var wg sync.WaitGroup
	wg.Add(goroutines)
	
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < iterations; j++ {
				// Получение буфера
				buf := pool.Get()
				if buf == nil {
					t.Errorf("Горутина %d: BufferPool.Get() вернула nil в итерации %d", id, j)
					continue
				}
				
				// Запись данных
				data := []byte("Test data from goroutine")
				_, err := buf.Write(data)
				if err != nil {
					t.Errorf("Горутина %d: Ошибка записи данных: %v", id, err)
				}
				
				// Проверка данных
				if !bytes.Contains(buf.Bytes(), data) {
					t.Errorf("Горутина %d: Данные не были записаны корректно", id)
				}
				
				// Возврат буфера
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

// Сравнительный тест для оценки производительности пула буферов
func BenchmarkBufferWithoutPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := &bytes.Buffer{}
		buf.WriteString("Benchmark test data that simulates a typical log entry with some realistic content")
		// без возврата в пул буфер будет собран сборщиком мусора
	}
}
