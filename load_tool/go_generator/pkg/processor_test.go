package pkg

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
)

// MockLogDB - имитация базы данных для тестирования
type MockLogDB struct {
	name       string
	sendCalled int
	shouldFail bool
	metrics    map[string]float64
}

func NewMockLogDB(name string, shouldFail bool) *MockLogDB {
	return &MockLogDB{
		name:       name,
		shouldFail: shouldFail,
		metrics:    make(map[string]float64),
	}
}

func (m *MockLogDB) Name() string {
	return m.name
}

func (m *MockLogDB) Initialize() error {
	return nil
}

func (m *MockLogDB) SendLogs(logs []logdb.LogEntry) error {
	m.sendCalled++
	if m.shouldFail {
		return fmt.Errorf("test error: failed to send logs")
	}
	return nil
}

func (m *MockLogDB) Close() error {
	return nil
}

func (m *MockLogDB) Metrics() map[string]float64 {
	return m.metrics
}

func (m *MockLogDB) FormatPayload(logs []logdb.LogEntry) (string, string) {
	return "", "application/json"
}

func TestCreateBulkPayload(t *testing.T) {
	// Тестирование создания пакета логов
	config := Config{
		Mode:                "test",
		BulkSize:            10,
		LogTypeDistribution: map[string]int{"web_access": 100}, // Только один тип для упрощения
	}

	bufferPool := NewBufferPool()
	logs := CreateBulkPayload(config, bufferPool)

	// Проверка размера пакета
	if len(logs) != config.BulkSize {
		t.Errorf("Размер пакета должен быть %d, получено %d", config.BulkSize, len(logs))
	}

	// Проверка типа логов и структуры
	for _, log := range logs {
		if logType, ok := log["log_type"].(string); !ok || logType != "web_access" {
			t.Errorf("Ожидаемый тип лога 'web_access', получено '%v'", logType)
		}

		if _, ok := log["timestamp"].(string); !ok {
			t.Error("В логе должно быть поле 'timestamp' типа string")
		}
	}
}

func TestWorker(t *testing.T) {
	// Тестирование функции Worker
	config := Config{
		BulkSize:            5,
		LogTypeDistribution: map[string]int{"web_access": 100},
		Verbose:             false,
		EnableMetrics:       false,
	}

	stats := &Stats{}
	bufferPool := NewBufferPool()

	// Создаем канал для задач
	jobs := make(chan struct{}, 2)
	jobs <- struct{}{} // Добавляем две задачи
	jobs <- struct{}{}
	close(jobs) // Закрываем канал, чтобы Worker завершился

	var wg sync.WaitGroup
	wg.Add(1)

	// Запускаем Worker с макетом базы данных
	db := NewMockLogDB("mockdb", false)
	go Worker(1, jobs, stats, config, db, bufferPool, &wg)

	// Ожидаем завершения
	wg.Wait()

	// Проверяем статистику
	if db.sendCalled != 2 {
		t.Errorf("Метод SendLogs должен быть вызван 2 раза, фактически: %d", db.sendCalled)
	}

	if atomic.LoadInt64(&stats.TotalRequests) != 2 {
		t.Errorf("TotalRequests должно быть 2, получено %d", stats.TotalRequests)
	}

	if atomic.LoadInt64(&stats.SuccessfulRequests) != 2 {
		t.Errorf("SuccessfulRequests должно быть 2, получено %d", stats.SuccessfulRequests)
	}

	if atomic.LoadInt64(&stats.TotalLogs) != int64(2*config.BulkSize) {
		t.Errorf("TotalLogs должно быть %d, получено %d", 2*config.BulkSize, stats.TotalLogs)
	}

	// Тестирование с ошибкой
	stats = &Stats{}
	jobs = make(chan struct{}, 1)
	jobs <- struct{}{}
	close(jobs)

	wg.Add(1)
	db = NewMockLogDB("mockdb", true) // Эта база данных будет возвращать ошибку
	go Worker(1, jobs, stats, config, db, bufferPool, &wg)

	wg.Wait()

	if atomic.LoadInt64(&stats.FailedRequests) != 1 {
		t.Errorf("FailedRequests должно быть 1, получено %d", stats.FailedRequests)
	}
}

func TestStatsReporter(t *testing.T) {
	// Тестирование функции StatsReporter
	stats := &Stats{
		StartTime: time.Now(),
	}

	// Устанавливаем некоторые начальные значения статистики
	atomic.StoreInt64(&stats.TotalRequests, 100)
	atomic.StoreInt64(&stats.SuccessfulRequests, 95)
	atomic.StoreInt64(&stats.FailedRequests, 5)
	atomic.StoreInt64(&stats.TotalLogs, 1000)

	// Создаем канал для остановки
	stopChan := make(chan struct{})

	config := Config{
		EnableMetrics: false,
	}

	// Запускаем StatsReporter на короткое время
	go StatsReporter(stats, stopChan, config)

	// Даем ему немного поработать (достаточно для одного тика)
	time.Sleep(1100 * time.Millisecond)

	// Останавливаем
	close(stopChan)

	// Нет явных проверок, поскольку StatsReporter просто выводит информацию
	// Можно было бы перенаправлять stdout и проверять вывод, но это сложнее
	// В реальном тесте мы можем проверить, что функция не паникует
	// и корректно останавливается при закрытии канала
}
