package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/pkg"
)

// TestMain - точка входа для запуска тестов
func TestMain(m *testing.M) {
	// Здесь можно выполнить подготовку перед запуском тестов
	setupTestEnvironment()

	// Запуск тестов
	code := m.Run()

	// Очистка после выполнения тестов
	teardownTestEnvironment()

	// Выход с кодом выполнения тестов
	os.Exit(code)
}

// Настройка тестового окружения
func setupTestEnvironment() {
	// Здесь можно подготовить тестовое окружение:
	// - Установить переменные окружения
	// - Инициализировать ресурсы
	// - Создать временные файлы
}

// Очистка тестового окружения
func teardownTestEnvironment() {
	// Здесь можно очистить тестовое окружение:
	// - Удалить временные файлы
	// - Освободить ресурсы
}

// Интеграционный тест для проверки основного функционала
func TestLogGeneratorIntegration(t *testing.T) {
	// Пропускаем этот тест при обычном выполнении,
	// так как он требует внешних зависимостей и настроенного окружения
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Интеграционные тесты пропущены. Установите RUN_INTEGRATION_TESTS=true для их запуска.")
	}

	// Создаем тестовую конфигурацию с коротким временем выполнения
	config := pkg.Config{
		Mode:        "mock",                  // Используем mock для тестирования
		BaseURL:     "http://localhost:8000", // Адрес мок-сервера
		URL:         "http://localhost:8000",
		RPS:         10,
		Duration:    100 * time.Millisecond, // Короткая продолжительность для теста
		BulkSize:    5,
		WorkerCount: 2,
		LogTypeDistribution: map[string]int{
			"web_access":  60,
			"web_error":   10,
			"application": 20,
			"metric":      5,
			"event":       5,
		},
		Verbose:       true,
		MaxRetries:    1,
		RetryDelay:    50 * time.Millisecond,
		EnableMetrics: false,
	}

	// Создаем мок базы данных для тестирования
	mockDB := NewMockLogDB("integration_test")

	// Запускаем генератор с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan error)
	go func() {
		err := pkg.RunGenerator(config, mockDB)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Ошибка при запуске генератора: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Тест превысил время ожидания")
	}

	// Проверяем, что логи были отправлены
	if mockDB.sendCalled == 0 {
		t.Error("Не было вызовов для отправки логов")
	}
}

// MockLogDB для тестов интеграции
type MockLogDB struct {
	name       string
	sendCalled int
	metrics    map[string]float64
}

// Создаем новый экземпляр MockLogDB для интеграционных тестов
func NewMockLogDB(name string) *MockLogDB {
	return &MockLogDB{
		name:       name,
		sendCalled: 0,
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
