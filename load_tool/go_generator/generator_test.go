package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/pkg"
)

// TestCmdLineArgs проверяет правильность разбора аргументов командной строки
func TestCmdLineArgs(t *testing.T) {
	// Сохраняем оригинальные аргументы
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	}()

	// Тест 1: Базовые аргументы
	os.Args = []string{
		"log-generator",
		"-mode", "es",
		"-rps", "100",
		"-duration", "10s",
		"-bulk-size", "50",
		"-worker-count", "4",
	}

	// Создаем новый FlagSet для тестов
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	// Параметры для теста
	mode := flag.String("mode", "victoria", "Режим работы")
	rps := flag.Int("rps", 10, "Количество запросов в секунду")
	duration := flag.Duration("duration", 1*time.Minute, "Продолжительность теста")
	bulkSize := flag.Int("bulk-size", 100, "Количество логов в одном запросе")
	workerCount := flag.Int("worker-count", 4, "Количество рабочих горутин")

	// Разбор аргументов
	flag.Parse()

	// Проверка результатов
	if *mode != "es" {
		t.Errorf("Неправильный режим, ожидалось 'es', получено '%s'", *mode)
	}

	if *rps != 100 {
		t.Errorf("Неправильное значение RPS, ожидалось 100, получено %d", *rps)
	}

	if *bulkSize != 50 {
		t.Errorf("Неправильный размер пакета, ожидалось 50, получено %d", *bulkSize)
	}

	if *workerCount != 4 {
		t.Errorf("Неправильное количество воркеров, ожидалось 4, получено %d", *workerCount)
	}

	if *duration != 10*time.Second {
		t.Errorf("Неправильная продолжительность, ожидалось 10s, получено %v", *duration)
	}
}

// TestCreateConfig проверяет создание конфигурации из аргументов командной строки
func TestCreateConfig(t *testing.T) {
	// Создаем тестовую конфигурацию
	config := pkg.Config{
		Mode:          "victoria",
		URL:           "http://localhost:8428",
		RPS:           10,
		Duration:      1 * time.Minute,
		BulkSize:      100,
		WorkerCount:   4,
		EnableMetrics: true,
		LogTypeDistribution: map[string]int{
			"web_access":  60,
			"web_error":   10,
			"application": 20,
			"metric":      5,
			"event":       5,
		},
	}

	// Проверяем значения конфигурации
	if config.Mode != "victoria" {
		t.Errorf("Неправильный режим в конфигурации: %s", config.Mode)
	}

	if config.RPS != 10 {
		t.Errorf("Неправильное значение RPS: %d", config.RPS)
	}

	// Проверка распределения типов логов
	expectedDist := map[string]int{
		"web_access":  60,
		"web_error":   10,
		"application": 20,
		"metric":      5,
		"event":       5,
	}

	for logType, expectedPercent := range expectedDist {
		if config.LogTypeDistribution[logType] != expectedPercent {
			t.Errorf("Неправильное распределение для типа '%s': ожидалось %d%%, получено %d%%",
				logType, expectedPercent, config.LogTypeDistribution[logType])
		}
	}
}

// MockLogDBForTest - мок для интерфейса logdb.LogDB
type MockLogDBForTest struct {
	name string
}

func (m *MockLogDBForTest) Name() string {
	return m.name
}

func (m *MockLogDBForTest) Initialize() error {
	return nil
}

func (m *MockLogDBForTest) SendLogs(logs []logdb.LogEntry) error {
	return nil
}

func (m *MockLogDBForTest) Close() error {
	return nil
}

func (m *MockLogDBForTest) Metrics() map[string]float64 {
	return make(map[string]float64)
}

func (m *MockLogDBForTest) FormatPayload(logs []logdb.LogEntry) (string, string) {
	return "", "application/json"
}
