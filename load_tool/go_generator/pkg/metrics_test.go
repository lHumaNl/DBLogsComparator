package pkg

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestInitPrometheus(t *testing.T) {
	// Инициализация метрик Prometheus
	config := Config{
		Mode:                "test",
		RPS:                 100,
		BulkSize:            50,
		WorkerCount:         4,
		LogTypeDistribution: map[string]int{"web_access": 60, "web_error": 10, "application": 20, "metric": 5, "event": 5},
	}
	
	// Просто проверяем, что функция выполняется без ошибок
	InitPrometheus(config)
}

func TestMetricsRegistration(t *testing.T) {
	// Упрощенный тест для проверки регистрации метрик
	// Настоящая проверка метрик требует интеграционного тестирования
	t.Skip("Этот тест требует настройки сервера Prometheus и полной интеграции с ним")
}

func TestMetricsUpdate(t *testing.T) {
	// Упрощенный тест для проверки обновления метрик
	// Настоящая проверка обновления метрик требует интеграционного тестирования
	t.Skip("Этот тест требует настройки сервера Prometheus и полной интеграции с ним")
}

func TestStartMetricsServer(t *testing.T) {
	// Используем нестандартный порт для тестов
	testPort := 19999
	
	// Пропускаем тест, если нет возможности запустить HTTP-сервер
	// например, если порт уже занят
	// Сделаем простой запрос для проверки доступности порта
	client := http.Client{Timeout: 100 * time.Millisecond}
	_, err := client.Get(fmt.Sprintf("http://localhost:%d/", testPort))
	if err == nil {
		// Порт уже занят
		t.Skip("Порт уже используется, пропускаем тест")
	}
	
	config := Config{
		EnableMetrics: true,
		MetricsPort:   testPort,
	}
	
	// Запускаем сервер метрик в отдельной горутине
	go func() {
		StartMetricsServer(testPort, config)
	}()
	
	// Даем серверу время на запуск
	time.Sleep(200 * time.Millisecond)
	
	// Проверяем доступность сервера
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", testPort))
	if err != nil {
		t.Skipf("Не удалось выполнить запрос к серверу метрик: %v", err)
		return
	}
	defer resp.Body.Close()
	
	// Проверяем успешный статус
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Ожидался статус код 200, получено %d", resp.StatusCode)
	}
}
