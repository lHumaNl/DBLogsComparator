package pkg

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
)

// CreateBulkPayload создает пакет логов для отправки в базу данных
func CreateBulkPayload(config Config, bufferPool *BufferPool) []logdb.LogEntry {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	logs := make([]logdb.LogEntry, 0, config.BulkSize)

	for i := 0; i < config.BulkSize; i++ {
		logType := SelectRandomLogType(config.LogTypeDistribution)
		logObject := GenerateLog(logType, timestamp)

		// Преобразуем в JSON и обратно в map[string]interface{}
		logJSON, err := json.Marshal(logObject)
		if err != nil {
			continue
		}

		var logEntry logdb.LogEntry
		if err := json.Unmarshal(logJSON, &logEntry); err != nil {
			continue
		}

		logs = append(logs, logEntry)
	}

	return logs
}

// Worker обрабатывает задачи из канала jobs
func Worker(id int, jobs <-chan struct{}, stats *Stats, config Config, db logdb.LogDB, bufferPool *BufferPool, wg *sync.WaitGroup) {
	defer wg.Done()

	for range jobs {
		// Создаем пакет логов
		logs := CreateBulkPayload(config, bufferPool)

		// Отправляем логи в базу данных
		startTime := time.Now()
		err := db.SendLogs(logs)
		duration := time.Since(startTime).Seconds()

		// Обновляем метрики и статистику
		if err != nil {
			atomic.AddInt64(&stats.FailedRequests, 1)
			if config.Verbose {
				fmt.Printf("Worker %d: Error sending logs: %v\n", id, err)
			}

			if config.EnableMetrics {
				RequestDuration.WithLabelValues("error", db.Name()).Observe(duration)
				RequestsTotal.WithLabelValues("error", db.Name()).Inc()
			}
		} else {
			atomic.AddInt64(&stats.SuccessfulRequests, 1)
			atomic.AddInt64(&stats.TotalLogs, int64(len(logs)))

			if config.EnableMetrics {
				RequestDuration.WithLabelValues("success", db.Name()).Observe(duration)
				RequestsTotal.WithLabelValues("success", db.Name()).Inc()

				// Увеличиваем счетчики для каждого типа лога
				logCounts := make(map[string]int)
				for _, log := range logs {
					if logType, ok := log["log_type"].(string); ok {
						logCounts[logType]++
					}
				}

				for logType, count := range logCounts {
					LogsTotal.WithLabelValues(logType, db.Name()).Add(float64(count))
				}
			}
		}

		atomic.AddInt64(&stats.TotalRequests, 1)
	}
}

// StatsReporter выводит статистику работы
func StatsReporter(stats *Stats, stopChan <-chan struct{}, config Config) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Расчет текущих значений
			elapsed := time.Since(stats.StartTime).Seconds()
			totalReqs := atomic.LoadInt64(&stats.TotalRequests)
			successReqs := atomic.LoadInt64(&stats.SuccessfulRequests)
			failedReqs := atomic.LoadInt64(&stats.FailedRequests)
			totalLogs := atomic.LoadInt64(&stats.TotalLogs)
			retriedReqs := atomic.LoadInt64(&stats.RetriedRequests)

			currentRPS := float64(totalReqs) / elapsed
			currentLPS := float64(totalLogs) / elapsed

			// Обновление метрик
			if config.EnableMetrics {
				RPSGauge.Set(currentRPS)
				LPSGauge.Set(currentLPS)
			}

			// Вывод статистики
			fmt.Printf("\rВремя: %.1f с | Запросы: %d (%.1f/с) | Логи: %d (%.1f/с) | Успех: %.1f%% | Ошибки: %.1f%% | Повторы: %d  ",
				elapsed, totalReqs, currentRPS, totalLogs, currentLPS,
				float64(successReqs)/float64(totalReqs+1)*100,
				float64(failedReqs)/float64(totalReqs+1)*100,
				retriedReqs)

		case <-stopChan:
			return
		}
	}
}

// RunGenerator запускает генератор логов
func RunGenerator(config Config, db logdb.LogDB) error {
	// Инициализация статистики
	stats := &Stats{
		StartTime: time.Now(),
	}

	// Инициализация пулов
	bufferPool := NewBufferPool()

	// Инициализация каналов и горутин
	jobs := make(chan struct{}, config.RPS*2)
	stopChan := make(chan struct{})

	// Запуск сервера метрик, если включено
	if config.EnableMetrics {
		StartMetricsServer(config.MetricsPort, config)
		InitPrometheus(config)
	}

	// Запуск горутины для отображения статистики
	go StatsReporter(stats, stopChan, config)

	// Запуск рабочих горутин
	var wg sync.WaitGroup
	for w := 1; w <= config.WorkerCount; w++ {
		wg.Add(1)
		go Worker(w, jobs, stats, config, db, bufferPool, &wg)
	}

	// Основной цикл генерации нагрузки
	tickInterval := time.Second / time.Duration(config.RPS)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	endTime := time.Now().Add(config.Duration)

	for time.Now().Before(endTime) {
		<-ticker.C
		jobs <- struct{}{}
	}

	close(jobs)
	wg.Wait()
	close(stopChan)

	// Вывод итоговой статистики
	elapsed := time.Since(stats.StartTime).Seconds()
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	successReqs := atomic.LoadInt64(&stats.SuccessfulRequests)
	failedReqs := atomic.LoadInt64(&stats.FailedRequests)
	totalLogs := atomic.LoadInt64(&stats.TotalLogs)
	retriedReqs := atomic.LoadInt64(&stats.RetriedRequests)

	currentRPS := float64(totalReqs) / elapsed
	currentLPS := float64(totalLogs) / elapsed

	fmt.Printf("\n\nТест завершен!\n")
	fmt.Printf("Продолжительность: %.2f секунд\n", elapsed)
	fmt.Printf("Всего запросов: %d (%.2f/с)\n", totalReqs, currentRPS)
	fmt.Printf("Всего логов: %d (%.2f/с)\n", totalLogs, currentLPS)
	fmt.Printf("Успешных запросов: %d (%.2f%%)\n", successReqs, float64(successReqs)/float64(totalReqs)*100)
	fmt.Printf("Неудачных запросов: %d (%.2f%%)\n", failedReqs, float64(failedReqs)/float64(totalReqs)*100)
	fmt.Printf("Повторных попыток: %d\n", retriedReqs)

	return nil
}
