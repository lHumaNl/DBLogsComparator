package common

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Stats представляет статистику выполнения тестов
type Stats struct {
	StartTime           time.Time
	TotalRequests       int64 // Общее количество запросов на запись
	SuccessfulRequests  int64 // Успешные запросы на запись
	FailedRequests      int64 // Неудачные запросы на запись
	TotalLogs           int64 // Общее количество отправленных логов
	RetriedRequests     int64 // Повторные попытки запросов на запись
	
	// Статистика запросов на чтение
	TotalQueries        int64 // Общее количество запросов на чтение
	SuccessfulQueries   int64 // Успешные запросы на чтение
	FailedQueries       int64 // Неудачные запросы на чтение
	RetriedQueries      int64 // Повторные попытки запросов на чтение
	
	// Статистика производительности запросов
	QueryLatencySum     int64 // Сумма задержек запросов (в микросекундах)
	QueryCount          int64 // Количество запросов для расчета средней задержки
	
	// Количества по типам запросов
	SimpleQueryCount    int64
	ComplexQueryCount   int64
	AnalyticalQueryCount int64
	TimeSeriesQueryCount int64
	
	// Статистика результатов запросов
	TotalHits           int64 // Общее количество найденных документов
	TotalBytesRead      int64 // Общее количество прочитанных байт
}

// NewStats создает новый экземпляр статистики
func NewStats() *Stats {
	return &Stats{
		StartTime: time.Now(),
	}
}

// IncrementTotalRequests увеличивает счетчик общих запросов на запись
func (s *Stats) IncrementTotalRequests() {
	atomic.AddInt64(&s.TotalRequests, 1)
}

// IncrementSuccessfulRequests увеличивает счетчик успешных запросов на запись
func (s *Stats) IncrementSuccessfulRequests() {
	atomic.AddInt64(&s.SuccessfulRequests, 1)
}

// IncrementFailedRequests увеличивает счетчик неудачных запросов на запись
func (s *Stats) IncrementFailedRequests() {
	atomic.AddInt64(&s.FailedRequests, 1)
}

// AddLogs добавляет количество отправленных логов
func (s *Stats) AddLogs(count int) {
	atomic.AddInt64(&s.TotalLogs, int64(count))
}

// IncrementRetriedRequests увеличивает счетчик повторных попыток запросов на запись
func (s *Stats) IncrementRetriedRequests() {
	atomic.AddInt64(&s.RetriedRequests, 1)
}

// IncrementTotalQueries увеличивает счетчик общих запросов на чтение
func (s *Stats) IncrementTotalQueries() {
	atomic.AddInt64(&s.TotalQueries, 1)
}

// IncrementSuccessfulQueries увеличивает счетчик успешных запросов на чтение
func (s *Stats) IncrementSuccessfulQueries() {
	atomic.AddInt64(&s.SuccessfulQueries, 1)
}

// IncrementFailedQueries увеличивает счетчик неудачных запросов на чтение
func (s *Stats) IncrementFailedQueries() {
	atomic.AddInt64(&s.FailedQueries, 1)
}

// IncrementRetriedQueries увеличивает счетчик повторных попыток запросов на чтение
func (s *Stats) IncrementRetriedQueries() {
	atomic.AddInt64(&s.RetriedQueries, 1)
}

// AddQueryLatency добавляет значение задержки запроса (в микросекундах)
func (s *Stats) AddQueryLatency(latencyMicros int64) {
	atomic.AddInt64(&s.QueryLatencySum, latencyMicros)
	atomic.AddInt64(&s.QueryCount, 1)
}

// IncrementQueryTypeCount увеличивает счетчик для определенного типа запроса
func (s *Stats) IncrementQueryTypeCount(queryType string) {
	switch queryType {
	case "simple":
		atomic.AddInt64(&s.SimpleQueryCount, 1)
	case "complex":
		atomic.AddInt64(&s.ComplexQueryCount, 1)
	case "analytical":
		atomic.AddInt64(&s.AnalyticalQueryCount, 1)
	case "timeseries":
		atomic.AddInt64(&s.TimeSeriesQueryCount, 1)
	}
}

// AddHits добавляет количество найденных документов
func (s *Stats) AddHits(count int) {
	atomic.AddInt64(&s.TotalHits, int64(count))
}

// AddBytesRead добавляет количество прочитанных байт
func (s *Stats) AddBytesRead(bytes int64) {
	atomic.AddInt64(&s.TotalBytesRead, bytes)
}

// PrintSummary выводит сводную статистику
func (s *Stats) PrintSummary() {
	elapsed := time.Since(s.StartTime).Seconds()
	
	fmt.Printf("\n=== Итоги тестирования ===\n")
	fmt.Printf("Продолжительность: %.2f секунд\n", elapsed)
	
	// Статистика записи
	if s.TotalRequests > 0 {
		writeRPS := float64(s.TotalRequests) / elapsed
		writeLPS := float64(s.TotalLogs) / elapsed
		
		fmt.Printf("\n--- Статистика записи ---\n")
		fmt.Printf("Всего запросов на запись: %d (%.2f/с)\n", s.TotalRequests, writeRPS)
		fmt.Printf("Всего логов отправлено: %d (%.2f/с)\n", s.TotalLogs, writeLPS)
		fmt.Printf("Успешных запросов: %d (%.2f%%)\n", s.SuccessfulRequests, 
			float64(s.SuccessfulRequests)/float64(s.TotalRequests)*100)
		fmt.Printf("Неудачных запросов: %d (%.2f%%)\n", s.FailedRequests, 
			float64(s.FailedRequests)/float64(s.TotalRequests)*100)
		fmt.Printf("Повторных попыток: %d\n", s.RetriedRequests)
	}
	
	// Статистика чтения
	if s.TotalQueries > 0 {
		readQPS := float64(s.TotalQueries) / elapsed
		avgLatencyMs := float64(s.QueryLatencySum) / float64(s.QueryCount) / 1000.0
		avgHitsPerQuery := float64(s.TotalHits) / float64(s.TotalQueries)
		avgBytesPerQuery := float64(s.TotalBytesRead) / float64(s.TotalQueries) / 1024.0 // КБ
		
		fmt.Printf("\n--- Статистика чтения ---\n")
		fmt.Printf("Всего запросов на чтение: %d (%.2f/с)\n", s.TotalQueries, readQPS)
		fmt.Printf("Успешных запросов: %d (%.2f%%)\n", s.SuccessfulQueries, 
			float64(s.SuccessfulQueries)/float64(s.TotalQueries)*100)
		fmt.Printf("Неудачных запросов: %d (%.2f%%)\n", s.FailedQueries, 
			float64(s.FailedQueries)/float64(s.TotalQueries)*100)
		fmt.Printf("Повторных попыток: %d\n", s.RetriedQueries)
		
		// Производительность запросов
		fmt.Printf("\nСредняя задержка запроса: %.2f мс\n", avgLatencyMs)
		fmt.Printf("Среднее кол-во результатов: %.2f записей/запрос\n", avgHitsPerQuery)
		fmt.Printf("Средний размер результата: %.2f КБ/запрос\n", avgBytesPerQuery)
		
		// Распределение типов запросов
		if s.QueryCount > 0 {
			fmt.Printf("\nРаспределение типов запросов:\n")
			fmt.Printf("  Простые: %d (%.2f%%)\n", s.SimpleQueryCount, 
				float64(s.SimpleQueryCount)/float64(s.QueryCount)*100)
			fmt.Printf("  Сложные: %d (%.2f%%)\n", s.ComplexQueryCount, 
				float64(s.ComplexQueryCount)/float64(s.QueryCount)*100)
			fmt.Printf("  Аналитические: %d (%.2f%%)\n", s.AnalyticalQueryCount, 
				float64(s.AnalyticalQueryCount)/float64(s.QueryCount)*100)
			fmt.Printf("  Временные ряды: %d (%.2f%%)\n", s.TimeSeriesQueryCount, 
				float64(s.TimeSeriesQueryCount)/float64(s.QueryCount)*100)
		}
	}
	
	// Общая статистика
	if s.TotalRequests > 0 && s.TotalQueries > 0 {
		totalOps := s.TotalRequests + s.TotalQueries
		opsPerSec := float64(totalOps) / elapsed
		
		fmt.Printf("\n--- Общая статистика ---\n")
		fmt.Printf("Всего операций: %d (%.2f/с)\n", totalOps, opsPerSec)
		fmt.Printf("Соотношение чтение/запись: %.2f\n", 
			float64(s.TotalQueries)/float64(s.TotalRequests))
	}
}
