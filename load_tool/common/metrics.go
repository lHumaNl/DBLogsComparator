package common

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Метрики для операций записи
	WriteRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_write_requests_total",
		Help: "Общее количество запросов на запись",
	})

	WriteRequestsSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_write_requests_success",
		Help: "Количество успешных запросов на запись",
	})

	WriteRequestsFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_write_requests_failure",
		Help: "Количество неудачных запросов на запись",
	})

	WriteLogsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_logs_total",
		Help: "Общее количество отправленных логов",
	})

	WriteRequestsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_write_requests_retried",
		Help: "Количество повторных попыток запросов на запись",
	})

	WriteDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "load_test_write_duration_seconds",
		Help:    "Гистограмма длительности запросов на запись",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // от 1мс до ~16с
	})

	// Метрики для операций чтения
	ReadRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_read_requests_total",
		Help: "Общее количество запросов на чтение",
	})

	ReadRequestsSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_read_requests_success",
		Help: "Количество успешных запросов на чтение",
	})

	ReadRequestsFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_read_requests_failure",
		Help: "Количество неудачных запросов на чтение",
	})

	ReadRequestsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_read_requests_retried",
		Help: "Количество повторных попыток запросов на чтение",
	})

	ReadDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "load_test_read_duration_seconds",
		Help:    "Гистограмма длительности запросов на чтение",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // от 1мс до ~16с
	})

	QueryTypeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "load_test_query_type_total",
		Help: "Количество запросов по типам",
	}, []string{"type"})

	ResultSizeHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "load_test_result_size_bytes",
		Help:    "Гистограмма размера результатов запросов",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 10), // от 1KB до ~1MB
	})

	ResultHitsHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "load_test_result_hits",
		Help:    "Гистограмма количества результатов в запросах",
		Buckets: prometheus.LinearBuckets(0, 10, 10), // от 0 до 90 с шагом 10
	})

	// Метрики по системам
	OperationCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "load_test_operations_total",
		Help: "Количество операций по типу и системе",
	}, []string{"type", "system"})

	// Общие метрики производительности
	CurrentRPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "load_test_current_rps",
		Help: "Текущее количество запросов на запись в секунду",
	})

	CurrentQPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "load_test_current_qps",
		Help: "Текущее количество запросов на чтение в секунду",
	})

	CurrentOPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "load_test_current_ops",
		Help: "Общее текущее количество операций в секунду",
	})

	// Метрики генератора логов из pkg/metrics.go
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "log_generator_requests_total",
			Help: "Общее количество запросов, отправленных генератором логов",
		},
		[]string{"status", "destination"},
	)

	LogsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "log_generator_logs_total",
			Help: "Общее количество сгенерированных логов по типам",
		},
		[]string{"log_type", "destination"},
	)

	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "log_generator_request_duration_seconds",
			Help:    "Время выполнения запросов",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"status", "destination"},
	)

	RetryCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "log_generator_retry_count",
			Help: "Количество повторных попыток отправки запросов",
		},
	)

	RPSGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_rps",
			Help: "Текущее количество запросов в секунду",
		},
	)

	LPSGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_lps",
			Help: "Текущее количество логов в секунду",
		},
	)

	BatchSizeGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_batch_size",
			Help: "Размер пакета логов",
		},
	)
)

var (
	writeRequestsCount int64
	readRequestsCount  int64
)

// IncrementWriteRequests увеличивает счетчик запросов записи
func IncrementWriteRequests() {
	WriteRequestsTotal.Inc()
	atomic.AddInt64(&writeRequestsCount, 1)
}

// IncrementReadRequests увеличивает счетчик запросов чтения
func IncrementReadRequests() {
	ReadRequestsTotal.Inc()
	atomic.AddInt64(&readRequestsCount, 1)
}

// IncrementSuccessfulWrite увеличивает счетчик успешных запросов записи
func IncrementSuccessfulWrite() {
	WriteRequestsSuccess.Inc()
}

// IncrementFailedWrite увеличивает счетчик неудачных запросов записи
func IncrementFailedWrite() {
	WriteRequestsFailure.Inc()
}

// IncrementSuccessfulRead увеличивает счетчик успешных запросов чтения
func IncrementSuccessfulRead() {
	ReadRequestsSuccess.Inc()
}

// IncrementFailedRead увеличивает счетчик неудачных запросов чтения
func IncrementFailedRead() {
	ReadRequestsFailure.Inc()
}

// InitPrometheus инициализирует регистрацию метрик
func InitPrometheus() {
	// Запускаем отдельную горутину для обновления метрик в реальном времени
	go updateRealTimeMetrics()
}

// updateRealTimeMetrics обновляет метрики в реальном времени
func updateRealTimeMetrics() {
	lastWriteRequests := int64(0)
	lastReadRequests := int64(0)
	lastTime := time.Now()

	for {
		time.Sleep(1 * time.Second)

		now := time.Now()
		elapsed := now.Sub(lastTime).Seconds()
		lastTime = now

		// Получаем текущие значения из Prometheus счетчиков
		// Используем метрики из общего пакета, вместо прямого обращения к Prometheus
		currentWriteRequests := writeRequestsCount
		currentReadRequests := readRequestsCount

		// Вычисляем RPS и QPS
		writeRequests := float64(currentWriteRequests - lastWriteRequests)
		readRequests := float64(currentReadRequests - lastReadRequests)

		rps := writeRequests / elapsed
		qps := readRequests / elapsed
		ops := (writeRequests + readRequests) / elapsed

		// Обновляем метрики
		CurrentRPS.Set(rps)
		CurrentQPS.Set(qps)
		CurrentOPS.Set(ops)

		// Запоминаем значения для следующего цикла
		lastWriteRequests = currentWriteRequests
		lastReadRequests = currentReadRequests
	}
}

// StartMetricsServer запускает HTTP-сервер для Prometheus метрик
func StartMetricsServer(port int) {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", port)
		fmt.Printf("Metrics server started at %s/metrics\n", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Printf("Error starting metrics server: %v\n", err)
		}
	}()
}

// InitGeneratorMetrics инициализирует метрики для генератора логов
func InitGeneratorMetrics(config *Config) {
	// Устанавливаем начальное значение для размера пакета
	if config.Generator.BulkSize > 0 {
		BatchSizeGauge.Set(float64(config.Generator.BulkSize))
	}
}
