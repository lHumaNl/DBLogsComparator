package pkg

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
)

// Prometheus метрики
var (
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

// InitPrometheus инициализирует метрики Prometheus
func InitPrometheus(config Config) {
	// Вызываем инициализацию метрик из общего пакета
	commonConfig := &common.Config{
		Generator: common.GeneratorConfig{
			BulkSize: config.BulkSize,
		},
	}
	common.InitGeneratorMetrics(commonConfig)
}

// StartMetricsServer запускает HTTP-сервер для метрик Prometheus
func StartMetricsServer(metricsPort int, config Config) {
	// Используем общую реализацию сервера метрик
	common.StartMetricsServer(metricsPort)
}
