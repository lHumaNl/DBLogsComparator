package pkg

import (
	"net/http"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	// Устанавливаем начальное значение для размера пакета
	BatchSizeGauge.Set(float64(config.BulkSize))
}

// StartMetricsServer запускает HTTP-сервер для метрик Prometheus
func StartMetricsServer(metricsPort int, config Config) {
	http.Handle("/metrics", promhttp.Handler())
	
	go func() {
		addr := fmt.Sprintf(":%d", metricsPort)
		fmt.Printf("Запуск сервера метрик Prometheus на %s\n", addr)
		
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Printf("Ошибка запуска сервера метрик: %v\n", err)
		}
	}()
}
