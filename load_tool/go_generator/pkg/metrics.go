package pkg

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
)

// Объявляем переменные для метрик, но не инициализируем их сразу
// Это позволит избежать конфликтов при инициализации
var (
	RequestsTotal   *prometheus.CounterVec
	LogsTotal       *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	RetryCounter    prometheus.Counter
	RPSGauge        prometheus.Gauge
	LPSGauge        prometheus.Gauge
	BatchSizeGauge  prometheus.Gauge

	// Флаг, показывающий, были ли инициализированы метрики
	metricsInitialized bool
)

// InitPrometheus инициализирует метрики Prometheus
func InitPrometheus(config Config) {
	// Проверяем, не были ли уже инициализированы метрики
	if metricsInitialized {
		return
	}

	// Используем метрики из common, если они уже инициализированы
	// Это предотвратит дублирование регистрации
	RequestsTotal = common.RequestsTotal
	LogsTotal = common.LogsTotal
	RequestDuration = common.RequestDuration
	RetryCounter = common.RetryCounter
	RPSGauge = common.RPSGauge
	LPSGauge = common.LPSGauge
	BatchSizeGauge = common.BatchSizeGauge

	// Вызываем инициализацию метрик из общего пакета
	commonConfig := &common.Config{
		Generator: common.GeneratorConfig{
			BulkSize: config.BulkSize,
		},
	}
	common.InitGeneratorMetrics(commonConfig)

	// Устанавливаем флаг инициализации
	metricsInitialized = true
}

// StartMetricsServer запускает HTTP-сервер для метрик Prometheus
func StartMetricsServer(metricsPort int, config Config) {
	// Используем общую реализацию сервера метрик
	common.StartMetricsServer(metricsPort)
}
