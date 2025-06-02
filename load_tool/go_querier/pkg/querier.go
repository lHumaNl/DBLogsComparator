package pkg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	
	"github.com/dblogscomparator/load_tool/common"
	"github.com/dblogscomparator/load_tool/go_querier/pkg/executors"
)

// QueryType определяет тип запроса
type QueryType string

const (
	SimpleQuery     QueryType = "simple"     // Простой поиск по ключевому слову или полю
	ComplexQuery    QueryType = "complex"    // Сложный поиск с несколькими условиями
	AnalyticalQuery QueryType = "analytical" // Запрос с агрегациями
	TimeSeriesQuery QueryType = "timeseries" // Запрос временных рядов
)

// QueryResult представляет результат выполнения запроса
type QueryResult struct {
	Duration  time.Duration // Время выполнения
	HitCount  int           // Количество найденных документов
	BytesRead int64         // Количество прочитанных байт
	Status    string        // Статус запроса
}

// QueryExecutor интерфейс для выполнения запросов
type QueryExecutor interface {
	// ExecuteQuery выполняет запрос указанного типа и возвращает результат
	ExecuteQuery(ctx context.Context, queryType QueryType) (QueryResult, error)
	
	// GenerateRandomQuery создает случайный запрос указанного типа
	GenerateRandomQuery(queryType QueryType) interface{}
	
	// GetSystemName возвращает название системы
	GetSystemName() string
}

// Options настройки для исполнителя запросов
type Options struct {
	Timeout    time.Duration
	RetryCount int
	RetryDelay time.Duration
	Verbose    bool
}

// QueryConfig конфигурация для модуля запросов
type QueryConfig struct {
	Mode                 string
	BaseURL              string
	QPS                  int
	Duration             time.Duration
	WorkerCount          int
	QueryTypeDistribution map[QueryType]int
	QueryTimeout         time.Duration
	MaxRetries           int
	RetryDelay           time.Duration
	Verbose              bool
}

// Worker представляет рабочую горутину
type Worker struct {
	ID        int
	Jobs      <-chan struct{}
	Stats     *common.Stats
	Config    QueryConfig
	Executor  QueryExecutor
	WaitGroup *sync.WaitGroup
}

// CreateQueryExecutor создает исполнитель запросов для указанной системы
func CreateQueryExecutor(mode, baseURL string, options Options) (QueryExecutor, error) {
	switch mode {
	case "victoria":
		return executors.NewVictoriaLogsExecutor(baseURL, options), nil
	case "es":
		return executors.NewElasticsearchExecutor(baseURL, options), nil
	case "loki":
		return executors.NewLokiExecutor(baseURL, options), nil
	default:
		return nil, errors.New(fmt.Sprintf("неизвестная система логирования: %s", mode))
	}
}

// RunQuerier запускает модуль запросов
func RunQuerier(config QueryConfig, executor QueryExecutor, stats *common.Stats) error {
	// Инициализация каналов и горутин
	jobs := make(chan struct{}, config.QPS*2)
	stopChan := make(chan struct{})
	
	// Запуск рабочих горутин
	var wg sync.WaitGroup
	for w := 1; w <= config.WorkerCount; w++ {
		wg.Add(1)
		worker := Worker{
			ID:       w,
			Jobs:     jobs,
			Stats:    stats,
			Config:   config,
			Executor: executor,
			WaitGroup: &wg,
		}
		go runWorker(worker)
	}
	
	// Основной цикл генерации нагрузки
	tickInterval := time.Second / time.Duration(config.QPS)
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
	
	return nil
}

// runWorker запускает рабочую горутину
func runWorker(worker Worker) {
	defer worker.WaitGroup.Done()
	
	ctx := context.Background()
	
	for range worker.Jobs {
		// Выбираем случайный тип запроса на основе распределения
		queryType := selectRandomQueryType(worker.Config.QueryTypeDistribution)
		
		// Инкрементируем общий счетчик запросов
		worker.Stats.IncrementTotalQueries()
		common.ReadRequestsTotal.Inc()
		common.OperationCounter.WithLabelValues("query", worker.Executor.GetSystemName()).Inc()
		
		// Создаем контекст с таймаутом
		queryCtx, cancel := context.WithTimeout(ctx, worker.Config.QueryTimeout)
		
		// Выполняем запрос
		startTime := time.Now()
		result, err := worker.Executor.ExecuteQuery(queryCtx, queryType)
		duration := time.Since(startTime)
		
		// Отменяем контекст
		cancel()
		
		// Обновляем метрики
		common.ReadDurationHistogram.Observe(duration.Seconds())
		common.QueryTypeCounter.WithLabelValues(string(queryType)).Inc()
		
		// Обновляем счетчики в зависимости от результата
		if err != nil {
			worker.Stats.IncrementFailedQueries()
			common.ReadRequestsFailure.Inc()
			
			// Если ошибка не связана с таймаутом или контекстом, повторяем запрос
			if err != context.DeadlineExceeded && err != context.Canceled {
				for i := 0; i < worker.Config.MaxRetries; i++ {
					worker.Stats.IncrementRetriedQueries()
					common.ReadRequestsRetried.Inc()
					
					time.Sleep(worker.Config.RetryDelay)
					
					// Создаем новый контекст для повторного запроса
					retryCtx, retryCancel := context.WithTimeout(ctx, worker.Config.QueryTimeout)
					
					// Повторяем запрос
					retryStartTime := time.Now()
					result, err = worker.Executor.ExecuteQuery(retryCtx, queryType)
					retryDuration := time.Since(retryStartTime)
					
					// Отменяем контекст
					retryCancel()
					
					// Обновляем метрики
					common.ReadDurationHistogram.Observe(retryDuration.Seconds())
					
					// Если запрос успешен, прерываем цикл повторений
					if err == nil {
						break
					}
				}
			}
		}
		
		// Если запрос в итоге успешен, обновляем счетчики
		if err == nil {
			worker.Stats.IncrementSuccessfulQueries()
			common.ReadRequestsSuccess.Inc()
			
			// Обновляем статистику по результатам
			worker.Stats.AddHits(result.HitCount)
			worker.Stats.AddBytesRead(result.BytesRead)
			worker.Stats.IncrementQueryTypeCount(string(queryType))
			worker.Stats.AddQueryLatency(duration.Microseconds())
			
			// Обновляем метрики Prometheus
			common.ResultHitsHistogram.Observe(float64(result.HitCount))
			common.ResultSizeHistogram.Observe(float64(result.BytesRead))
		}
		
		// Если включен подробный вывод, выводим информацию о запросе
		if worker.Config.Verbose {
			if err != nil {
				fmt.Printf("[Worker %d] Query %s failed: %v\n", worker.ID, queryType, err)
			} else {
				fmt.Printf("[Worker %d] Query %s completed in %v: %d hits, %d bytes\n", 
					worker.ID, queryType, duration, result.HitCount, result.BytesRead)
			}
		}
	}
}

// selectRandomQueryType выбирает случайный тип запроса на основе распределения
func selectRandomQueryType(distribution map[QueryType]int) QueryType {
	// Вычисляем общий вес
	totalWeight := 0
	for _, weight := range distribution {
		totalWeight += weight
	}
	
	// Если общий вес равен 0, возвращаем простой запрос
	if totalWeight == 0 {
		return SimpleQuery
	}
	
	// Выбираем случайное число в диапазоне [0, totalWeight)
	r := randInt(0, totalWeight)
	
	// Выбираем тип запроса на основе распределения весов
	currentWeight := 0
	for queryType, weight := range distribution {
		currentWeight += weight
		if r < currentWeight {
			return queryType
		}
	}
	
	// По умолчанию возвращаем простой запрос
	return SimpleQuery
}

// randInt возвращает случайное число в диапазоне [min, max)
func randInt(min, max int) int {
	return min + time.Now().Nanosecond()%(max-min)
}
