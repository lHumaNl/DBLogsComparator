package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/executors"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

func main() {
	queryType := flag.String("type", "all", "Type of query to test: simple, complex, analytical, or all")
	count := flag.Int("count", 10, "Number of queries to execute")
	verbose := flag.Bool("verbose", true, "Verbose output")
	lokiURL := flag.String("url", "http://localhost:3100", "Loki server URL")
	flag.Parse()

	options := models.Options{
		Timeout:    5 * time.Second,
		RetryCount: 3,
		RetryDelay: 500 * time.Millisecond,
		Verbose:    *verbose,
	}

	executor := executors.NewLokiExecutor(*lokiURL, options)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received interrupt signal, shutting down...")
		cancel()
	}()

	log.Printf("Testing Loki queries against %s\n", *lokiURL)
	log.Printf("Query type: %s, Count: %d\n", *queryType, *count)
	log.Println("Press Ctrl+C to stop the test.")

	// Pre-warm the cache
	log.Println("Pre-warming label cache...")
	time.Sleep(2 * time.Second) // Give time for async cache refresh

	for i := 1; i <= *count && ctx.Err() == nil; i++ {
		var qType models.QueryType

		// Determine query type to run
		switch *queryType {
		case "simple":
			qType = models.SimpleQuery
		case "complex":
			qType = models.ComplexQuery
		case "analytical":
			qType = models.AnalyticalQuery
		default:
			// Alternate between query types
			switch i % 3 {
			case 0:
				qType = models.SimpleQuery
			case 1:
				qType = models.ComplexQuery
			case 2:
				qType = models.AnalyticalQuery
			}
		}

		log.Printf("--- Query %d/%d (Type: %s) ---\n", i, *count, qType)

		// Execute the query
		startTime := time.Now()
		result, err := executor.ExecuteQuery(ctx, qType)
		duration := time.Since(startTime)

		if err != nil {
			log.Printf("Error executing query: %v\n", err)
			continue
		}

		log.Printf("Result: %d records, %d bytes, time: %v\n",
			result.HitCount, result.BytesRead, duration)

		time.Sleep(2 * time.Second)
	}

	log.Println("Test completed.")
}
