package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/pkg"
)

func main() {
	fmt.Println("=== Log generator test with improved variability ===")

	timestamp := time.Now().Format(time.RFC3339)

	// Generate and output several samples of each log type
	fmt.Println("\n=== WEB_ACCESS LOGS ===")
	for i := 0; i < 3; i++ {
		log := pkg.GenerateLog("web_access", timestamp)
		printLog(log)
	}

	fmt.Println("\n=== WEB_ERROR LOGS ===")
	for i := 0; i < 5; i++ {
		log := pkg.GenerateLog("web_error", timestamp)
		printLog(log)
	}

	fmt.Println("\n=== APPLICATION LOGS ===")
	for i := 0; i < 5; i++ {
		log := pkg.GenerateLog("application", timestamp)
		printLog(log)
	}

	fmt.Println("\n=== METRIC LOGS ===")
	for i := 0; i < 2; i++ {
		log := pkg.GenerateLog("metric", timestamp)
		printLog(log)
	}

	fmt.Println("\n=== EVENT LOGS ===")
	for i := 0; i < 2; i++ {
		log := pkg.GenerateLog("event", timestamp)
		printLog(log)
	}
}

func printLog(log interface{}) {
	jsonData, err := json.MarshalIndent(log, "", "  ")
	if err != nil {
		fmt.Printf("Serialization error: %v\n", err)
		return
	}
	fmt.Println(string(jsonData))
	fmt.Println("--------------------------------------")
}
