package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/pkg"
)

func main() {
	fmt.Println("=== Тест генератора логов с улучшенной вариативностью ===")

	timestamp := time.Now().Format(time.RFC3339)

	// Генерация и вывод нескольких образцов логов каждого типа
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
		fmt.Printf("Ошибка сериализации: %v\n", err)
		return
	}
	fmt.Println(string(jsonData))
	fmt.Println("--------------------------------------")
}
