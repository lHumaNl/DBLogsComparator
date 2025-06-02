package pkg

import (
	"encoding/json"
	"testing"
)

func TestGenerateRandomData(t *testing.T) {
	// Тестирование функции генерации случайного IP-адреса
	ip := GenerateRandomIP()
	if ip == "" {
		t.Error("GenerateRandomIP должен возвращать непустую строку")
	}

	// Тестирование функции генерации User-Agent
	ua := GenerateRandomUserAgent()
	if ua == "" {
		t.Error("GenerateRandomUserAgent должен возвращать непустую строку")
	}

	// Тестирование функции генерации HTTP-статуса
	status := GenerateRandomHttpStatus()
	validStatuses := []int{200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503}
	validStatus := false
	for _, s := range validStatuses {
		if status == s {
			validStatus = true
			break
		}
	}
	if !validStatus {
		t.Errorf("GenerateRandomHttpStatus должен возвращать один из валидных HTTP-статусов, получено: %d", status)
	}

	// Тестирование функции генерации HTTP-метода
	method := GenerateRandomHttpMethod()
	validMethods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	validMethod := false
	for _, m := range validMethods {
		if method == m {
			validMethod = true
			break
		}
	}
	if !validMethod {
		t.Errorf("GenerateRandomHttpMethod должен возвращать один из валидных HTTP-методов, получено: %s", method)
	}
}

func TestSelectRandomLogType(t *testing.T) {
	// Тестирование функции выбора типа лога на основе распределения
	distribution := map[string]int{
		"web_access":  60,
		"web_error":   10,
		"application": 20,
		"metric":      5,
		"event":       5,
	}

	// Запускаем выбор типа лога несколько раз
	// и проверяем, что все типы возможны
	types := make(map[string]bool)
	iterations := 1000

	for i := 0; i < iterations; i++ {
		logType := SelectRandomLogType(distribution)
		types[logType] = true
	}

	// Проверяем, что все типы логов были выбраны хотя бы раз
	for logType := range distribution {
		if !types[logType] {
			t.Errorf("Тип логов '%s' не был выбран ни разу за %d итераций", logType, iterations)
		}
	}
}

func TestGenerateLog(t *testing.T) {
	timestamp := "2023-01-01T12:00:00Z"
	
	// Тест для типа web_access
	webAccessLog := GenerateLog("web_access", timestamp)
	
	// Преобразуем в JSON и обратно в map для проверки полей
	jsonData, err := json.Marshal(webAccessLog)
	if err != nil {
		t.Fatalf("Ошибка сериализации web_access лога: %v", err)
	}
	
	var webAccessMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &webAccessMap); err != nil {
		t.Fatalf("Ошибка десериализации web_access лога: %v", err)
	}
	
	checkBaseLogFields(t, webAccessMap, "web_access", timestamp)
	if _, ok := webAccessMap["remote_addr"]; !ok {
		t.Error("В web_access логе должно быть поле 'remote_addr'")
	}
	if _, ok := webAccessMap["status"]; !ok {
		t.Error("В web_access логе должно быть поле 'status'")
	}
	
	// Тест для типа web_error
	webErrorLog := GenerateLog("web_error", timestamp)
	jsonData, err = json.Marshal(webErrorLog)
	if err != nil {
		t.Fatalf("Ошибка сериализации web_error лога: %v", err)
	}
	
	var webErrorMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &webErrorMap); err != nil {
		t.Fatalf("Ошибка десериализации web_error лога: %v", err)
	}
	
	checkBaseLogFields(t, webErrorMap, "web_error", timestamp)
	if _, ok := webErrorMap["level"]; !ok {
		t.Error("В web_error логе должно быть поле 'level'")
	}
	if _, ok := webErrorMap["error_code"]; !ok {
		t.Error("В web_error логе должно быть поле 'error_code'")
	}
	
	// Тест для типа application
	appLog := GenerateLog("application", timestamp)
	jsonData, err = json.Marshal(appLog)
	if err != nil {
		t.Fatalf("Ошибка сериализации application лога: %v", err)
	}
	
	var appLogMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &appLogMap); err != nil {
		t.Fatalf("Ошибка десериализации application лога: %v", err)
	}
	
	checkBaseLogFields(t, appLogMap, "application", timestamp)
	if _, ok := appLogMap["level"]; !ok {
		t.Error("В application логе должно быть поле 'level'")
	}
	if _, ok := appLogMap["trace_id"]; !ok {
		t.Error("В application логе должно быть поле 'trace_id'")
	}
	
	// Тест для типа metric
	metricLog := GenerateLog("metric", timestamp)
	jsonData, err = json.Marshal(metricLog)
	if err != nil {
		t.Fatalf("Ошибка сериализации metric лога: %v", err)
	}
	
	var metricLogMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &metricLogMap); err != nil {
		t.Fatalf("Ошибка десериализации metric лога: %v", err)
	}
	
	checkBaseLogFields(t, metricLogMap, "metric", timestamp)
	if _, ok := metricLogMap["metric_name"]; !ok {
		t.Error("В metric логе должно быть поле 'metric_name'")
	}
	if _, ok := metricLogMap["value"]; !ok {
		t.Error("В metric логе должно быть поле 'value'")
	}
	
	// Тест для типа event
	eventLog := GenerateLog("event", timestamp)
	jsonData, err = json.Marshal(eventLog)
	if err != nil {
		t.Fatalf("Ошибка сериализации event лога: %v", err)
	}
	
	var eventLogMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &eventLogMap); err != nil {
		t.Fatalf("Ошибка десериализации event лога: %v", err)
	}
	
	checkBaseLogFields(t, eventLogMap, "event", timestamp)
	if _, ok := eventLogMap["event_type"]; !ok {
		t.Error("В event логе должно быть поле 'event_type'")
	}
	if _, ok := eventLogMap["resource_id"]; !ok {
		t.Error("В event логе должно быть поле 'resource_id'")
	}
	
	// Тест для неизвестного типа (должен вернуть BaseLog)
	unknownLog := GenerateLog("unknown", timestamp)
	jsonData, err = json.Marshal(unknownLog)
	if err != nil {
		t.Fatalf("Ошибка сериализации unknown лога: %v", err)
	}
	
	var unknownLogMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &unknownLogMap); err != nil {
		t.Fatalf("Ошибка десериализации unknown лога: %v", err)
	}
	
	checkBaseLogFields(t, unknownLogMap, "unknown", timestamp)
}

// Вспомогательная функция для проверки базовых полей лога
func checkBaseLogFields(t *testing.T, log map[string]interface{}, expectedType string, expectedTimestamp string) {
	if log["log_type"] != expectedType {
		t.Errorf("Неправильное поле 'log_type': ожидалось '%s', получено '%s'", expectedType, log["log_type"])
	}
	
	if log["timestamp"] != expectedTimestamp {
		t.Errorf("Неправильное поле 'timestamp': ожидалось '%s', получено '%s'", expectedTimestamp, log["timestamp"])
	}
	
	if _, ok := log["host"]; !ok {
		t.Error("В логе должно быть поле 'host'")
	}
	
	if _, ok := log["container_name"]; !ok {
		t.Error("В логе должно быть поле 'container_name'")
	}
}

// TestLogSerialization проверяет корректность сериализации и десериализации логов
func TestLogSerialization(t *testing.T) {
	timestamp := "2023-01-01T12:00:00Z"
	logTypes := []string{"web_access", "web_error", "application", "metric", "event"}
	
	for _, logType := range logTypes {
		log := GenerateLog(logType, timestamp)
		
		// Сериализация в JSON
		jsonData, err := json.Marshal(log)
		if err != nil {
			t.Errorf("Ошибка сериализации лога типа '%s': %v", logType, err)
		}
		
		// Десериализация из JSON
		var parsedLog map[string]interface{}
		if err := json.Unmarshal(jsonData, &parsedLog); err != nil {
			t.Errorf("Ошибка десериализации лога типа '%s': %v", logType, err)
		}
		
		// Проверка основных полей
		if parsedLog["log_type"] != logType {
			t.Errorf("После сериализации тип лога изменился: ожидалось '%s', получено '%v'", 
				logType, parsedLog["log_type"])
		}
	}
}
