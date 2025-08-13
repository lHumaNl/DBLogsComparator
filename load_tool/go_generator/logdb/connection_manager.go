package logdb

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// GeneratorConnectionManager manages HTTP clients for the generator module
// This is separate from querier connection pools to avoid conflicts
type GeneratorConnectionManager struct {
	clients map[string]*http.Client
	mutex   sync.RWMutex
}

// Global connection manager instance for go_generator only
var generatorConnectionManager = &GeneratorConnectionManager{
	clients: make(map[string]*http.Client),
}

// GetOrCreateClient returns an existing HTTP client or creates a new one
// Key format: "system_baseURL_timeout" for uniqueness
func (cm *GeneratorConnectionManager) GetOrCreateClient(system, baseURL string, options Options) *http.Client {
	// Create unique key based on system, URL, and critical options
	key := fmt.Sprintf("generator_%s_%s_%v", system, baseURL, options.Timeout)

	// Try to get existing client (fast path with read lock)
	cm.mutex.RLock()
	if client, exists := cm.clients[key]; exists {
		cm.mutex.RUnlock()
		return client
	}
	cm.mutex.RUnlock()

	// Client doesn't exist, need to create (slow path with write lock)
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Double-check pattern - another goroutine might have created it
	if client, exists := cm.clients[key]; exists {
		return client
	}

	// Create new optimized HTTP client for bulk operations
	client := createOptimizedHTTPClient(options)
	cm.clients[key] = client

	return client
}

// GetClientCount returns the number of cached HTTP clients
func (cm *GeneratorConnectionManager) GetClientCount() int {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return len(cm.clients)
}

// CleanupIdleConnections closes idle connections for all cached clients
func (cm *GeneratorConnectionManager) CleanupIdleConnections() {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	for _, client := range cm.clients {
		if transport, ok := client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
}

// Shutdown closes all connections and clears the cache
// Should be called when the generator is shutting down
func (cm *GeneratorConnectionManager) Shutdown() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Close all idle connections
	for _, client := range cm.clients {
		if transport, ok := client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}

	// Clear the cache
	cm.clients = make(map[string]*http.Client)
}

// createOptimizedHTTPClient creates an HTTP client optimized for bulk log operations
func createOptimizedHTTPClient(options Options) *http.Client {
	return &http.Client{
		Timeout: options.Timeout,
		Transport: &http.Transport{
			// Connection pool settings optimized for bulk operations
			MaxIdleConns:        100,              // Global pool size
			MaxIdleConnsPerHost: 100,              // Per-host limit (higher for bulk operations)
			MaxConnsPerHost:     100,              // Total active connections per host
			IdleConnTimeout:     30 * time.Second, // Keep connections alive
			DisableKeepAlives:   false,            // Enable keep-alive for performance

			// Timeouts optimized for bulk operations
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,

			// Buffer sizes optimized for large payloads
			WriteBufferSize: 32768, // 32KB write buffer
			ReadBufferSize:  32768, // 32KB read buffer

			// HTTP/2 support
			ForceAttemptHTTP2: true,

			// TLS configuration
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: false,
				MinVersion:         tls.VersionTLS12,
			},
		},
	}
}

// Global functions for easy access

// GetGeneratorClient returns an HTTP client for the specified system and URL
// This is the main function that should be used by logdb implementations
func GetGeneratorClient(system, baseURL string, options Options) *http.Client {
	return generatorConnectionManager.GetOrCreateClient(system, baseURL, options)
}

// GetGeneratorClientCount returns the number of cached HTTP clients
func GetGeneratorClientCount() int {
	return generatorConnectionManager.GetClientCount()
}

// CleanupGeneratorConnections closes idle connections for all cached clients
func CleanupGeneratorConnections() {
	generatorConnectionManager.CleanupIdleConnections()
}

// ShutdownGeneratorConnections shuts down all generator HTTP clients
// Should be called when the generator is shutting down
func ShutdownGeneratorConnections() {
	generatorConnectionManager.Shutdown()
}
