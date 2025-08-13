package logdata

import randv2 "math/rand/v2"

// RandomIntn generates a thread-safe random number in [0,n) using math/rand/v2
// Uses per-thread generators for better performance without mutex overhead
func RandomIntn(n int) int {
	if n <= 0 {
		return 0
	}
	return randv2.IntN(n) // IntN in v2, not Intn
}

// RandomFloat64 generates a thread-safe random float64 in [0.0,1.0) using math/rand/v2
func RandomFloat64() float64 {
	return randv2.Float64()
}
