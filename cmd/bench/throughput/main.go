// cmd/benchmark/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	alog "github.com/lesismal/arpc/log"

	"github.com/DeltaLaboratory/shard/internal/client"
)

type Config struct {
	LeaderAddr    string
	NumOperations int
	Concurrency   int
	ValueSize     int
	WriteRatio    float64
	Duration      time.Duration
}

type Metrics struct {
	totalOps     int64
	successOps   int64
	failedOps    int64
	totalLatency int64 // in microseconds
	writeLatency int64
	readLatency  int64
	writeOps     int64
	readOps      int64
	duration     time.Duration
}

func main() {
	alog.Output = os.DevNull

	cfg := parseFlags()

	// Create client
	c, err := client.NewClient(cfg.LeaderAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Generate random keys
	keys := generateKeys(cfg.NumOperations)

	// Generate random values
	values := generateValues(cfg.NumOperations, cfg.ValueSize)

	// Run benchmark
	metrics := runBenchmark(c, cfg, keys, values)

	// Print results
	printResults(metrics, cfg)
}

func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.LeaderAddr, "addr", "localhost:8000", "Leader address")
	flag.IntVar(&cfg.NumOperations, "n", 100000, "Number of operations")
	flag.IntVar(&cfg.Concurrency, "c", 100, "Number of concurrent workers")
	flag.IntVar(&cfg.ValueSize, "size", 1024, "Value size in bytes")
	flag.Float64Var(&cfg.WriteRatio, "write-ratio", 0.2, "Ratio of write operations")
	flag.DurationVar(&cfg.Duration, "duration", 1*time.Minute, "Benchmark duration")

	flag.Parse()
	return cfg
}

func generateKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%07d", i))
	}
	return keys
}

func generateValues(n, size int) [][]byte {
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		value := make([]byte, size)
		rand.Read(value)
		values[i] = value
	}
	return values
}

func runBenchmark(c *client.Client, cfg *Config, keys, values [][]byte) *Metrics {
	metrics := &Metrics{}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	// Create worker pool
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go worker(ctx, c, cfg, keys, values, metrics, &wg)
	}

	// Print progress periodically
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		var lastOps int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentOps := atomic.LoadInt64(&metrics.totalOps)
				opsPerSec := currentOps - lastOps
				lastOps = currentOps
				log.Printf("Ops/sec: %d", opsPerSec)
			}
		}
	}()

	wg.Wait()
	metrics.duration = time.Since(startTime)
	return metrics
}

func worker(ctx context.Context, c *client.Client, cfg *Config, keys, values [][]byte, metrics *Metrics, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Choose random key and operation
			keyIdx := rand.Intn(len(keys))
			isWrite := rand.Float64() < cfg.WriteRatio

			start := time.Now()
			var err error

			if isWrite {
				err = c.Set(ctx, keys[keyIdx], values[keyIdx])
				if err == nil {
					atomic.AddInt64(&metrics.writeOps, 1)
					atomic.AddInt64(&metrics.writeLatency, time.Since(start).Microseconds())
				}
			} else {
				_, err = c.Get(ctx, keys[keyIdx])
				if err == nil {
					atomic.AddInt64(&metrics.readOps, 1)
					atomic.AddInt64(&metrics.readLatency, time.Since(start).Microseconds())
				}
			}

			atomic.AddInt64(&metrics.totalOps, 1)
			if err != nil {
				atomic.AddInt64(&metrics.failedOps, 1)
			} else {
				atomic.AddInt64(&metrics.successOps, 1)
				atomic.AddInt64(&metrics.totalLatency, time.Since(start).Microseconds())
			}
		}
	}
}

func printResults(m *Metrics, cfg *Config) {
	fmt.Println("\nBenchmark Results:")
	fmt.Println("==================")
	fmt.Printf("Duration: %v\n", m.duration)
	fmt.Printf("Total Operations: %d\n", m.totalOps)
	fmt.Printf("Successful Operations: %d\n", m.successOps)
	fmt.Printf("Failed Operations: %d\n", m.failedOps)
	fmt.Printf("Operations/sec: %.2f\n", float64(m.totalOps)/m.duration.Seconds())

	if m.successOps > 0 {
		fmt.Printf("Average Latency: %.2f ms\n", float64(m.totalLatency)/float64(m.successOps)/1000)
	}

	if m.writeOps > 0 {
		fmt.Printf("Write Operations: %d\n", m.writeOps)
		fmt.Printf("Average Write Latency: %.2f ms\n", float64(m.writeLatency)/float64(m.writeOps)/1000)
	}

	if m.readOps > 0 {
		fmt.Printf("Read Operations: %d\n", m.readOps)
		fmt.Printf("Average Read Latency: %.2f ms\n", float64(m.readLatency)/float64(m.readOps)/1000)
	}

	fmt.Printf("Success Rate: %.2f%%\n", float64(m.successOps)*100/float64(m.totalOps))
}
