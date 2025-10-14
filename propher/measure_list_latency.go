package propher

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"propher/internal"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Record struct {
	TSms      int64  `json:"ts_ms"`
	ObsQueue  string `json:"obs_queue"`
	HoldQueue string `json:"hold_queue"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
	TraceID   string `json:"trace_id,omitempty"`
	T0ms      *int64 `json:"t0_ms,omitempty"`
	LatencyMS *int64 `json:"latency_ms,omitempty"`
}

func percentile(sortedVals []int64, q float64) int64 {
	n := len(sortedVals)
	if n == 0 {
		return 0
	}
	idx := int(math.Ceil(q*float64(n)) - 1)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sortedVals[idx]
}

func extractInt64(v any, unit string) (*int64, error) {
	// Accept numbers (json decodes as float64) and numeric strings.
	switch t := v.(type) {
	case float64:
		val := t
		if unit == "s" {
			val *= 1000.0
		}
		x := int64(val)
		return &x, nil
	case int64:
		val := float64(t)
		if unit == "s" {
			val *= 1000.0
		}
		x := int64(val)
		return &x, nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return nil, fmt.Errorf("empty string")
		}
		var f float64
		_, err := fmt.Sscanf(s, "%f", &f)
		if err != nil {
			return nil, err
		}
		if unit == "s" {
			f *= 1000.0
		}
		x := int64(f)
		return &x, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func main() {
	var (
		redisAddr     = flag.String("redis-addr", "127.0.0.1:6379", "Redis address host:port")
		redisPass     = flag.String("redis-pass", "", "Redis password")
		redisDB       = flag.Int("redis-db", 0, "Redis DB number")
		obsQueue      = flag.String("obs-queue", "", "Observed LIST key (required)")
		holdQueue     = flag.String("hold-queue", "", "Hold LIST key (default: <obs-queue>:hold)")
		durationSec   = flag.Int("duration-sec", 600, "How long to measure (seconds)")
		blockSec      = flag.Int("block-sec", 1, "BRPOPLPUSH timeout (seconds)")
		maxMessages   = flag.Int("max-messages", 0, "Stop after N messages (0 = unlimited)")
		outJSONL      = flag.String("out-jsonl", "latency.jsonl", "Output JSONL path")
		t0Field       = flag.String("t0-field", "sent_epoch", "Field containing t0")
		t0Unit        = flag.String("t0-unit", "ms", "Unit for t0: ms or s")
		traceField    = flag.String("trace-field", "trace_id", "Field containing trace id")
		restore       = flag.Bool("restore", false, "Restore messages from hold back to obs after measurement")
		restoreVerify = flag.Bool("restore-verify-empty", false, "Refuse restore if obs-queue is non-empty at restore time")
	)
	flag.Parse()

	if *obsQueue == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --obs-queue is required")
		os.Exit(2)
	}
	if *t0Unit != "ms" && *t0Unit != "s" {
		fmt.Fprintln(os.Stderr, "ERROR: --t0-unit must be ms or s")
		os.Exit(2)
	}

	hq := *holdQueue
	if hq == "" {
		hq = *obsQueue + ":hold"
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPass,
		DB:       *redisDB,
	})

	startMS := internal.NowMS()
	endMS := startMS + int64(*durationSec)*1000

	f, err := os.Create(*outJSONL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: create out file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	var (
		latencies []int64
		total     int
		okCount   int
		badCount  int
	)

	for {
		if internal.NowMS() >= endMS {
			break
		}
		if *maxMessages > 0 && total >= *maxMessages {
			break
		}

		// Atomic move obs -> hold
		raw, err := rdb.BRPopLPush(ctx, *obsQueue, hq, time.Duration(*blockSec)*time.Second).Result()
		if err != nil {
			if err == redis.Nil {
				continue // timeout, queue empty
			}
			fmt.Fprintf(os.Stderr, "ERROR: BRPOPLPUSH: %v\n", err)
			break
		}

		total++
		ts := internal.NowMS()

		rec := Record{
			TSms:      ts,
			ObsQueue:  *obsQueue,
			HoldQueue: hq,
			OK:        false,
		}

		// Parse JSON object
		var obj map[string]any
		if err := json.Unmarshal([]byte(raw), &obj); err != nil {
			badCount++
			rec.Error = "json_parse_error: " + err.Error()
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			continue
		}

		// trace_id
		if v, ok := obj[*traceField]; ok && v != nil {
			rec.TraceID = fmt.Sprintf("%v", v)
		}

		// t0
		var t0ms *int64
		if v, ok := obj[*t0Field]; ok && v != nil {
			x, e := extractInt64(v, *t0Unit)
			if e == nil {
				t0ms = x
			}
		}
		rec.T0ms = t0ms
		if t0ms == nil {
			badCount++
			rec.Error = "missing_or_bad_" + *t0Field
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			continue
		}

		lat := ts - *t0ms
		rec.OK = true
		rec.LatencyMS = &lat
		okCount++
		latencies = append(latencies, lat)

		b, _ := json.Marshal(rec)
		w.Write(b)
		w.WriteByte('\n')
	}

	durS := float64(internal.NowMS()-startMS) / 1000.0
	if durS <= 0 {
		durS = 1e-9
	}
	throughput := float64(okCount) / durS
	fmt.Printf("[RESULT] total_read=%d ok=%d bad=%d duration_s=%.3f ok_throughput_msg_s=%.3f\n",
		total, okCount, badCount, durS, throughput)

	if okCount > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		fmt.Printf("[LAT] p50=%d ms\n", percentile(latencies, 0.50))
		fmt.Printf("[LAT] p90=%d ms\n", percentile(latencies, 0.90))
		fmt.Printf("[LAT] p95=%d ms\n", percentile(latencies, 0.95))
		fmt.Printf("[LAT] p99=%d ms\n", percentile(latencies, 0.99))
		fmt.Printf("[LAT] max=%d ms\n", latencies[len(latencies)-1])
	}

	if *restore {
		if *restoreVerify {
			cur, err := rdb.LLen(ctx, *obsQueue).Result()
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: LLEN verify: %v\n", err)
				os.Exit(1)
			}
			if cur != 0 {
				fmt.Fprintf(os.Stderr, "Refuse restore: obs-queue %q is not empty (LLEN=%d)\n", *obsQueue, cur)
				os.Exit(2)
			}
		}

		moved := 0
		for {
			x, err := rdb.RPopLPush(ctx, hq, *obsQueue).Result()
			if err != nil {
				if err == redis.Nil {
					break
				}
				fmt.Fprintf(os.Stderr, "ERROR: RPOPLPUSH restore: %v\n", err)
				os.Exit(1)
			}
			if x == "" {
				// Not expected from Redis, but keep safe.
			}
			moved++
		}
		fmt.Printf("[RESTORE] moved_back=%d from %s -> %s\n", moved, hq, *obsQueue)
	}
}
