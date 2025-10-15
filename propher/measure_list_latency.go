package propher

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"propher/internal"
	"propher/internal/config"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Record struct {
	// TSms - время измерения в миллисекундах.
	TSms int64 `json:"ts_ms"`
	// ObsQueue - исходная очередь.
	ObsQueue string `json:"obs_queue"`
	// HoldQueue - очередь удержания.
	HoldQueue string `json:"hold_queue"`
	// OK - признак успешной обработки.
	OK bool `json:"ok"`
	// Error - описание ошибки (если есть).
	Error string `json:"error,omitempty"`
	// TraceID - идентификатор трассировки.
	TraceID string `json:"trace_id,omitempty"`
	// T0ms - исходный timestamp.
	T0ms *int64 `json:"t0_ms,omitempty"`
	// LatencyMS - задержка в миллисекундах.
	LatencyMS *int64 `json:"latency_ms,omitempty"`
}

func percentile(sortedVals []int64, q float64) int64 {
	// Возвращаем персентиль в отсортированном массиве.
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
	// Преобразуем число или строку в миллисекунды.
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

func RunMeasureListLatency(cfg *config.Config) error {
	// Измеряем задержку сообщений в очереди Redis.
	measureCfg := cfg.MeasureListLatency
	if measureCfg.ObsQueue == "" {
		return fmt.Errorf("obs-queue is required")
	}
	if measureCfg.T0Unit != "ms" && measureCfg.T0Unit != "s" {
		return fmt.Errorf("t0-unit must be ms or s")
	}

	hq := measureCfg.HoldQueue
	if hq == "" {
		hq = measureCfg.ObsQueue + ":hold"
	}

	// Подключение к Redis.
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Pass,
		DB:       cfg.Redis.DB,
	})

	startMS := internal.NowMS()
	endMS := startMS + int64(measureCfg.DurationSec)*1000

	// Файл для записи результатов.
	f, err := os.Create(measureCfg.OutJSONL)
	if err != nil {
		return fmt.Errorf("create out file: %w", err)
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

	// Основной цикл измерений.
	for {
		if internal.NowMS() >= endMS {
			break
		}
		if measureCfg.MaxMessages > 0 && total >= measureCfg.MaxMessages {
			break
		}

		// Atomic move obs -> hold
		raw, err := rdb.BRPopLPush(ctx, measureCfg.ObsQueue, hq, time.Duration(measureCfg.BlockSec)*time.Second).Result()
		if err != nil {
			if err == redis.Nil {
				continue // timeout, queue empty
			}
			return fmt.Errorf("brpoplpush: %w", err)
		}

		total++
		ts := internal.NowMS()

		rec := Record{
			TSms:      ts,
			ObsQueue:  measureCfg.ObsQueue,
			HoldQueue: hq,
			OK:        false,
		}

		// Парсим JSON объект.
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
		if v, ok := obj[measureCfg.TraceField]; ok && v != nil {
			rec.TraceID = fmt.Sprintf("%v", v)
		}

		// t0
		var t0ms *int64
		if v, ok := obj[measureCfg.T0Field]; ok && v != nil {
			x, e := extractInt64(v, measureCfg.T0Unit)
			if e == nil {
				t0ms = x
			}
		}
		rec.T0ms = t0ms
		if t0ms == nil {
			badCount++
			rec.Error = "missing_or_bad_" + measureCfg.T0Field
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
	// Итоговая статистика.
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

	// Опциональное восстановление сообщений.
	if measureCfg.Restore {
		if measureCfg.RestoreVerify {
			cur, err := rdb.LLen(ctx, measureCfg.ObsQueue).Result()
			if err != nil {
				return fmt.Errorf("llen verify: %w", err)
			}
			if cur != 0 {
				return fmt.Errorf("refuse restore: obs-queue %q is not empty (LLEN=%d)", measureCfg.ObsQueue, cur)
			}
		}

		moved := 0
		for {
			x, err := rdb.RPopLPush(ctx, hq, measureCfg.ObsQueue).Result()
			if err != nil {
				if err == redis.Nil {
					break
				}
				return fmt.Errorf("rpoplpush restore: %w", err)
			}
			if x == "" {
				// Not expected from Redis, but keep safe.
			}
			moved++
		}
		fmt.Printf("[RESTORE] moved_back=%d from %s -> %s\n", moved, hq, measureCfg.ObsQueue)
	}
	return nil
}
