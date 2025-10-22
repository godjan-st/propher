package propher

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"propher/internal"
	"propher/internal/config"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Record struct {
	// OK - признак успешной обработки.
	OK bool `json:"ok"`
	// Error - описание ошибки (если есть).
	Error string `json:"error,omitempty"`
	// MessageID - идентификатор сообщения.
	MessageID string `json:"message_id,omitempty"`
	// SourceSentUs - sent_epoch в источнике.
	SourceSentUs *int64 `json:"source_sent_us,omitempty"`
	// ResultSentUs - sent_epoch в результате.
	ResultSentUs *int64 `json:"result_sent_us,omitempty"`
	// ServeUs - задержка между source и result.
	ServeUs *int64 `json:"serve_us,omitempty"`
	// LatencyUs - задержка между result и чтением из Redis.
	LatencyUs *int64 `json:"latency_us,omitempty"`
}

type percentileStats struct {
	P50 int64 `json:"p50_us"`
	P90 int64 `json:"p90_us"`
	P95 int64 `json:"p95_us"`
	P99 int64 `json:"p99_us"`
}

type measureStatsFile struct {
	TotalRead        int              `json:"total_read"`
	OK               int              `json:"ok"`
	Bad              int              `json:"bad"`
	DurationSec      float64          `json:"duration_sec"`
	OKThroughputMsgS float64          `json:"ok_throughput_msg_s"`
	ServeUs          *percentileStats `json:"serve_us,omitempty"`
	LatencyUs        *percentileStats `json:"latency_us,omitempty"`
}

var measureLogger = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

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

func normalizeUnit(unit string) string {
	return strings.ToLower(strings.TrimSpace(unit))
}

func parseFloat(v any) (float64, error) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return float64(i), nil
		}
		return t.Float64()
	case float64:
		return t, nil
	case int64:
		return float64(t), nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, fmt.Errorf("empty string")
		}
		return strconv.ParseFloat(s, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func parseInt(v any) (int64, error) {
	switch t := v.(type) {
	case json.Number:
		i, err := t.Int64()
		if err != nil {
			return 0, fmt.Errorf("failed to parse json.Number: %w", err)
		}
		return i, nil
	case float64:
		return int64(t), nil
	case int64:
		return t, nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, fmt.Errorf("empty string")
		}
		return strconv.ParseInt(s, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func parseFieldToEpoch(v any, unit string) (*int64, error) {
	// Преобразуем поле в микросекунды (ISO или epoch s/ms/us).
	if v == nil {
		return nil, fmt.Errorf("nil value")
	}
	var num int64
	var err error
	if s, ok := v.(string); ok {
		trimmed := strings.TrimSpace(s)
		var parsed time.Time
		if trimmed == "" {
			return nil, fmt.Errorf("empty string")
		}
		if strings.ContainsAny(trimmed, "T:-") && strings.ContainsAny(trimmed, "Z") {
			parsed, err = time.Parse(time.RFC3339Nano, trimmed)
			if err == nil {
				num = parsed.UnixMicro()
				//return &x, nil
			}
		} else if strings.ContainsAny(trimmed, "T:-") {
			const layout = "2006-01-02T15:04:05.999999"
			parsed, err = time.ParseInLocation(layout, trimmed, time.UTC)
			if err == nil {
				num = parsed.UnixMicro()
				//return &x, nil
			}
		}
		if num == 0 && err != nil {
			return nil, fmt.Errorf("failed to parse %q: %w", trimmed, err)
		}
	} else {
		num, err = parseInt(v)
		if err != nil {
			return nil, err
		}
	}
	u := normalizeUnit(unit)
	abs := num

	var micros int64
	switch u {
	case "auto":
		switch {
		case abs >= 1e15:
			micros = num
		case abs >= 1e12:
			micros = num * 1_000
		case abs >= 1e9:
			micros = num * 1_000_000
		default:
			return nil, fmt.Errorf("unknown epoch precision")
		}
	case "s":
		micros = num * 1_000_000
	case "ms":
		micros = num * 1_000
	case "us":
		micros = num
	default:
		return nil, fmt.Errorf("unsupported unit %q", unit)
	}
	return &micros, nil
}

func decodeJSONMap(line []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func extractString(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return "", false
		}
		return s, true
	case json.Number:
		return t.String(), true
	default:
		s := fmt.Sprintf("%v", t)
		if strings.TrimSpace(s) == "" {
			return "", false
		}
		return s, true
	}
}

type sourceIndexStats struct {
	Total      int
	Indexed    int
	Bad        int
	Duplicates int
}

type sourceRecord struct {
	SentUs int64
	Raw    json.RawMessage
}

func loadSourceIndex(path, idField, sentField, unit string) (map[string]sourceRecord, sourceIndexStats, error) {
	stats := sourceIndexStats{}
	if path == "" {
		return nil, stats, fmt.Errorf("source dump path is empty")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, stats, fmt.Errorf("open source dump: %w", err)
	}
	defer f.Close()

	scan := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scan.Buffer(buf, 32*1024*1024)

	index := make(map[string]sourceRecord, 1024)
	for scan.Scan() {
		stats.Total++
		line := bytes.TrimSpace(scan.Bytes())
		if len(line) == 0 {
			stats.Bad++
			continue
		}
		obj, err := decodeJSONMap(line)
		if err != nil {
			stats.Bad++
			continue
		}
		idVal, ok := obj[idField]
		if !ok {
			stats.Bad++
			continue
		}
		msgID, ok := extractString(idVal)
		if !ok {
			stats.Bad++
			continue
		}
		sentVal, ok := obj[sentField]
		if !ok {
			stats.Bad++
			continue
		}
		sentUs, err := parseFieldToEpoch(sentVal, unit)
		if err != nil {
			stats.Bad++
			continue
		}
		if _, exists := index[msgID]; exists {
			stats.Duplicates++
			continue
		}
		rawCopy := append([]byte(nil), line...)
		index[msgID] = sourceRecord{
			SentUs: *sentUs,
			Raw:    json.RawMessage(rawCopy),
		}
		stats.Indexed++
	}
	if err := scan.Err(); err != nil {
		return nil, stats, fmt.Errorf("scan source dump: %w", err)
	}
	return index, stats, nil
}

func formatIntPtr(v *int64) string {
	if v == nil {
		return "nil"
	}
	return strconv.FormatInt(*v, 10)
}

func logRecord(rec Record) {
	if rec.OK {
		measureLogger.Printf("[RECORD] ok=true message_id=%s source_sent_us=%s result_sent_us=%s serve_us=%s latency_us=%s",
			rec.MessageID,
			formatIntPtr(rec.SourceSentUs),
			formatIntPtr(rec.ResultSentUs),
			formatIntPtr(rec.ServeUs),
			formatIntPtr(rec.LatencyUs),
		)
		return
	}
	measureLogger.Printf("[RECORD] ok=false message_id=%s error=%s", rec.MessageID, rec.Error)
}

func buildStatsJSONPath(outJSONL string) string {
	trimmed := strings.TrimSpace(outJSONL)
	if trimmed == "" {
		return "latency.stats.json"
	}
	if strings.HasSuffix(trimmed, ".jsonl") {
		return strings.TrimSuffix(trimmed, ".jsonl") + ".stats.json"
	}
	if strings.HasSuffix(trimmed, ".json") {
		return strings.TrimSuffix(trimmed, ".json") + ".stats.json"
	}
	return trimmed + ".stats.json"
}

func writeStatsJSON(path string, stats measureStatsFile) error {
	b, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal stats json: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write stats json: %w", err)
	}
	return nil
}

func writeLostJSON(path string, lost []json.RawMessage) error {
	b, err := json.MarshalIndent(lost, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal lost json: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write lost json: %w", err)
	}
	return nil
}

func RunMeasureListLatency(cfg *config.Config) error {
	// Измеряем задержку сообщений в очереди Redis.
	measureCfg := cfg.MeasureListLatency
	if measureCfg.ObsQueue == "" {
		return fmt.Errorf("obs-queue is required")
	}
	if measureCfg.SourceDump == "" {
		return fmt.Errorf("source-dump is required")
	}
	if measureCfg.MessageIDField == "" {
		return fmt.Errorf("message-id-field is required")
	}
	if measureCfg.SourceSentField == "" {
		return fmt.Errorf("source-sent-field is required")
	}
	if measureCfg.T0Field == "" {
		return fmt.Errorf("t0-field is required")
	}
	if measureCfg.SourceSentUnit == "" {
		measureCfg.SourceSentUnit = "auto"
	}
	if measureCfg.T0Unit == "" {
		measureCfg.T0Unit = "auto"
	}
	if u := normalizeUnit(measureCfg.SourceSentUnit); u != "auto" && u != "s" && u != "ms" && u != "us" {
		return fmt.Errorf("source-sent-unit must be auto, s, ms, or us")
	}
	if u := normalizeUnit(measureCfg.T0Unit); u != "auto" && u != "s" && u != "ms" && u != "us" {
		return fmt.Errorf("t0-unit must be auto, s, ms, or us")
	}

	sourceIndex, sourceStats, err := loadSourceIndex(
		measureCfg.SourceDump,
		measureCfg.MessageIDField,
		measureCfg.SourceSentField,
		measureCfg.SourceSentUnit,
	)
	if err != nil {
		return err
	}
	if len(sourceIndex) == 0 {
		return fmt.Errorf("source dump contains no valid message_id entries")
	}
	measureLogger.Printf("[SOURCE] lines=%d indexed=%d bad=%d dup=%d",
		sourceStats.Total, sourceStats.Indexed, sourceStats.Bad, sourceStats.Duplicates)
	targetCount := len(sourceIndex)
	found := make(map[string]struct{}, targetCount)
	foundCount := 0

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

	startUs := internal.NowMicros()
	endUs := startUs + int64(measureCfg.DurationSec)*1_000_000

	// Файл для записи результатов.
	f, err := os.Create(measureCfg.OutJSONL)
	if err != nil {
		return fmt.Errorf("create out file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	var (
		serveTimes []int64
		latencies  []int64
		total      int
		okCount    int
		badCount   int
		stopReason string
	)

	// Основной цикл измерений.
	for {
		if internal.NowMicros() >= endUs {
			stopReason = "timeout"
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
		ts := internal.NowMicros()

		rec := Record{
			OK: false,
		}

		// Парсим JSON объект.
		obj, err := decodeJSONMap([]byte(raw))
		if err != nil {
			badCount++
			rec.Error = "json_parse_error: " + err.Error()
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			logRecord(rec)
			continue
		}

		// message_id
		msgIDVal, ok := obj[measureCfg.MessageIDField]
		if !ok {
			badCount++
			rec.Error = "missing_" + measureCfg.MessageIDField
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			logRecord(rec)
			continue
		}
		msgID, ok := extractString(msgIDVal)
		if !ok {
			badCount++
			rec.Error = "bad_" + measureCfg.MessageIDField
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			logRecord(rec)
			continue
		}
		rec.MessageID = msgID
		if _, ok := sourceIndex[msgID]; ok {
			if _, seen := found[msgID]; !seen {
				found[msgID] = struct{}{}
				foundCount++
			}
		}
		shouldStop := foundCount >= targetCount

		// result sent_epoch
		var resultSentUs *int64
		if v, ok := obj[measureCfg.T0Field]; ok && v != nil {
			x, e := parseFieldToEpoch(v, measureCfg.T0Unit)
			if e == nil {
				resultSentUs = x
			}
		}
		rec.ResultSentUs = resultSentUs
		if resultSentUs == nil {
			badCount++
			rec.Error = "missing_or_bad_" + measureCfg.T0Field
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			logRecord(rec)
			if shouldStop {
				stopReason = "all-found"
				measureLogger.Printf("[STOP] all_messages_found=%d", foundCount)
				break
			}
			continue
		}

		sourceRec, ok := sourceIndex[msgID]
		if !ok {
			badCount++
			rec.Error = "source_not_found"
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			logRecord(rec)
			if shouldStop {
				stopReason = "all-found"
				measureLogger.Printf("[STOP] all_messages_found=%d", foundCount)
				break
			}
			continue
		}
		sourceSentUs := sourceRec.SentUs
		rec.SourceSentUs = &sourceSentUs

		serveUs := *resultSentUs - sourceSentUs
		if serveUs < 0 {
			badCount++
			rec.Error = "result_sent_before_source"
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			logRecord(rec)
			if shouldStop {
				stopReason = "all-found"
				measureLogger.Printf("[STOP] all_messages_found=%d", foundCount)
				break
			}
			continue
		}

		lat := ts - *resultSentUs
		if lat < 0 {
			badCount++
			rec.Error = "result_sent_in_future"
			b, _ := json.Marshal(rec)
			w.Write(b)
			w.WriteByte('\n')
			logRecord(rec)
			if shouldStop {
				stopReason = "all-found"
				measureLogger.Printf("[STOP] all_messages_found=%d", foundCount)
				break
			}
			continue
		}

		rec.OK = true
		rec.ServeUs = &serveUs
		rec.LatencyUs = &lat
		okCount++
		serveTimes = append(serveTimes, serveUs)
		latencies = append(latencies, lat)

		b, _ := json.Marshal(rec)
		w.Write(b)
		w.WriteByte('\n')
		logRecord(rec)

		if shouldStop {
			stopReason = "all-found"
			measureLogger.Printf("[STOP] all_messages_found=%d", foundCount)
			break
		}
	}
	if stopReason == "timeout" && foundCount < targetCount {
		measureLogger.Printf("[WARN] timeout before all dump messages were found: messages_received=%d messages_in_dump=%d missing=%d timeout_sec=%d total_read=%d",
			foundCount, targetCount, targetCount-foundCount, measureCfg.DurationSec, total)
	}
	lostIDs := make([]string, 0, targetCount-foundCount)
	for msgID := range sourceIndex {
		if _, ok := found[msgID]; !ok {
			lostIDs = append(lostIDs, msgID)
		}
	}
	sort.Strings(lostIDs)
	lostMessages := make([]json.RawMessage, 0, len(lostIDs))
	for _, msgID := range lostIDs {
		lostMessages = append(lostMessages, sourceIndex[msgID].Raw)
	}
	if err := writeLostJSON("lost.json", lostMessages); err != nil {
		return err
	}
	measureLogger.Printf("[LOST] path=lost.json count=%d", len(lostMessages))

	durS := float64(internal.NowMicros()-startUs) / 1_000_000.0
	if durS <= 0 {
		durS = 1e-9
	}
	// Итоговая статистика.
	throughput := float64(okCount) / durS
	measureLogger.Printf("[RESULT] total_read=%d ok=%d bad=%d duration_s=%.3f ok_throughput_msg_s=%.3f",
		total, okCount, badCount, durS, throughput)

	var (
		serveStats *percentileStats
		latStats   *percentileStats
	)
	if okCount > 0 {
		sort.Slice(serveTimes, func(i, j int) bool { return serveTimes[i] < serveTimes[j] })
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		serveStats = &percentileStats{
			P50: percentile(serveTimes, 0.50),
			P90: percentile(serveTimes, 0.90),
			P95: percentile(serveTimes, 0.95),
			P99: percentile(serveTimes, 0.99),
		}
		latStats = &percentileStats{
			P50: percentile(latencies, 0.50),
			P90: percentile(latencies, 0.90),
			P95: percentile(latencies, 0.95),
			P99: percentile(latencies, 0.99),
		}
		measureLogger.Printf("[SERVE] p50=%d us", serveStats.P50)
		measureLogger.Printf("[SERVE] p90=%d us", serveStats.P90)
		measureLogger.Printf("[SERVE] p95=%d us", serveStats.P95)
		measureLogger.Printf("[SERVE] p99=%d us", serveStats.P99)
		measureLogger.Printf("[SERVE] max=%d us", serveTimes[len(serveTimes)-1])
		measureLogger.Printf("[LAT] p50=%d us", latStats.P50)
		measureLogger.Printf("[LAT] p90=%d us", latStats.P90)
		measureLogger.Printf("[LAT] p95=%d us", latStats.P95)
		measureLogger.Printf("[LAT] p99=%d us", latStats.P99)
		measureLogger.Printf("[LAT] max=%d us", latencies[len(latencies)-1])
	}
	statsJSONPath := buildStatsJSONPath(measureCfg.OutJSONL)
	if err := writeStatsJSON(statsJSONPath, measureStatsFile{
		TotalRead:        total,
		OK:               okCount,
		Bad:              badCount,
		DurationSec:      durS,
		OKThroughputMsgS: throughput,
		ServeUs:          serveStats,
		LatencyUs:        latStats,
	}); err != nil {
		return err
	}
	measureLogger.Printf("[STATS] path=%s", statsJSONPath)

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
		measureLogger.Printf("[RESTORE] moved_back=%d from %s -> %s", moved, hq, measureCfg.ObsQueue)
	}
	return nil
}
