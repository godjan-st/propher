package propher

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"propher/internal"
	"propher/internal/config"
	"time"

	"github.com/redis/go-redis/v9"
)

func RunLoadDumpAndRewrite(cfg *config.Config) error {
	// Основная логика переписывания дампа и загрузки в Redis (опционально).
	loadCfg := cfg.LoadDump
	if loadCfg.InDump == "" || loadCfg.OutDump == "" {
		return fmt.Errorf("in-dump and out-dump are required")
	}
	if loadCfg.EpochUnit != "ms" && loadCfg.EpochUnit != "s" {
		return fmt.Errorf("epoch-unit must be ms or s")
	}
	if loadCfg.Mode != "same" && loadCfg.Mode != "increment" {
		return fmt.Errorf("mode must be same or increment")
	}
	if loadCfg.RedisQueue != "" && loadCfg.RedisPush != "rpush" && loadCfg.RedisPush != "lpush" {
		return fmt.Errorf("redis-push must be rpush or lpush")
	}

	base := loadCfg.BaseEpoch
	if base == 0 {
		if loadCfg.EpochUnit == "ms" {
			base = internal.NowMS()
		} else {
			base = time.Now().Unix()
		}
	}

	inF, err := os.Open(loadCfg.InDump)
	if err != nil {
		return fmt.Errorf("open in dump: %w", err)
	}
	defer inF.Close()

	outF, err := os.Create(loadCfg.OutDump)
	if err != nil {
		return fmt.Errorf("create out dump: %w", err)
	}
	defer outF.Close()

	// Сканы входных строк и буфер вывода.
	inScan := bufio.NewScanner(inF)
	// Allow big lines (messages) up to 32MB.
	buf := make([]byte, 0, 1024*1024)
	inScan.Buffer(buf, 32*1024*1024)

	outW := bufio.NewWriterSize(outF, 1<<20)
	defer outW.Flush()

	var (
		nIn  int64
		nOut int64
		nBad int64
		cur  = base
	)

	var rdb *redis.Client
	var pipe redis.Pipeliner
	ctx := context.Background()

	// Подключение к Redis при наличии очереди.
	if loadCfg.RedisQueue != "" {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Pass, DB: cfg.Redis.DB})
		if loadCfg.ClearQueue {
			if err := rdb.Del(ctx, loadCfg.RedisQueue).Err(); err != nil {
				return fmt.Errorf("del queue: %w", err)
			}
			fmt.Printf("[REDIS] DEL %s\n", loadCfg.RedisQueue)
		}
		pipe = rdb.Pipeline()
	}

	flushPipe := func() error {
		// Сбрасываем пайплайн при достижении батча.
		if pipe == nil {
			return nil
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("redis pipeline exec: %w", err)
		}
		return nil
	}

	enq := func(b []byte) {
		// Добавляем сообщение в очередь Redis.
		if pipe == nil {
			return
		}
		if loadCfg.RedisPush == "rpush" {
			pipe.RPush(ctx, loadCfg.RedisQueue, b)
		} else {
			pipe.LPush(ctx, loadCfg.RedisQueue, b)
		}
	}

	batch := loadCfg.BatchSize
	if batch <= 0 {
		batch = 1000
	}
	pending := 0

	// Основной проход по строкам дампа.
	for inScan.Scan() {
		nIn++
		line := inScan.Bytes()
		trimmed := bytesTrimSpace(line)
		if len(trimmed) == 0 {
			nBad++
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			nBad++
			continue
		}

		var v int64
		if loadCfg.Mode == "same" {
			v = base
		} else {
			v = cur
			cur += loadCfg.Step
		}
		obj[loadCfg.SentField] = v

		outBytes, err := json.Marshal(obj)
		if err != nil {
			nBad++
			continue
		}

		if _, err := outW.Write(outBytes); err != nil {
			return fmt.Errorf("write out dump: %w", err)
		}
		if err := outW.WriteByte('\n'); err != nil {
			return fmt.Errorf("write newline: %w", err)
		}
		nOut++

		// Пакетная отправка в Redis.
		if pipe != nil {
			enq(outBytes)
			pending++
			if pending >= batch {
				if err := flushPipe(); err != nil {
					return err
				}
				pending = 0
				fmt.Printf("[REDIS] pushed=%d\n", nOut)
			}
		}
	}
	if err := inScan.Err(); err != nil {
		return fmt.Errorf("scan input: %w", err)
	}

	// Досылаем оставшийся пайплайн.
	if pipe != nil && pending > 0 {
		if err := flushPipe(); err != nil {
			return err
		}
	}

	fmt.Printf("[DUMP] in_lines=%d out_lines=%d bad_lines_skipped=%d base=%d unit=%s mode=%s\n",
		nIn, nOut, nBad, base, loadCfg.EpochUnit, loadCfg.Mode)

	// Проверка длины очереди, если Redis был задействован.
	if rdb != nil {
		llen, err := rdb.LLen(ctx, loadCfg.RedisQueue).Result()
		if err != nil {
			return fmt.Errorf("llen: %w", err)
		}
		fmt.Printf("[REDIS] done queue=%s llen=%d\n", loadCfg.RedisQueue, llen)
	}
	return nil
}

// Minimal trim to avoid pulling bytes package just for this.
func bytesTrimSpace(b []byte) []byte {
	// Обрезаем пробелы по краям без дополнительных зависимостей.
	left := 0
	right := len(b) - 1
	for left <= right {
		if b[left] == ' ' || b[left] == '\n' || b[left] == '\r' || b[left] == '\t' {
			left++
		} else {
			break
		}
	}
	for right >= left {
		if b[right] == ' ' || b[right] == '\n' || b[right] == '\r' || b[right] == '\t' {
			right--
		} else {
			break
		}
	}
	if left > right {
		return []byte{}
	}
	return b[left : right+1]
}
