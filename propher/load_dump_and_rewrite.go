package propher

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"propher/internal"
	"time"
)

func main() {
	var (
		inDump     = flag.String("in-dump", "", "Input dump file (JSONL) (required)")
		outDump    = flag.String("out-dump", "", "Output dump file (JSONL) (required)")
		sentField  = flag.String("sent-field", "sent_epoch", "Field to rewrite (default sent_epoch)")
		epochUnit  = flag.String("epoch-unit", "ms", "Unit to write: ms or s (default ms)")
		mode       = flag.String("mode", "increment", "Rewrite mode: same or increment (default increment)")
		step       = flag.Int64("step", 1, "Step for increment mode (in ms or s depending on epoch-unit)")
		baseEpoch  = flag.Int64("base-epoch", 0, "Base epoch override (0 = now)")
		redisAddr  = flag.String("redis-addr", "127.0.0.1:6379", "If set, also load into Redis (host:port)")
		redisPass  = flag.String("redis-pass", "", "Redis password")
		redisDB    = flag.Int("redis-db", 0, "Redis DB")
		redisQueue = flag.String("redis-queue", "", "Target Redis LIST key to load into")
		redisPush  = flag.String("redis-push", "rpush", "rpush or lpush (default rpush)")
		clearQueue = flag.Bool("clear-queue", false, "DEL target queue before loading")
		batchSize  = flag.Int("batch", 1000, "Pipeline batch size (default 1000)")
	)
	flag.Parse()

	if *inDump == "" || *outDump == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --in-dump and --out-dump are required")
		os.Exit(2)
	}
	if *epochUnit != "ms" && *epochUnit != "s" {
		fmt.Fprintln(os.Stderr, "ERROR: --epoch-unit must be ms or s")
		os.Exit(2)
	}
	if *mode != "same" && *mode != "increment" {
		fmt.Fprintln(os.Stderr, "ERROR: --mode must be same or increment")
		os.Exit(2)
	}
	if *redisAddr != "" && *redisQueue == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --redis-queue is required when --redis-addr is set")
		os.Exit(2)
	}
	if *redisPush != "rpush" && *redisPush != "lpush" {
		fmt.Fprintln(os.Stderr, "ERROR: --redis-push must be rpush or lpush")
		os.Exit(2)
	}

	base := *baseEpoch
	if base == 0 {
		if *epochUnit == "ms" {
			base = internal.NowMS()
		} else {
			base = time.Now().Unix()
		}
	}

	inF, err := os.Open(*inDump)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: open in dump: %v\n", err)
		os.Exit(1)
	}
	defer inF.Close()

	outF, err := os.Create(*outDump)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: create out dump: %v\n", err)
		os.Exit(1)
	}
	defer outF.Close()

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

	if *redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: *redisAddr, Password: *redisPass, DB: *redisDB})
		if *clearQueue {
			if err := rdb.Del(ctx, *redisQueue).Err(); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: DEL queue: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("[REDIS] DEL %s\n", *redisQueue)
		}
		pipe = rdb.Pipeline()
	}

	flushPipe := func() {
		if pipe == nil {
			return
		}
		if _, err := pipe.Exec(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: redis pipeline exec: %v\n", err)
			os.Exit(1)
		}
	}

	enq := func(b []byte) {
		if pipe == nil {
			return
		}
		if *redisPush == "rpush" {
			pipe.RPush(ctx, *redisQueue, b)
		} else {
			pipe.LPush(ctx, *redisQueue, b)
		}
	}

	batch := *batchSize
	if batch <= 0 {
		batch = 1000
	}
	pending := 0

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
		if *mode == "same" {
			v = base
		} else {
			v = cur
			cur += *step
		}
		obj[*sentField] = v

		outBytes, err := json.Marshal(obj)
		if err != nil {
			nBad++
			continue
		}

		if _, err := outW.Write(outBytes); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: write out dump: %v\n", err)
			os.Exit(1)
		}
		if err := outW.WriteByte('\n'); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: write newline: %v\n", err)
			os.Exit(1)
		}
		nOut++

		if pipe != nil {
			enq(outBytes)
			pending++
			if pending >= batch {
				flushPipe()
				pending = 0
				fmt.Printf("[REDIS] pushed=%d\n", nOut)
			}
		}
	}
	if err := inScan.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: scan input: %v\n", err)
		os.Exit(1)
	}

	if pipe != nil && pending > 0 {
		flushPipe()
	}

	fmt.Printf("[DUMP] in_lines=%d out_lines=%d bad_lines_skipped=%d base=%d unit=%s mode=%s\n",
		nIn, nOut, nBad, base, *epochUnit, *mode)

	if rdb != nil {
		llen, err := rdb.LLen(ctx, *redisQueue).Result()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: LLEN: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[REDIS] done queue=%s llen=%d\n", *redisQueue, llen)
	}
}

// Minimal trim to avoid pulling bytes package just for this.
func bytesTrimSpace(b []byte) []byte {
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
