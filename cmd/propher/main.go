package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"propher/internal/config"
	app "propher/propher"
	"strings"
)

// Эти переменные обычно пробрасываются через -ldflags
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const (
	modeRun                = "run"
	modeLoadDumpAndRewrite = "load-dump-and-rewrite"
	modeMeasureListLatency = "measure-list-latency"
)

func main() {
	// Запуск приложения и выход с кодом результата.
	os.Exit(realMain())
}

func realMain() int {
	// Загружаем конфигурацию и определяем режим запуска.
	cfg, mode, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	// Диспатчим подкоманды.
	switch mode {
	case modeLoadDumpAndRewrite:
		if err := app.RunLoadDumpAndRewrite(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		return 0
	case modeMeasureListLatency:
		if err := app.RunMeasureListLatency(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		return 0
	default:
	}

	// Базовый режим исполнения.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	return 0
}

// run is placeholder
func run(ctx context.Context, cfg *config.Config) error {
	// TODO: основной режим не реализован.
	return nil
}

func parseConfig(args []string) (*config.Config, string, error) {
	// Базовая конфигурация из окружения и .env.
	cfg, err := config.Load()
	if err != nil {
		return nil, "", err
	}

	// Определяем режим до парсинга флагов.
	mode, modeSet, rest, err := extractMode(args)
	if err != nil {
		return nil, "", err
	}
	if len(rest) > 0 && isMode(rest[0]) {
		if modeSet && rest[0] != mode {
			return nil, "", fmt.Errorf("mode conflict: %q vs %q", mode, rest[0])
		}
		mode = rest[0]
		rest = rest[1:]
	}
	if mode == "" {
		mode = modeRun
	}

	// Общие флаги доступны всегда.
	fs := flag.NewFlagSet("propher", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	bindCommonFlags(fs, cfg)

	// Режимные флаги подключаем по выбранному режиму.
	switch mode {
	case modeLoadDumpAndRewrite:
		bindLoadDumpFlags(fs, &cfg.LoadDump)
	case modeMeasureListLatency:
		bindMeasureListLatencyFlags(fs, &cfg.MeasureListLatency)
	}

	if err := fs.Parse(rest); err != nil {
		return nil, "", err
	}

	// Сохраняем приоритет редис-конфигурации.
	setFlags := collectSetFlags(fs)
	if setFlags["redis-url"] {
		cfg.Redis.URL = strings.TrimSpace(cfg.Redis.URL)
	} else if setFlags["redis-addr"] || setFlags["redis-pass"] || setFlags["redis-db"] {
		cfg.Redis.URL = ""
	}

	return cfg, mode, nil
}

func bindCommonFlags(fs *flag.FlagSet, cfg *config.Config) {
	// Общие параметры CLI.
	fs.BoolVar(&cfg.Debug, "debug", cfg.Debug, "Enable debug logging")
	fs.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "Timeout duration (e.g. 5s, 1m)")
	fs.StringVar(&cfg.QueueName, "queue", cfg.QueueName, "Queue name")
	fs.StringVar(&cfg.Redis.URL, "redis-url", cfg.Redis.URL, "Redis URL")
	fs.StringVar(&cfg.Redis.Addr, "redis-addr", cfg.Redis.Addr, "Redis address")
	fs.StringVar(&cfg.Redis.Pass, "redis-pass", cfg.Redis.Pass, "Redis password")
	fs.IntVar(&cfg.Redis.DB, "redis-db", cfg.Redis.DB, "Redis database number")
}

func bindLoadDumpFlags(fs *flag.FlagSet, cfg *config.LoadDumpConfig) {
	// Параметры режима load-dump-and-rewrite.
	fs.StringVar(&cfg.InDump, "in-dump", cfg.InDump, "Input dump file (JSONL) (required)")
	fs.StringVar(&cfg.OutDump, "out-dump", cfg.OutDump, "Output dump file (JSONL) (required)")
	fs.StringVar(&cfg.SentField, "sent-field", cfg.SentField, "Field to rewrite")
	fs.StringVar(&cfg.EpochUnit, "epoch-unit", cfg.EpochUnit, "Unit to write: ms or s")
	fs.StringVar(&cfg.Mode, "mode", cfg.Mode, "Rewrite mode: same or increment")
	fs.Int64Var(&cfg.Step, "step", cfg.Step, "Step for increment mode (in ms or s depending on epoch-unit)")
	fs.Int64Var(&cfg.BaseEpoch, "base-epoch", cfg.BaseEpoch, "Base epoch override (0 = now)")
	fs.StringVar(&cfg.RedisQueue, "redis-queue", cfg.RedisQueue, "Target Redis LIST key to load into")
	fs.StringVar(&cfg.RedisPush, "redis-push", cfg.RedisPush, "rpush or lpush")
	fs.BoolVar(&cfg.ClearQueue, "clear-queue", cfg.ClearQueue, "DEL target queue before loading")
	fs.IntVar(&cfg.BatchSize, "batch", cfg.BatchSize, "Pipeline batch size")
}

func bindMeasureListLatencyFlags(fs *flag.FlagSet, cfg *config.MeasureListLatencyConfig) {
	// Параметры режима measure-list-latency.
	fs.StringVar(&cfg.ObsQueue, "obs-queue", cfg.ObsQueue, "Observed LIST key (required)")
	fs.StringVar(&cfg.HoldQueue, "hold-queue", cfg.HoldQueue, "Hold LIST key (default: <obs-queue>:hold)")
	fs.IntVar(&cfg.DurationSec, "duration-sec", cfg.DurationSec, "How long to measure (seconds)")
	fs.IntVar(&cfg.BlockSec, "block-sec", cfg.BlockSec, "BRPOPLPUSH timeout (seconds)")
	fs.IntVar(&cfg.MaxMessages, "max-messages", cfg.MaxMessages, "Stop after N messages (0 = unlimited)")
	fs.StringVar(&cfg.OutJSONL, "out-jsonl", cfg.OutJSONL, "Output JSONL path")
	fs.StringVar(&cfg.T0Field, "t0-field", cfg.T0Field, "Field containing t0")
	fs.StringVar(&cfg.T0Unit, "t0-unit", cfg.T0Unit, "Unit for t0: ms or s")
	fs.StringVar(&cfg.TraceField, "trace-field", cfg.TraceField, "Field containing trace id")
	fs.BoolVar(&cfg.Restore, "restore", cfg.Restore, "Restore messages from hold back to obs after measurement")
	fs.BoolVar(&cfg.RestoreVerify, "restore-verify-empty", cfg.RestoreVerify, "Refuse restore if obs-queue is non-empty at restore time")
}

func extractMode(args []string) (string, bool, []string, error) {
	// Разбираем режим из аргументов до обработки флагов.
	mode := ""
	modeSet := false
	rest := make([]string, 0, len(args))

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-mode" || arg == "--mode":
			if i+1 >= len(args) {
				return "", false, nil, fmt.Errorf("mode value missing")
			}
			mode = args[i+1]
			modeSet = true
			i++
			continue
		case strings.HasPrefix(arg, "-mode="):
			mode = strings.TrimPrefix(arg, "-mode=")
			modeSet = true
			continue
		case strings.HasPrefix(arg, "--mode="):
			mode = strings.TrimPrefix(arg, "--mode=")
			modeSet = true
			continue
		default:
			rest = append(rest, arg)
		}
	}

	return mode, modeSet, rest, nil
}

func isMode(value string) bool {
	// Проверяем, является ли значение известным режимом.
	switch value {
	case modeRun, modeLoadDumpAndRewrite, modeMeasureListLatency:
		return true
	default:
		return false
	}
}

func collectSetFlags(fs *flag.FlagSet) map[string]bool {
	// Собираем явно заданные флаги для приоритетов.
	setFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true
	})
	return setFlags
}
