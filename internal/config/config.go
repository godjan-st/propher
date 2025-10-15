package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type RedisConfig struct {
	// Addr - адрес Redis в формате host:port.
	Addr string
	// Pass - пароль Redis.
	Pass string
	// DB - номер Redis DB.
	DB int
	// URL - полный URL Redis, приоритетнее адреса.
	URL string // optional, preferred if set
}

type Config struct {
	// Debug включает отладочный режим.
	Debug bool
	// Timeout - таймаут запросов.
	Timeout time.Duration
	// QueueName - имя очереди по умолчанию.
	QueueName string
	// Redis - параметры подключения к Redis.
	Redis RedisConfig
	// LoadDump - настройки режима load-dump-and-rewrite.
	LoadDump LoadDumpConfig
	// MeasureListLatency - настройки режима measure-list-latency.
	MeasureListLatency MeasureListLatencyConfig
}

type LoadDumpConfig struct {
	// InDump - путь к входному JSONL дампу.
	InDump string
	// OutDump - путь к выходному JSONL дампу.
	OutDump string
	// SentField - имя переписываемого поля.
	SentField string
	// EpochUnit - единица времени: ms или s.
	EpochUnit string
	// Mode - режим переписывания: same или increment.
	Mode string
	// Step - шаг инкремента времени.
	Step int64
	// BaseEpoch - базовое значение epoch (0 = текущий).
	BaseEpoch int64
	// RedisQueue - очередь Redis для загрузки.
	RedisQueue string
	// RedisPush - rpush или lpush.
	RedisPush string
	// ClearQueue - очищать очередь перед загрузкой.
	ClearQueue bool
	// BatchSize - размер батча пайплайна.
	BatchSize int
}

type MeasureListLatencyConfig struct {
	// ObsQueue - наблюдаемая очередь.
	ObsQueue string
	// HoldQueue - очередь удержания.
	HoldQueue string
	// DurationSec - длительность измерений в секундах.
	DurationSec int
	// BlockSec - таймаут BRPOPLPUSH в секундах.
	BlockSec int
	// MaxMessages - ограничение на число сообщений.
	MaxMessages int
	// OutJSONL - путь к JSONL-отчету.
	OutJSONL string
	// T0Field - поле с исходным временем.
	T0Field string
	// T0Unit - единица времени: ms или s.
	T0Unit string
	// TraceField - поле trace id.
	TraceField string
	// Restore - возвращать сообщения обратно.
	Restore bool
	// RestoreVerify - проверять пустоту очереди перед восстановлением.
	RestoreVerify bool
}

// Load loads .env (if present) and returns app config with defaults applied.
func Load() (*Config, error) {
	// Загружаем .env без ошибки, если файла нет.
	// Load .env if it exists; ignore error intentionally
	_ = godotenv.Load()

	redis, err := loadRedisConfig()
	if err != nil {
		return nil, err
	}

	timeout, err := getenvDuration("TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, err
	}

	// Собираем конфигурацию со значениями по умолчанию.
	return &Config{
		Debug:     getenvBool("DEBUG", false),
		Timeout:   timeout,
		QueueName: getenvDefault("QUEUE_NAME", "default"),
		Redis:     redis,
		LoadDump: LoadDumpConfig{
			SentField: "sent_epoch",
			EpochUnit: "ms",
			Mode:      "increment",
			Step:      1,
			RedisPush: "rpush",
			BatchSize: 1000,
		},
		MeasureListLatency: MeasureListLatencyConfig{
			DurationSec: 600,
			BlockSec:    1,
			OutJSONL:    "latency.jsonl",
			T0Field:     "sent_epoch",
			T0Unit:      "ms",
			TraceField:  "trace_id",
		},
	}, nil
}

// Priority:
//  1. REDIS_URL
//  2. REDIS_ADDR / REDIS_PASS / REDIS_DB
//
// Defaults:
//
//	REDIS_ADDR = 127.0.0.1:6379
//	REDIS_PASS = ""
//	REDIS_DB   = 0
func loadRedisConfig() (RedisConfig, error) {
	// Полный URL имеет приоритет.
	if url := os.Getenv("REDIS_URL"); url != "" {
		return RedisConfig{
			URL: url,
		}, nil
	}

	addr := getenvDefault("REDIS_ADDR", "127.0.0.1:6379")
	pass := os.Getenv("REDIS_PASS")
	db, err := getenvInt("REDIS_DB", 0)
	if err != nil {
		return RedisConfig{}, err
	}

	// Возвращаем адресную конфигурацию.
	return RedisConfig{
		Addr: addr,
		Pass: pass,
		DB:   db,
	}, nil
}

func getenvDefault(key, def string) string {
	// Берем строку из окружения или дефолт.
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) (int, error) {
	// Парсим int из окружения.
	if v := os.Getenv(key); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", key, err)
		}
		return parsed, nil
	}
	return def, nil
}

func getenvBool(key string, def bool) bool {
	// Парсим bool из окружения.
	if v := os.Getenv(key); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err == nil {
			return parsed
		}
	}
	return def
}

func getenvDuration(key string, def time.Duration) (time.Duration, error) {
	// Парсим duration из окружения.
	if v := os.Getenv(key); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", key, err)
		}
		return parsed, nil
	}
	return def, nil
}
