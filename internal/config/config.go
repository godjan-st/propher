package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type RedisConfig struct {
	Addr string
	Pass string
	DB   int
	URL  string // optional, preferred if set
}

type Config struct {
	BaseURL   string
	Debug     bool
	Timeout   time.Duration
	QueueName string
	Redis     RedisConfig
}

// Load loads .env (if present) and returns app config with defaults applied.
func Load() (*Config, error) {
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

	return &Config{
		BaseURL:   getenvDefault("BASE_URL", "http://localhost:8080"),
		Debug:     getenvBool("DEBUG", false),
		Timeout:   timeout,
		QueueName: getenvDefault("QUEUE_NAME", "default"),
		Redis:     redis,
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

	return RedisConfig{
		Addr: addr,
		Pass: pass,
		DB:   db,
	}, nil
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) (int, error) {
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
	if v := os.Getenv(key); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err == nil {
			return parsed
		}
	}
	return def
}

func getenvDuration(key string, def time.Duration) (time.Duration, error) {
	if v := os.Getenv(key); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", key, err)
		}
		return parsed, nil
	}
	return def, nil
}
