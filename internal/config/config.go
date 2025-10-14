package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type RedisConfig struct {
	Addr string
	Pass string
	DB   int
	URL  string // optional, preferred if set
}

// Load loads .env (if present) and returns Redis config.
// Priority:
//  1. REDIS_URL
//  2. REDIS_ADDR / REDIS_PASS / REDIS_DB
//
// Defaults:
//
//	REDIS_ADDR = 127.0.0.1:6379
//	REDIS_PASS = ""
//	REDIS_DB   = 0
func LoadRedisConfig() (RedisConfig, error) {
	// Load .env if it exists; ignore error intentionally
	_ = godotenv.Load()

	// Prefer REDIS_URL if provided
	if url := os.Getenv("REDIS_URL"); url != "" {
		return RedisConfig{
			URL: url,
		}, nil
	}
	addr := os.Getenv("REDIS_URL")
	pass := os.Getenv("REDIS_PASS")
	db, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err == nil {
		db = 0
	}
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	return RedisConfig{
		Addr: addr,
		Pass: pass,
		DB:   db,
	}, nil
}
