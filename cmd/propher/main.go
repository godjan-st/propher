package propher

import (
	"context"
	"flag"
	"fmt"
	"os"
	"propher/internal/config"
	"strconv"
	"time"
)

// Эти переменные обычно пробрасываются через -ldflags
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

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
	return nil
}

func parseConfig() (*config.Config, error) {
	// Load .env-backed config first, then override with CLI flags (if set).
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}

	// Use custom flag types to track whether a flag was explicitly set.
	baseURL := stringFlag{value: cfg.BaseURL}
	debug := boolFlag{value: cfg.Debug}
	timeout := durationFlag{value: cfg.Timeout}
	queue := stringFlag{value: cfg.QueueName}
	redisURL := stringFlag{value: cfg.Redis.URL}
	redisAddr := stringFlag{value: cfg.Redis.Addr}
	redisPass := stringFlag{value: cfg.Redis.Pass}
	redisDB := intFlag{value: cfg.Redis.DB}

	flag.Var(&baseURL, "base-url", "Base URL")
	flag.Var(&debug, "debug", "Enable debug logging")
	flag.Var(&timeout, "timeout", "Timeout duration (e.g. 5s, 1m)")
	flag.Var(&queue, "queue", "Queue name")
	flag.Var(&redisURL, "redis-url", "Redis URL")
	flag.Var(&redisAddr, "redis-addr", "Redis address")
	flag.Var(&redisPass, "redis-pass", "Redis password")
	flag.Var(&redisDB, "redis-db", "Redis database number")
	flag.Parse()

	// Apply overrides only for flags that were passed.
	applyIfSet(baseURL.set, func() { cfg.BaseURL = baseURL.value })
	applyIfSet(debug.set, func() { cfg.Debug = debug.value })
	applyIfSet(timeout.set, func() { cfg.Timeout = timeout.value })
	applyIfSet(queue.set, func() { cfg.QueueName = queue.value })

	// Redis URL wins if set; otherwise allow fine-grained overrides.
	if redisURL.set {
		cfg.Redis.URL = redisURL.value
	} else if redisAddr.set || redisPass.set || redisDB.set {
		cfg.Redis.URL = ""
	}

	applyIfSet(redisAddr.set, func() { cfg.Redis.Addr = redisAddr.value })
	applyIfSet(redisPass.set, func() { cfg.Redis.Pass = redisPass.value })
	applyIfSet(redisDB.set, func() { cfg.Redis.DB = redisDB.value })

	return cfg, nil
}

func applyIfSet(set bool, apply func()) {
	if set {
		apply()
	}
}

type stringFlag struct {
	value string
	set   bool
}

func (f *stringFlag) String() string {
	return f.value
}

func (f *stringFlag) Set(value string) error {
	f.value = value
	f.set = true
	return nil
}

type boolFlag struct {
	value bool
	set   bool
}

func (f *boolFlag) String() string {
	return strconv.FormatBool(f.value)
}

func (f *boolFlag) Set(value string) error {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	f.value = parsed
	f.set = true
	return nil
}

type intFlag struct {
	value int
	set   bool
}

func (f *intFlag) String() string {
	return strconv.Itoa(f.value)
}

func (f *intFlag) Set(value string) error {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	f.value = parsed
	f.set = true
	return nil
}

type durationFlag struct {
	value time.Duration
	set   bool
}

func (f *durationFlag) String() string {
	return f.value.String()
}

func (f *durationFlag) Set(value string) error {
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	f.value = parsed
	f.set = true
	return nil
}
