package propher

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Эти переменные обычно пробрасываются через -ldflags
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type Config struct {
	Input   string
	Timeout time.Duration
	Debug   bool

	// env
	APIKey  string
	BaseURL string
}

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

func parseConfig() (*Config, error) {
	var cfg Config

	envFile := flag.String("env-file", "", "path to .env file (optional)")
	flag.StringVar(&cfg.Input, "input", "", "input file path")
	flag.DurationVar(&cfg.Timeout, "timeout", 10*time.Second, "operation timeout")
	flag.BoolVar(&cfg.Debug, "debug", false, "enable debug output")
	showVersion := flag.Bool("version", false, "print version and exit")

	flag.Parse()

	if *showVersion {
		fmt.Printf("version=%s commit=%s date=%s\n", version, commit, date)
		os.Exit(0)
	}

	// Загружаем .env только если указан -env-file
	if *envFile != "" {
		path, err := expandPath(*envFile)
		if err != nil {
			return nil, fmt.Errorf("env-file: %w", err)
		}
		if err := loadDotEnv(path, false /* override */); err != nil {
			return nil, fmt.Errorf("load env-file: %w", err)
		}
	}

	// ENV
	cfg.APIKey = os.Getenv("MYTOOL_API_KEY")
	cfg.BaseURL = getenvDefault("MYTOOL_BASE_URL", "https://api.example.com")

	// Валидируем
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("MYTOOL_API_KEY is required (set env or use -env-file)")
	}
	if cfg.Input == "" {
		return nil, fmt.Errorf("-input is required")
	}

	return &cfg, nil
}

func run(ctx context.Context, cfg *Config) error {
	if cfg.Debug {
		fmt.Fprintf(os.Stderr, "debug: base_url=%s\n", cfg.BaseURL)
	}

	// пример работы
	select {
	case <-time.After(300 * time.Millisecond):
		fmt.Fprintln(os.Stdout, "ok")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// loadDotEnv читает .env и выставляет переменные окружения.
// override=false: НЕ перетирать уже заданные переменные в окружении.
// override=true: перетирать (обычно не надо).
func loadDotEnv(path string, override bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	lineNo := 0

	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())

		// пустые строки и комментарии
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// допускаем: export KEY=VALUE
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("%s:%d: invalid line (expected KEY=VALUE)", path, lineNo)
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		if key == "" {
			return fmt.Errorf("%s:%d: empty key", path, lineNo)
		}

		// убираем кавычки "..." или '...'
		val = unquoteEnv(val)

		if !override {
			if _, exists := os.LookupEnv(key); exists {
				continue
			}
		}

		if err := os.Setenv(key, val); err != nil {
			return fmt.Errorf("%s:%d: setenv(%s): %w", path, lineNo, key, err)
		}
	}

	if err := sc.Err(); err != nil {
		return err
	}

	return nil
}

func unquoteEnv(v string) string {
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			return v[1 : len(v)-1]
		}
	}
	return v
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func expandPath(p string) (string, error) {
	p = strings.TrimSpace(p)
	if p == "" {
		return "", errors.New("empty path")
	}
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if p == "~" {
			p = home
		} else if strings.HasPrefix(p, "~/") {
			p = filepath.Join(home, p[2:])
		}
	}
	return filepath.Clean(p), nil
}
