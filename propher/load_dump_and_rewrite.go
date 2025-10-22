package propher

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"propher/internal"
	"propher/internal/config"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redis/go-redis/v9"
)

func RunLoadDumpAndRewrite(cfg *config.Config) error {
	// Входная точка для режима load-dump-and-rewrite.
	return runLoadDumpAndRewrite(context.Background(), cfg, newQueueWriter)
}

// queueWriterFactory описывает DI-фабрику для очередей.
type queueWriterFactory func(ctx context.Context, cfg *config.Config) (queueWriter, error)

// queueWriter описывает минимальный интерфейс очереди.
type queueWriter interface {
	// Enqueue добавляет сообщение в очередь.
	Enqueue(ctx context.Context, payload []byte) error
	// Flush сбрасывает отложенные сообщения.
	Flush(ctx context.Context) error
	// Close освобождает ресурсы.
	Close(ctx context.Context) error
	// Label возвращает имя транспорта для логов.
	Label() string
}

type queueReporter interface {
	// Report возвращает финальную строку отчета.
	Report(ctx context.Context) (string, error)
}

// runLoadDumpAndRewrite выполняет переписывание дампа с DI для очередей.
func runLoadDumpAndRewrite(ctx context.Context, cfg *config.Config, factory queueWriterFactory) error {
	// Основная логика переписывания дампа и загрузки в очередь (опционально).
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
	if loadCfg.MQTTTopic != "" && (loadCfg.MQTTQoS < 0 || loadCfg.MQTTQoS > 2) {
		return fmt.Errorf("mqtt-qos must be 0, 1, or 2")
	}
	if loadCfg.RedisQueue != "" && loadCfg.MQTTTopic != "" {
		return fmt.Errorf("redis-queue and mqtt-topic are mutually exclusive")
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

	writer, err := factory(ctx, cfg)
	if err != nil {
		return err
	}
	if writer != nil {
		defer writer.Close(ctx)
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
		if writer != nil {
			if err := writer.Enqueue(ctx, outBytes); err != nil {
				return err
			}
			pending++
			if pending >= batch {
				if err := writer.Flush(ctx); err != nil {
					return err
				}
				pending = 0
				fmt.Printf("[%s] pushed=%d\n", strings.ToUpper(writer.Label()), nOut)
			}
		}
	}
	if err := inScan.Err(); err != nil {
		return fmt.Errorf("scan input: %w", err)
	}

	// Досылаем оставшийся пайплайн.
	if writer != nil && pending > 0 {
		if err := writer.Flush(ctx); err != nil {
			return err
		}
	}

	fmt.Printf("[DUMP] in_lines=%d out_lines=%d bad_lines_skipped=%d base=%d unit=%s mode=%s\n",
		nIn, nOut, nBad, base, loadCfg.EpochUnit, loadCfg.Mode)

	// Проверка состояния очереди, если доступна отчетность.
	if reporter, ok := writer.(queueReporter); ok {
		report, err := reporter.Report(ctx)
		if err != nil {
			return err
		}
		if report != "" {
			fmt.Println(report)
		}
	}
	return nil
}

type redisQueueWriter struct {
	// Клиент Redis и пайплайн.
	client *redis.Client
	pipe   redis.Pipeliner
	queue  string
	push   string
}

// newRedisQueueWriter создает Redis-обертку для очереди.
func newRedisQueueWriter(ctx context.Context, cfg *config.Config) (*redisQueueWriter, error) {
	opts, err := redisOptions(cfg.Redis)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	if cfg.LoadDump.ClearQueue {
		if err := client.Del(ctx, cfg.LoadDump.RedisQueue).Err(); err != nil {
			return nil, fmt.Errorf("del queue: %w", err)
		}
		fmt.Printf("[REDIS] DEL %s\n", cfg.LoadDump.RedisQueue)
	}
	return &redisQueueWriter{
		client: client,
		pipe:   client.Pipeline(),
		queue:  cfg.LoadDump.RedisQueue,
		push:   cfg.LoadDump.RedisPush,
	}, nil
}

// Enqueue добавляет сообщение в Redis LIST.
func (r *redisQueueWriter) Enqueue(ctx context.Context, payload []byte) error {
	// Добавляем сообщение в очередь Redis.
	if r.push == "rpush" {
		r.pipe.RPush(ctx, r.queue, payload)
	} else {
		r.pipe.LPush(ctx, r.queue, payload)
	}
	return nil
}

// Flush выполняет Exec пайплайна Redis.
func (r *redisQueueWriter) Flush(ctx context.Context) error {
	// Сбрасываем пайплайн при достижении батча.
	if _, err := r.pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis pipeline exec: %w", err)
	}
	return nil
}

// Close закрывает Redis-клиент.
func (r *redisQueueWriter) Close(ctx context.Context) error {
	// Освобождаем ресурсы Redis.
	_ = ctx
	return r.client.Close()
}

// Label возвращает метку логов.
func (r *redisQueueWriter) Label() string {
	// Используем Redis как метку.
	return "redis"
}

// Report возвращает строку с длиной очереди.
func (r *redisQueueWriter) Report(ctx context.Context) (string, error) {
	// Формируем отчет по длине очереди.
	llen, err := r.client.LLen(ctx, r.queue).Result()
	if err != nil {
		return "", fmt.Errorf("llen: %w", err)
	}
	return fmt.Sprintf("[REDIS] done queue=%s llen=%d", r.queue, llen), nil
}

type mqttQueueWriter struct {
	// Клиент MQTT и параметры публикации.
	client  mqtt.Client
	topic   string
	qos     byte
	retain  bool
	timeout time.Duration
}

// newMQTTQueueWriter создает MQTT-обертку для очереди.
func newMQTTQueueWriter(cfg *config.Config) (*mqttQueueWriter, error) {
	if cfg.MQTT.Broker == "" {
		return nil, fmt.Errorf("mqtt-broker is required when mqtt-topic is set")
	}
	opts := mqtt.NewClientOptions().AddBroker(cfg.MQTT.Broker)
	if cfg.MQTT.ClientID != "" {
		opts.SetClientID(cfg.MQTT.ClientID)
	}
	if cfg.MQTT.Username != "" {
		opts.SetUsername(cfg.MQTT.Username)
		opts.SetPassword(cfg.MQTT.Password)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(cfg.Timeout) {
		return nil, fmt.Errorf("mqtt connect timeout")
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt connect: %w", err)
	}
	return &mqttQueueWriter{
		client:  client,
		topic:   cfg.LoadDump.MQTTTopic,
		qos:     byte(cfg.LoadDump.MQTTQoS),
		retain:  cfg.LoadDump.MQTTRetain,
		timeout: cfg.Timeout,
	}, nil
}

// Enqueue публикует сообщение в MQTT.
func (m *mqttQueueWriter) Enqueue(ctx context.Context, payload []byte) error {
	// Публикуем сообщение в MQTT.
	_ = ctx
	token := m.client.Publish(m.topic, m.qos, m.retain, payload)
	if !token.WaitTimeout(m.timeout) {
		return fmt.Errorf("mqtt publish timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt publish: %w", err)
	}
	return nil
}

// Flush для MQTT не требуется, поэтому это no-op.
func (m *mqttQueueWriter) Flush(ctx context.Context) error {
	// Ничего не делаем для MQTT.
	_ = ctx
	return nil
}

// Close закрывает MQTT-соединение.
func (m *mqttQueueWriter) Close(ctx context.Context) error {
	// Закрываем соединение MQTT.
	_ = ctx
	m.client.Disconnect(250)
	return nil
}

// Label возвращает метку логов.
func (m *mqttQueueWriter) Label() string {
	// Используем MQTT как метку.
	return "mqtt"
}

// newQueueWriter выбирает реализацию очереди по конфигурации.
func newQueueWriter(ctx context.Context, cfg *config.Config) (queueWriter, error) {
	// Возвращаем подходящий транспорт очереди.
	switch {
	case cfg.LoadDump.RedisQueue != "":
		return newRedisQueueWriter(ctx, cfg)
	case cfg.LoadDump.MQTTTopic != "":
		return newMQTTQueueWriter(cfg)
	default:
		return nil, nil
	}
}

// redisOptions готовит redis.Options с учетом URL.
func redisOptions(cfg config.RedisConfig) (*redis.Options, error) {
	// Предпочитаем URL, если он задан.
	if cfg.URL != "" {
		opts, err := redis.ParseURL(cfg.URL)
		if err != nil {
			return nil, fmt.Errorf("redis parse url: %w", err)
		}
		return opts, nil
	}
	return &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Pass,
		DB:       cfg.DB,
	}, nil
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
