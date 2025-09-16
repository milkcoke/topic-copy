package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/joho/godotenv"
)

// Load .env (base) + .env.<APP_ENV> (overrides)
func loadDotEnv(env string) {
	if env == "" {
		env = "dev"
	}
	files := []string{".env", fmt.Sprintf(".env.%s", env)}
	if err := godotenv.Overload(files...); err != nil {
		log.Printf("[config] no .env files loaded (%v); relying on real env", err)
	} else {
		log.Printf("[config] loaded %v", files)
	}
}

type ConnCfg struct {
	BootstrapServers string
	SecurityProtocol string // SASL_SSL
	SaslMechanism    string // SCRAM-SHA-512
	SaslUsername     string
	SaslPassword     string

	// consumer
	GroupID            string
	AutoOffsetReset    string // earliest/latest/none
	EnablePartitionEof bool   // true to get partition EOF messages
	IsolationLevel     string // read_committed/read_uncommitted

	// producer
	EnableIdempotence bool
	Acks              string // all, 1, 0

	// tuning
	LingerMs          int
	BatchNumMessages  int
	CompressionType   string // lz4, zstd, snappy, gzip, none
	SocketKeepaliveMs int
}

func GetCfgFromEnv(env string) ConnCfg {
	loadDotEnv(env)

	get := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	getInt := func(k string, def int) int {
		if v := os.Getenv(k); v != "" {
			var out int
			fmt.Sscanf(v, "%d", &out)
			return out
		}
		return def
	}
	getBool := func(k string, def bool) bool {
		v := strings.ToLower(os.Getenv(env + k))
		switch v {
		case "true", "1", "yes", "y":
			return true
		case "false", "0", "no", "n":
			return false
		default:
			return def
		}
	}

	return ConnCfg{
		BootstrapServers: get("BOOTSTRAP_SERVERS", ""),
		SecurityProtocol: get("SECURITY_PROTOCOL", "SASL_SSL"),
		SaslMechanism:    get("SASL_MECHANISM", "SCRAM-SHA-512"),
		SaslUsername:     get("SASL_USERNAME", ""),
		SaslPassword:     get("SASL_PASSWORD", ""),

		// ConsumerConfig
		GroupID:            get("GROUP_ID", "kafkacopy-"+fmt.Sprint(time.Now().Unix())),
		AutoOffsetReset:    get("AUTO_OFFSET_RESET", "earliest"),
		IsolationLevel:     get("ISOLATION_LEVEL", "read_committed"),
		EnablePartitionEof: getBool("ENABLE_PARTITION_EOF", true),

		// ProducerConfig
		EnableIdempotence: getBool("ENABLE_IDEMPOTENCE", true),
		Acks:              get("ACKS", "all"),

		LingerMs:         getInt("LINGER_MS", 100),
		BatchNumMessages: getInt("BATCH_NUM_MESSAGES", 10_000),
		CompressionType:  get("COMPRESSION_TYPE", "lz4"),
	}
}

func (c ConnCfg) ProducerConfig() *kafka.ConfigMap {
	m := &kafka.ConfigMap{
		"bootstrap.servers":  c.BootstrapServers,
		"security.protocol":  c.SecurityProtocol,
		"sasl.mechanisms":    c.SaslMechanism,
		"sasl.username":      c.SaslUsername,
		"sasl.password":      c.SaslPassword,
		"enable.idempotence": c.EnableIdempotence,
		"acks":               c.Acks,
		"linger.ms":          c.LingerMs,
		"batch.num.messages": c.BatchNumMessages,
		"compression.type":   c.CompressionType,
	}

	return m
}

func (c ConnCfg) ConsumerConfig() *kafka.ConfigMap {
	m := &kafka.ConfigMap{
		"bootstrap.servers":    c.BootstrapServers,
		"security.protocol":    c.SecurityProtocol,
		"sasl.mechanisms":      c.SaslMechanism,
		"sasl.username":        c.SaslUsername,
		"sasl.password":        c.SaslPassword,
		"group.id":             c.GroupID,
		"isolation.level":      c.IsolationLevel,
		"auto.offset.reset":    c.AutoOffsetReset,
		"enable.auto.commit":   false, // we'll commit ourselves
		"enable.partition.eof": c.EnablePartitionEof,
		// cooperative-sticky is safer for rolling restarts on modern brokers
		"partition.assignment.strategy": "cooperative-sticky",
	}

	return m
}
