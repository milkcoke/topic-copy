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
func LoadDotEnv(env string) {
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

	// optional TLS
	SSLCALocation                string // path to CA bundle if custom
	SSLEndpointIdentificationAlg string // "https" (default)

	// consumer
	GroupID         string
	AutoOffsetReset string // earliest/latest/none

	// producer
	EnableIdempotence bool
	Acks              string // all, 1, 0

	// tuning
	LingerMs          int
	BatchNumMessages  int
	CompressionType   string // lz4, zstd, snappy, gzip, none
	SocketKeepaliveMs int
}

// FromEnvPrefix reads envs with a prefix, e.g. "SRC_" or "TGT_"
func FromEnvPrefix(prefix string) ConnCfg {
	get := func(k, def string) string {
		if v := os.Getenv(prefix + k); v != "" {
			return v
		}
		return def
	}
	getInt := func(k string, def int) int {
		if v := os.Getenv(prefix + k); v != "" {
			var out int
			fmt.Sscanf(v, "%d", &out)
			return out
		}
		return def
	}
	getBool := func(k string, def bool) bool {
		v := strings.ToLower(os.Getenv(prefix + k))
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

		SSLCALocation:                get("SSL_CA_LOCATION", ""),
		SSLEndpointIdentificationAlg: get("SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "https"),

		GroupID:         get("GROUP_ID", "kafkacopy-"+fmt.Sprint(time.Now().Unix())),
		AutoOffsetReset: get("AUTO_OFFSET_RESET", "earliest"),

		EnableIdempotence: getBool("ENABLE_IDEMPOTENCE", true),
		Acks:              get("ACKS", "all"),

		LingerMs:          getInt("LINGER_MS", 5),
		BatchNumMessages:  getInt("BATCH_NUM_MESSAGES", 10_000),
		CompressionType:   get("COMPRESSION_TYPE", "lz4"),
		SocketKeepaliveMs: getInt("SOCKET_KEEPALIVE_MS", 60_000),
	}
}

func (c ConnCfg) ProducerConfig() *kafka.ConfigMap {
	m := &kafka.ConfigMap{
		"bootstrap.servers":       c.BootstrapServers,
		"security.protocol":       c.SecurityProtocol,
		"sasl.mechanisms":         c.SaslMechanism,
		"sasl.username":           c.SaslUsername,
		"sasl.password":           c.SaslPassword,
		"enable.idempotence":      c.EnableIdempotence,
		"acks":                    c.Acks,
		"linger.ms":               c.LingerMs,
		"batch.num.messages":      c.BatchNumMessages,
		"compression.type":        c.CompressionType,
		"socket.keepalive.enable": true,
		"socket.keepalive.ms":     c.SocketKeepaliveMs,
	}
	if c.SSLCALocation != "" {
		m.SetKey("ssl.ca.location", c.SSLCALocation)
	}
	if c.SSLEndpointIdentificationAlg != "" {
		m.SetKey("ssl.endpoint.identification.algorithm", c.SSLEndpointIdentificationAlg)
	}
	return m
}

func (c ConnCfg) ConsumerConfig() *kafka.ConfigMap {
	m := &kafka.ConfigMap{
		"bootstrap.servers":  c.BootstrapServers,
		"security.protocol":  c.SecurityProtocol,
		"sasl.mechanisms":    c.SaslMechanism,
		"sasl.username":      c.SaslUsername,
		"sasl.password":      c.SaslPassword,
		"group.id":           c.GroupID,
		"auto.offset.reset":  c.AutoOffsetReset,
		"enable.auto.commit": false, // we'll commit ourselves
		// cooperative-sticky is safer for rolling restarts on modern brokers
		"partition.assignment.strategy": "cooperative-sticky",
	}
	if c.SSLCALocation != "" {
		m.SetKey("ssl.ca.location", c.SSLCALocation)
	}
	if c.SSLEndpointIdentificationAlg != "" {
		m.SetKey("ssl.endpoint.identification.algorithm", c.SSLEndpointIdentificationAlg)
	}
	return m
}
