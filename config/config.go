package config

import (
	// Local Packages
	errors "tx-stream/errors"
)

var DefaultConfig = []byte(`
application: "tx-consumer"

logger:
  level: "info"

is_prod_mode: false

mongo:
  uri: "mongodb://localhost:27017"

redis:
  uri: "localhost:6379"
  password: ""

kafka:
  brokers:
    - "localhost:9092"
  consume: true
  topic: "transactions"
  channel_size: 1000
  records_per_poll: 5000
  consumer_name: "tx-consumer"
`)

type Config struct {
	Application string `koanf:"application"`
	Logger      Logger `koanf:"logger"`
	IsProdMode  bool   `koanf:"is_prod_mode"`
	Mongo       Mongo  `koanf:"mongo"`
	Redis       Redis  `koanf:"redis"`
	Kafka       Kafka  `koanf:"kafka"`
}

type Logger struct {
	Level string `koanf:"level"`
}

type Mongo struct {
	URI string `koanf:"uri"`
}

type Redis struct {
	URI      string `koanf:"uri"`
	Password string `koanf:"password"`
}

type Kafka struct {
	Brokers        []string `koanf:"brokers"`
	Consume        bool     `koanf:"consume"`
	Topic          string   `koanf:"topic"`
	ChannelSize    int      `koanf:"channel_size"`
	RecordsPerPoll int      `koanf:"records_per_poll"`
	ConsumerName   string   `koanf:"consumer_name"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	ve := errors.ValidationErrs()

	if c.Application == "" {
		ve.Add("application", "cannot be empty")
	}
	if c.Logger.Level == "" {
		ve.Add("logger.level", "cannot be empty")
	}
	if c.Mongo.URI == "" {
		ve.Add("mongo.uri", "cannot be empty")
	}
	if c.Redis.URI == "" {
		ve.Add("redis.uri", "cannot be empty")
	}
	if len(c.Kafka.Brokers) == 0 {
		ve.Add("kafka.brokers", "cannot be empty")
	}

	return ve.Err()
}
