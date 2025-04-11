package main

import (
	// Go Internal Packages
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Local Packages
	config "tx-stream/config"
	kafka "tx-stream/kafka"
	models "tx-stream/models"
	mongodb "tx-stream/repositories/mongodb"
	redis "tx-stream/repositories/redis"
	txpsr "tx-stream/services/processors"

	// External Packages
	"github.com/alecthomas/kingpin/v2"
	_ "github.com/jsternberg/zap-logfmt"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/zap"
)

// LoadConfig loads the default configuration and overrides it with the config file
// specified by the path defined in the config flag
func LoadConfig() *koanf.Koanf {
	configPathMsg := "path to the application config file"
	configPath := kingpin.Flag("config", configPathMsg).Short('c').Default("config.yml").String()

	kingpin.Parse()
	k := koanf.New(".")
	_ = k.Load(rawbytes.Provider(config.DefaultConfig), yaml.Parser())
	if *configPath != "" {
		_ = k.Load(file.Provider(*configPath), yaml.Parser())
	}
	return k
}

func main() {
	k := LoadConfig()
	appKonf := config.Config{}

	// Unmarshalling config into struct
	err := k.Unmarshal("", &appKonf)
	if err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	// Validate the config loaded
	if err = appKonf.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	if !appKonf.IsProdMode {
		k.Print()
	}

	if !appKonf.Kafka.Consume {
		log.Fatalf("kafka consumer is not enabled")
	}

	cfg := zap.NewProductionConfig()
	cfg.Encoding = "logfmt"
	_ = cfg.Level.UnmarshalText([]byte(k.String("logger.level")))
	cfg.InitialFields = make(map[string]any)
	cfg.InitialFields["host"], _ = os.Hostname()
	cfg.InitialFields["service"] = appKonf.Application
	cfg.OutputPaths = []string{"stdout"}
	logger, _ := cfg.Build()
	defer func() {
		_ = logger.Sync()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Mongo Connection
	mongoClient, err := mongodb.Connect(ctx, appKonf.Mongo.URI)
	if err != nil {
		logger.Fatal("cannot create mongo client", zap.Error(err))
	}

	// Redis Connection
	redisClient, err := redis.Connect(ctx, appKonf.Redis.URI, appKonf.Redis.Password)
	if err != nil {
		logger.Fatal("cannot create redis client", zap.Error(err))
	}

	txRepo := mongodb.NewTxRepository(mongoClient)
	dlQueue := redis.NewDeadLetterQueue(redisClient, logger)
	txProcessor := txpsr.NewTxProcessor(logger, txRepo)

	metrics := kprom.NewMetrics("et")
	conf := &models.ConsumerConfig{
		Brokers:               appKonf.Kafka.Brokers,
		Name:                  appKonf.Kafka.ConsumerName,
		Topic:                 appKonf.Kafka.Topic,
		EachPartitionChanSize: appKonf.Kafka.ChannelSize,
		RecordsPerPoll:        appKonf.Kafka.RecordsPerPoll,
	}

	txConsumer, err := kafka.NewTxConsumer(conf, logger, txProcessor, dlQueue, metrics)
	if err != nil {
		logger.Fatal("cannot create consumer", zap.Error(err))
	}

	go func() {
		if err = txConsumer.Poll(ctx); err != nil {
			logger.Fatal("cannot poll records from topic", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	<-shutdownCtx.Done()
	logger.Info("shutdown complete")
}
