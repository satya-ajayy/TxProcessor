package kafka

import (
	// Go Internal Packages
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	// Local Packages
	models "tx-stream/models"

	// External Packages
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/zap"
)

const (
	PartitionAssignedLog = "%s: new partition assigned %s %d"
	ErrorPollingLog      = "%s: error while polling records %s %d"
	PollingRecordsLog    = "%s: polling for records"
	KillingConsumerLog   = "%s: killing consumer %s %d"
)

type TxProcessor interface {
	ProcessRecords(ctx context.Context, records []models.Record) error
}

type DeadLetterQueue interface {
	Send(ctx context.Context, records []models.Record) error
}

type PartitionConsumer struct {
	client    *kgo.Client
	topic     string
	partition int32
	processor TxProcessor
	dlq       DeadLetterQueue
	recs      chan kgo.FetchTopicPartition
	quit      chan bool
	done      chan bool
	logger    *zap.Logger
}

type TopicPartition struct {
	topic     string
	partition int32
}

type Consumer struct {
	client    *kgo.Client
	config    *models.ConsumerConfig
	processor TxProcessor
	consumers map[TopicPartition]*PartitionConsumer
	logger    *zap.Logger
	dlq       DeadLetterQueue
}

// NewTxConsumer creates a new consumer and starts a goroutine for each partition to consume the records fetched
// PS: Must call Poll to start consuming the records
func NewTxConsumer(conf *models.ConsumerConfig, logger *zap.Logger, processor TxProcessor, dlq DeadLetterQueue, m *kprom.Metrics) (*Consumer, error) {
	c := &Consumer{
		config:    conf,
		processor: processor,
		consumers: make(map[TopicPartition]*PartitionConsumer),
		logger:    logger,
		dlq:       dlq,
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.ConsumerGroup(conf.Name),
		kgo.ConsumeRegex(),
		kgo.WithHooks(m),
		kgo.ConsumeTopics(conf.Topic),
		kgo.OnPartitionsAssigned(c.Assigned),
		kgo.OnPartitionsRevoked(c.Revoked),
		kgo.OnPartitionsLost(c.Lost),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	c.client = client
	return c, nil
}

// Assigned creates a new consumer for each assigned partition and starts a goroutine to consume the records.
func (c *Consumer) Assigned(ctx context.Context, client *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			c.logger.Info(fmt.Sprintf(PartitionAssignedLog, c.config.Name, topic, partition))
			pc := &PartitionConsumer{
				client:    client,
				topic:     topic,
				partition: partition,
				processor: c.processor,
				dlq:       c.dlq,
				recs:      make(chan kgo.FetchTopicPartition, c.config.EachPartitionChanSize),
				quit:      make(chan bool),
				done:      make(chan bool),
				logger:    c.logger,
			}
			c.consumers[TopicPartition{topic, partition}] = pc
			go pc.Consume(ctx)
		}
	}
}

// Revoked commits the marked offsets and kills the consumers.
func (c *Consumer) Revoked(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
	c.logger.Warn("partitions revoked", zap.Any("partitions", revoked))
	c.KillConsumers(revoked)
	if err := client.CommitMarkedOffsets(ctx); err != nil {
		c.logger.Error("failed to commit marked offsets", zap.Error(err))
	}
}

// Lost kills the consumers.
func (c *Consumer) Lost(ctx context.Context, client *kgo.Client, lost map[string][]int32) {
	c.logger.Warn("partitions lost", zap.Any("partitions", lost))
	c.KillConsumers(lost)
}

// KillConsumers kills the consumers for the lost partitions and closes the consumer goroutine.
func (c *Consumer) KillConsumers(lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := TopicPartition{topic, partition}
			pc := c.consumers[tp]
			c.logger.Info(fmt.Sprintf(KillingConsumerLog, c.config.Name, topic, partition))
			close(c.consumers[tp].quit)
			delete(c.consumers, tp)
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}

// Close closes the client.
func (c *Consumer) Close() {
	// on client close PartitionsRevoked is called where we commit the marked offsets
	c.client.Close()
}

// Consume consumes the records from the partition. this will be called in a separate
// goroutine for each assigned partition. marks the records after processing.
func (pc *PartitionConsumer) Consume(ctx context.Context) {
	defer close(pc.done)
	for {
		select {
		case <-pc.quit:
			return
		case p := <-pc.recs:
			records := make([]models.Record, len(p.Records), len(p.Records))
			for idx, record := range p.Records {
				records[idx] = models.Record{
					Key:   record.Key,
					Value: record.Value,
					Topic: record.Topic,
				}
			}

			if err := pc.ProcessRecordsWithRetry(ctx, records); err != nil {
				pc.logger.Error("processing failed after retries, sending to DLQ", zap.Error(err))
				if err := pc.dlq.Send(ctx, records); err != nil {
					pc.logger.Error("failed to send records to DLQ", zap.Error(err))
				}
			}
			pc.client.MarkCommitRecords(p.Records...)
		}
	}
}

func (pc *PartitionConsumer) ProcessRecordsWithRetry(ctx context.Context, records []models.Record) error {
	var err error
	for attempt := 1; attempt <= 2; attempt++ {
		err = pc.processor.ProcessRecords(ctx, records)
		if err == nil {
			pc.logger.Info("successfully processed records", zap.Int("count", len(records)))
			return nil
		}
		pc.logger.Warn("processing failed, retrying...", zap.Int("attempt", attempt), zap.Error(err))
		jitter := time.Duration(rand.Int63n(int64(time.Second)) * (1 << attempt)) // 1s, 2s-4s, 4s-8s, 8s-16s
		time.Sleep(jitter)
	}
	return err
}

func (c *Consumer) Poll(ctx context.Context) error {
	defer c.Close()

	for {
		// Check if the context is canceled before polling
		if ctx.Err() != nil {
			c.logger.Warn("polling stopped: context canceled")
			return ctx.Err() // Exit gracefully
		}

		c.logger.Info(fmt.Sprintf(PollingRecordsLog, c.config.Name))
		fetches := c.client.PollRecords(ctx, c.config.RecordsPerPoll)

		// Handle client shutdown
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}

		// Handle context cancellation explicitly
		if errors.Is(fetches.Err0(), context.Canceled) {
			return errors.New("context got canceled")
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Error(fmt.Sprintf(ErrorPollingLog, c.config.Name, topic, partition), zap.Error(err))
		})

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			tp := TopicPartition{p.Topic, p.Partition}
			c.consumers[tp].recs <- p
		})

		c.client.AllowRebalance()
	}
}
