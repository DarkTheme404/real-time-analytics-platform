package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/DarkTheme404/real-time-analytics-platform/internal/config"
)

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	handler       *ConsumerHandler
	logger        *zap.Logger
	brokers       []string
	groupID       string
	topics        []string
}

type ConsumerHandler struct {
	processor Processor
	logger    *zap.Logger
	ready     chan bool
}

type Processor interface {
	ProcessMessage(ctx context.Context, msg *sarama.ConsumerMessage) error
}

func NewConsumer(cfg config.KafkaConfig, processor Processor, logger *zap.Logger) (*Consumer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	// Начинаем с самого старого оффсета, чтобы не терять события при старте
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = true
	saramaCfg.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second
	saramaCfg.Version = sarama.V3_0_0_0

	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	handler := &ConsumerHandler{
		processor: processor,
		logger:    logger,
		ready:     make(chan bool),
	}

	return &Consumer{
		consumerGroup: group,
		handler:       handler,
		logger:        logger,
		brokers:       cfg.Brokers,
		groupID:       cfg.ConsumerGroup,
		topics:        []string{cfg.Topic},
	}, nil
}

func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("Starting Kafka consumer",
		zap.Strings("brokers", c.brokers),
		zap.String("group", c.groupID),
		zap.Strings("topics", c.topics),
	)

	go func() {
		for {
			if err := c.consumerGroup.Consume(ctx, c.topics, c.handler); err != nil {
				c.logger.Error("Consumer group error", zap.Error(err))
			}
			if ctx.Err() != nil {
				return
			}
			c.handler.ready = make(chan bool)
		}
	}()

	<-c.handler.ready
	c.logger.Info("Kafka consumer started successfully")
}

func (c *Consumer) Close() error {
	c.logger.Info("Closing Kafka consumer")
	return c.consumerGroup.Close()
}

func (h *ConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *ConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim - основной цикл обработки сообщений из partition.
// MarkMessage вызывается только после успешной обработки (at-least-once).
func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			h.logger.Debug("Message received",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
			)

			if err := h.processor.ProcessMessage(session.Context(), msg); err != nil {
				h.logger.Error("Failed to process message",
					zap.Error(err),
					zap.String("topic", msg.Topic),
					zap.Int32("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
				)
				continue
			}

			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
