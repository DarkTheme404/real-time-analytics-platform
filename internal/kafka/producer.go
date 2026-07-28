package kafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Producer struct {
	producer sarama.SyncProducer
	logger   *zap.Logger
	topic    string
	dlqTopic string
}

func NewProducer(brokers []string, topic, dlqTopic string, logger *zap.Logger) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 100 * time.Millisecond
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Version = sarama.V3_0_0_0

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &Producer{
		producer: producer,
		logger:   logger,
		topic:    topic,
		dlqTopic: dlqTopic,
	}, nil
}

func (p *Producer) SendMessage(key, value []byte, headers map[string]string) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	for k, v := range headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	p.logger.Debug("Message sent",
		zap.String("topic", p.topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	return nil
}

// SendToDLQ - отправка в Dead Letter Queue с метаданными о причине.
// DLQ позволяет исследовать проблемные сообщения и восстановить их обработку.
func (p *Producer) SendToDLQ(key, value []byte, reason string) error {
	msg := &sarama.ProducerMessage{
		Topic: p.dlqTopic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("dlq_reason"),
				Value: []byte(reason),
			},
			{
				Key:   []byte("original_topic"),
				Value: []byte(p.topic),
			},
			{
				Key:   []byte("dlq_timestamp"),
				Value: []byte(time.Now().UTC().Format(time.RFC3339)),
			},
		},
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		p.logger.Error("Failed to send message to DLQ",
			zap.Error(err),
			zap.String("topic", p.dlqTopic),
		)
		return fmt.Errorf("failed to send to DLQ: %w", err)
	}

	p.logger.Warn("Message sent to DLQ",
		zap.String("reason", reason),
		zap.String("topic", p.dlqTopic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	return nil
}

func (p *Producer) Close() error {
	p.logger.Info("Closing Kafka producer")
	return p.producer.Close()
}
