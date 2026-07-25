package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/DarkTheme404/real-time-analytics-platform/internal/config"
)

type mockProcessor struct {
	messages []*sarama.ConsumerMessage
	err      error
}

func (m *mockProcessor) ProcessMessage(_ context.Context, msg *sarama.ConsumerMessage) error {
	m.messages = append(m.messages, msg)
	return m.err
}

func TestConsumerHandler_ConsumeClaim(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	processor := &mockProcessor{}

	handler := &ConsumerHandler{
		processor: processor,
		logger:    logger,
		ready:     make(chan bool),
	}

	session := mocks.NewConsumerGroupSession(t)
	claim := mocks.NewConsumerGroupClaim(t)

	testValue, err := json.Marshal(map[string]interface{}{
		"event_id":   uuid.New().String(),
		"event_type": "page_view",
		"user_id":    "user-123",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	})
	require.NoError(t, err)

	msg := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Key:       []byte("test-key"),
		Value:     testValue,
		Timestamp: time.Now(),
	}

	claim.Messages().Return(msg)
	claim.Messages().Return(nil)
	claim.Context().Return(context.Background())
	session.Context().Return(context.Background())

	err = handler.ConsumeClaim(session, claim)
	assert.NoError(t, err)
	assert.Len(t, processor.messages, 1)
}

func TestProducer_SendMessage(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageSuccessfully()

	p := &Producer{
		producer: producer,
		logger:   logger,
		topic:    "test-topic",
		dlqTopic: "test-dlq",
	}

	err := p.SendMessage(
		[]byte("key"),
		[]byte("value"),
		map[string]string{"source": "test"},
	)
	assert.NoError(t, err)
	producer.Close()
}

func TestProducer_SendToDLQ(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageSuccessfully()

	p := &Producer{
		producer: producer,
		logger:   logger,
		topic:    "test-topic",
		dlqTopic: "test-dlq",
	}

	err := p.SendToDLQ([]byte("key"), []byte("value"), "test-reason")
	assert.NoError(t, err)
	producer.Close()
}

func TestNewConsumer(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	processor := &mockProcessor{}

	cfg := config.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "test-topic",
		ConsumerGroup: "test-group",
	}

	_, err := NewConsumer(cfg, processor, logger)
	assert.Error(t, err)
}

func TestNewProducer(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	_, err := NewProducer([]string{"localhost:9092"}, "test-topic", "test-dlq", logger)
	assert.Error(t, err)
}
