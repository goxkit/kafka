// Copyright (c) 2025, The GoKit Authors
// MIT License
// All rights reserved.

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/goxkit/configs"
	"github.com/goxkit/logging"
	"github.com/goxkit/messaging"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// kafkaPublisher is the concrete implementation of the messaging.Publisher interface.
// It uses a Kafka writer to send messages to Kafka topics.
//
// This implementation provides a reliable way to publish messages to Kafka topics
// with support for message keys, headers, and configurable delivery guarantees.
// It handles the serialization of message payloads and manages the underlying
// Kafka connection.
//
// Fields:
// - logger: A structured logger for logging events and errors.
// - writer: A Kafka writer instance for sending messages. Uses segmentio/kafka-go.
type kafkaPublisher struct {
	logger logging.Logger
	writer *kafka.Writer
}

// NewPublisher creates a new instance of kafkaPublisher.
//
// This function initializes a new Kafka publisher with the provided configuration.
// It sets up a Kafka writer with the broker address from the configuration and
// configures it to use the LeastBytes balancer strategy for distributing messages
// among partitions.
//
// Parameters:
//   - configs: Configuration settings including Kafka host and logger.
//     Required fields:
//   - KafkaConfigs.Host: The Kafka broker address (e.g., "localhost:9092")
//   - Logger: A configured logger instance
//
// Returns:
// - A new instance of kafkaPublisher that implements the messaging.Publisher interface.
func NewPublisher(configs *configs.Configs) messaging.Publisher {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(configs.KafkaConfigs.Host),
		Balancer: &kafka.LeastBytes{},
	}

	return &kafkaPublisher{
		logger: configs.Logger,
		writer: writer,
	}
}

// Publish sends a message to the specified Kafka topic.
//
// This method publishes a message to the specified Kafka topic. It converts the message
// payload to bytes and sets up the Kafka message with the provided topic and key.
// The method validates that a destination topic is provided before attempting to publish.
// It logs the publishing action with topic and key information for observability.
//
// Parameters:
//   - ctx: The context for managing deadlines, cancellations, and other request-scoped values.
//     Can be used to cancel the operation or set timeouts.
//   - to: The destination topic where the message should be sent. Required and must not be empty.
//   - from: The source or origin of the message (optional, not currently used in Kafka implementation).
//   - key: A routing key or identifier for the message (optional).
//     When provided, it helps with message routing and partition selection.
//   - msg: The message payload to be sent. Will be converted to string and then to bytes.
//   - options: Additional dynamic parameters for the message (optional).
//     Not currently used in the Kafka implementation.
//
// Returns:
// - An error if the message could not be sent or if the topic is empty.
func (p *kafkaPublisher) Publish(ctx context.Context, to, from, key *string, msg any, options ...*messaging.Option) error {
	if to == nil || *to == "" {
		return fmt.Errorf("destination topic cannot be empty")
	}

	topic := *to
	messageKey := ""
	if key != nil {
		messageKey = *key
	}

	message := kafka.Message{
		Topic: topic,
		Key:   []byte(messageKey),
		Value: []byte(fmt.Sprintf("%v", msg)),
	}

	p.logger.Info("Publishing message", zap.String("topic", topic), zap.String("key", messageKey))
	return p.writer.WriteMessages(ctx, message)
}

// PublishDeadline sends a message to the specified Kafka topic with a deadline.
//
// This method ensures that the message is sent within a specific time limit by
// creating a new context with a 10-second timeout. This prevents operations from
// hanging indefinitely if there are network issues or if the Kafka broker is
// unresponsive. After setting up the deadline context, it delegates to the
// standard Publish method.
//
// Parameters:
//   - ctx: The parent context for managing cancellations and request-scoped values.
//     This will be wrapped with a timeout context.
//   - to: The destination topic where the message should be sent. Required and must not be empty.
//   - from: The source or origin of the message (optional, not currently used in Kafka implementation).
//   - key: A routing key or identifier for the message (optional).
//     When provided, it helps with message routing and partition selection.
//   - msg: The message payload to be sent. Will be converted to string and then to bytes.
//   - options: Additional dynamic parameters for the message (optional).
//     Not currently used in the Kafka implementation.
//
// Returns:
// - An error if the message could not be sent within the deadline or if the topic is empty.
func (p *kafkaPublisher) PublishDeadline(ctx context.Context, to, from, key *string, msg any, options ...*messaging.Option) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10) // 10-second timeout
	defer cancel()

	return p.Publish(ctx, to, from, key, msg, options...)
}
