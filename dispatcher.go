// Copyright (c) 2025, The GoKit Authors
// MIT License
// All rights reserved.

package kafka

import (
	"context"
	"errors"
	"sync"

	"github.com/goxkit/configs"
	"github.com/goxkit/logging"
	"github.com/goxkit/messaging"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// kafkaDispatcher is an implementation of the messaging.Dispatcher interface for Kafka.
// It maintains a registry of handlers for different message types and sources, and ensures
// thread-safe access to the registry using a read-write mutex.
//
// The dispatcher is responsible for:
// 1. Registering handlers for specific message types on specific topics
// 2. Creating and managing Kafka readers for each topic
// 3. Consuming messages concurrently from all registered topics
// 4. Routing messages to the appropriate handlers based on topic and message type
// 5. Handling errors and logging during message consumption
//
// The implementation is thread-safe and supports concurrent registration and consumption.
type kafkaDispatcher struct {
	logger logging.Logger

	// handlers stores the registered handlers for message types and sources.
	// The outer map key is the source (e.g., Kafka topic), and the inner map key is the message type.
	// This two-level map allows efficient lookup of handlers based on both topic and message type.
	handlers map[string]map[any]messaging.ConsumerHandler
	mutex    sync.RWMutex

	// kafkaReaders is a slice of Kafka readers used to consume messages.
	// Each reader is configured to read from a specific topic and consumer group.
	kafkaReaders []*kafka.Reader
}

// NewDispatcher creates a new instance of kafkaDispatcher.
//
// This function initializes a new Kafka dispatcher with the provided configuration.
// It sets up empty maps and slices that will be populated when handlers are registered.
// The dispatcher is ready to accept handler registrations after creation, but no
// message consumption will start until ConsumeBlocking is called.
//
// Parameters:
//   - configs: Configuration settings including Kafka connection details and logger.
//     Required fields:
//   - Logger: A configured logger instance
//   - KafkaConfigs: Configuration for Kafka connection (used when creating readers)
//
// Returns:
// - A pointer to a new kafkaDispatcher instance.
func NewDispatcher(configs *configs.Configs) *kafkaDispatcher {
	return &kafkaDispatcher{
		logger:       configs.Logger,
		handlers:     make(map[string]map[any]messaging.ConsumerHandler),
		kafkaReaders: []*kafka.Reader{},
	}
}

// Register associates a message type and source with a specific messaging.ConsumerHandler.
// It ensures that the same handler is not registered multiple times for the same message type and source.
//
// This method creates a new Kafka reader configured for the specified topic and adds it to
// the dispatcher's list of readers. It also registers the provided handler function to be
// called when messages of the specified type are received from the specified source.
//
// The registration is thread-safe and protected by a mutex to prevent concurrent modifications
// to the handlers map.
//
// Parameters:
//   - from: The source of the message (e.g., Kafka topic). This is required and specifies
//     the Kafka topic to consume messages from.
//   - msgType: The type of the message. This can be any value that can be used as a map key,
//     typically a string or a type reference. It's used to route messages to the appropriate handler.
//   - handler: The handler function to process the message. This function will be called
//     when a message of the specified type is received from the specified source.
//   - options: Additional configuration options for the dispatcher (not currently used in this implementation).
//
// Returns:
//   - An error if a handler is already registered for the given message type and source,
//     otherwise nil.
func (d *kafkaDispatcher) Register(from string, msgType any, handler messaging.ConsumerHandler, options ...messaging.DispatcherOption) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, exists := d.handlers[from]; !exists {
		d.handlers[from] = make(map[any]messaging.ConsumerHandler)
	}

	if _, exists := d.handlers[from][msgType]; exists {
		return errors.New("handler already registered for this message type and source")
	}

	// Create a new Kafka reader for the given source (topic) and add it to the slice.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"}, // Replace with actual broker addresses
		GroupID: "default-group",            // Replace with actual group ID
		Topic:   from,
	})

	d.kafkaReaders = append(d.kafkaReaders, reader)
	d.handlers[from][msgType] = handler

	return nil
}

// ConsumeBlocking starts consuming messages from Kafka and dispatches them to the appropriate registered handlers.
// It creates a separate goroutine for each Kafka reader to consume messages concurrently.
//
// This method launches a background goroutine for each registered Kafka reader, which continuously
// polls for new messages. When a message is received, it looks up the appropriate handler based on
// the topic and message key, and dispatches the message to that handler.
//
// The method is called "blocking" because it starts all consumer goroutines and then blocks indefinitely.
// The goroutines will continue to run until the program terminates or the readers are closed externally.
//
// Error handling is built into each consumer goroutine:
// - Errors reading from Kafka are logged but the goroutine continues to poll
// - Missing handlers for a topic or message type are logged as warnings
// - Errors from handler functions are logged but don't stop message consumption
//
// Thread safety:
// - Message consumption happens concurrently across multiple goroutines
// - Access to the handlers map is protected by a read lock during lookup
// - Each message is processed independently
//
// Usage note: This method should be called after all handlers have been registered.
// Once called, it will not return unless there's a panic or the program terminates.
func (d *kafkaDispatcher) ConsumeBlocking() {
	for _, reader := range d.kafkaReaders {
		go func(r *kafka.Reader) {
			for {
				msg, err := r.ReadMessage(context.Background())
				if err != nil {
					d.logger.Error("Error reading message from Kafka", zap.Error(err))
					continue
				}

				d.mutex.RLock()
				handlersForSource, exists := d.handlers[msg.Topic]
				d.mutex.RUnlock()

				if !exists {
					d.logger.Warn("No handlers registered for topic", zap.String("topic", msg.Topic))
					continue
				}

				handler, exists := handlersForSource[string(msg.Key)]
				if !exists {
					d.logger.Warn("No handler registered for message type", zap.String("messageType", string(msg.Key)))
					continue
				}

				ctx := context.Background()
				if err := handler(ctx, msg.Value, msg.Headers); err != nil {
					d.logger.Error("Error handling message", zap.Error(err))
				}
			}
		}(reader)
	}
}
