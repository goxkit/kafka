# Kafka Package for GoKit

[![Go Reference](https://pkg.go.dev/badge/github.com/goxkit/kafka.svg)](https://pkg.go.dev/github.com/goxkit/kafka)
[![Go Report Card](https://goreportcard.com/badge/github.com/goxkit/kafka)](https://goreportcard.com/report/github.com/goxkit/kafka)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A robust and idiomatic Go implementation for working with Apache Kafka messaging systems within the GoKit ecosystem.

## Overview

The `kafka` package provides a clean, abstracted interface for interacting with Apache Kafka, implementing the messaging interfaces defined in the GoKit ecosystem. It wraps the well-established [segmentio/kafka-go](https://github.com/segmentio/kafka-go) library to provide a more developer-friendly API that integrates seamlessly with other GoKit components.

## Features

- **Type-safe Message Publishing**: Send strongly-typed messages to Kafka topics
- **Consumer-side Dispatching**: Register handlers for specific message types and topics
- **Concurrent Message Processing**: Handle messages from multiple topics simultaneously
- **Header Support**: Add and process message headers for metadata
- **Deadline Management**: Built-in timeout handling for publishing operations
- **Seamless Integration**: Works with GoKit's configuration, logging, and tracing systems
- **Error Handling**: Robust error handling and recovery mechanisms

## Installation

```bash
go get github.com/goxkit/kafka
```

## Usage

### Configuration

Configure Kafka using the GoKit configs package:

```go
import (
    configsBuilder "github.com/ralvescosta/gokit/configs_builder"
)

func main() {
    // Configure Kafka connection
    cfgs, err := configsBuilder.
        NewConfigsBuilder().
        Kafka().
        Build()
    if err != nil {
        cfgs.Logger.Fatal(err.Error())
    }
}
```

### Publishing Messages

```go
import (
    "context"
    "github.com/goxkit/kafka"
    configsBuilder "github.com/ralvescosta/gokit/configs_builder"
)

func main() {
    cfgs, err := configsBuilder.
        NewConfigsBuilder().
        Kafka().
        Build()
    if err != nil {
        cfgs.Logger.Fatal(err.Error())
    }

    // Create a new Kafka publisher
    publisher := kafka.NewPublisher(cfg)

    // Publish a message
    topic := "orders"
    key := "new-order"
    order := OrderCreated{ID: "123", CustomerID: "456"}

    err := publisher.Publish(context.Background(), &topic, nil, &key, order)
    if err != nil {
        // Handle error
    }
}
```

### Consuming Messages

```go
import (
    "context"
    "github.com/goxkit/kafka"
    configsBuilder "github.com/ralvescosta/gokit/configs_builder"
)

func main() {
    cfgs, err := configsBuilder.
        NewConfigsBuilder().
        Kafka().
        Build()
    if err != nil {
        cfgs.Logger.Fatal(err.Error())
    }

    // Create a new dispatcher
    dispatcher := kafka.NewDispatcher(cfg)

    // Define a handler function
    handler := func(ctx context.Context, msg []byte, headers map[string][]byte) error {
        // Process the message
        return nil
    }

    // Register the handler for a specific topic and message type
    err := dispatcher.Register("orders", OrderCreated{}, handler)
    if err != nil {
        // Handle error
    }

    // Start consuming messages (this is a blocking call)
    dispatcher.ConsumeBlocking()
}
```

## Advanced Usage

### Publishing with Deadlines

```go
func main() {
    // Publish with a 10-second deadline
    err := publisher.PublishDeadline(context.Background(), &topic, nil, &key, order)
}
```

### Multiple Handlers for Different Message Types

```go
func main() {
    // Register handlers for different message types on the same topic
    dispatcher.Register("orders", OrderCreated{}, handleOrderCreated)
    dispatcher.Register("orders", OrderCancelled{}, handleOrderCancelled)
}
```

## Integration with Other GoKit Packages

The Kafka package integrates with other GoKit components:

- **configs**: For Kafka connection configuration
- **logging**: For structured logging of operations
- **messaging**: Implements standard messaging interfaces
- **tracing**: For distributed tracing of message flows (when enabled)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [segmentio/kafka-go](https://github.com/segmentio/kafka-go) - The underlying Kafka client library
