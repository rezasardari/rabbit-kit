// Package rabbitmq provides a production-ready RabbitMQ client with DLQ support
package rabbit_kit

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const Version = "1.0.0"

type Publisher interface {
	Publish(ctx context.Context, exchange, routingKey string, msg *Message) error
	Close() error
}

type Consumer interface {
	Consume(ctx context.Context) error
	Close() error
}

type MessageHandler func(ctx context.Context, msg *Message)

type Client interface {
	Publisher() Publisher
	RegisterConsumer(handler MessageHandler, opts *ConsumerOptions) Consumer
	DeclareQueue(name string, opts QueueOptions) error
	DeclareExchange(name string, opts ExchangeOptions) error
	BindQueue(queue, exchange, routingKey string) error
	HealthCheck() error
	Close() error
}

type ConnectionManager interface {
	GetConnection() (*amqp.Connection, error)
	GetChannel() (*amqp.Channel, error)
	ReturnChannel(*amqp.Channel)
	Close() error
	IsConnected() bool
	NotifyConnectionLoss() <-chan *amqp.Error
}

type QueueOptions struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type ExchangeOptions struct {
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type ConsumerOptions struct {
	Queue         string
	ConsumerTag   string
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	PrefetchCount int
	PrefetchSize  int
	Args          amqp.Table
	ReconnectWait time.Duration
}

type PublisherOptions struct {
	ConfirmMode    bool
	Mandatory      bool
	Immediate      bool
	RetryAttempts  int
	RetryDelay     time.Duration
	ConfirmTimeout time.Duration
	Args           amqp.Table
}

const (
	ExchangeTypeDirect  = "direct"
	ExchangeTypeFanout  = "fanout"
	ExchangeTypeTopic   = "topic"
	ExchangeTypeHeaders = "headers"
)

const (
	DeliveryModeTransient  = 1
	DeliveryModePersistent = 2
)

const (
	PriorityLowest  = 0
	PriorityLow     = 64
	PriorityNormal  = 128
	PriorityHigh    = 192
	PriorityHighest = 255
)
