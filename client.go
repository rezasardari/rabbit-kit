package rabbit_kit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"sync"
)

type client struct {
	connectionManager ConnectionManager
	publisher         Publisher
	consumers         []Consumer
	consumersMutex    sync.RWMutex
	config            *Config
	logger            zerolog.Logger
}

func NewClient(config *Config, logger zerolog.Logger) (Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	config.ApplyDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	connMgr, err := NewConnectionManager(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	c := &client{
		connectionManager: connMgr,
		publisher:         NewPublisher(connMgr, config, logger),
		consumers:         make([]Consumer, 0),
		config:            config,
		logger:            logger,
	}

	return c, nil
}

func (c *client) Publisher() Publisher {

	return c.publisher
}

func (c *client) RegisterConsumer(handler MessageHandler, opts *ConsumerOptions) Consumer {
	newConsumer := NewConsumer(c.connectionManager, handler, opts, c.logger)

	c.consumersMutex.Lock()
	c.consumers = append(c.consumers, newConsumer)
	c.consumersMutex.Unlock()

	c.logger.Info().Msgf("registered consumer with options: %v", opts)
	return newConsumer
}

func (c *client) DeclareExchange(name string, opts ExchangeOptions) error {
	ch, err := c.connectionManager.GetChannel()
	if err != nil {
		return NewConnectionError("get channel for exchange declaration", err)
	}
	defer c.connectionManager.ReturnChannel(ch)

	err = ch.ExchangeDeclare(
		name,
		opts.Type,
		opts.Durable,
		opts.AutoDelete,
		opts.Internal,
		opts.NoWait,
		opts.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange '%s': %w", name, err)
	}

	c.logger.Info().Str("exchange", name).
		Str("type", opts.Type).
		Msg("Exchange declared successfully")

	return nil
}

func (c *client) DeclareQueue(name string, opts QueueOptions) error {
	ch, err := c.connectionManager.GetChannel()
	if err != nil {
		return NewConnectionError("get channel for queue declaration", err)
	}
	defer c.connectionManager.ReturnChannel(ch)

	args := amqp.Table{}
	if opts.Args != nil {
		for k, v := range opts.Args {
			args[k] = v
		}
	}

	_, err = ch.QueueDeclare(
		name,
		opts.Durable,
		opts.AutoDelete,
		opts.Exclusive,
		opts.NoWait,
		args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue '%s': %w", name, err)
	}

	c.logger.Info().Msgf("Queue declared successfully: %s", name)
	return nil
}

func (c *client) BindQueue(queue, exchange, routingKey string) error {
	ch, err := c.connectionManager.GetChannel()
	if err != nil {
		return NewConnectionError("get channel for queue binding", err)
	}
	defer c.connectionManager.ReturnChannel(ch)

	err = ch.QueueBind(
		queue,
		routingKey,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue '%s' to exchange '%s' with routing key '%s': %w", queue, exchange, routingKey, err)
	}

	c.logger.Info().Msgf("Queue binded successfully: %s", queue)
	return nil
}

func (c *client) HealthCheck() error {
	if !c.connectionManager.IsConnected() {
		return ErrConnectionLost
	}

	// Try to get a channel and perform a basic operation
	ch, err := c.connectionManager.GetChannel()
	if err != nil {
		return NewConnectionError("health check channel creation", err)
	}
	defer c.connectionManager.ReturnChannel(ch)

	return nil
}

func (c *client) Close() error {
	c.logger.Info().Msg("Closing RabbitMQ client...")

	var closeErrors []error

	if err := c.publisher.Close(); err != nil {
		closeErrors = append(closeErrors, fmt.Errorf("publisher close error: %w", err))
	}

	// Close all additional consumers
	c.consumersMutex.Lock()
	for i, consumer := range c.consumers {
		if err := consumer.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("consumer %d close error: %w", i, err))
		}
	}
	c.consumers = nil // Clear the slice
	c.consumersMutex.Unlock()

	if err := c.connectionManager.Close(); err != nil {
		closeErrors = append(closeErrors, fmt.Errorf("connection manager close error: %w", err))
	}

	if len(closeErrors) > 0 {
		return fmt.Errorf("errors during close: %v", closeErrors)
	}

	c.logger.Info().Msg("RabbitMQ client closed successfully")
	return nil
}
