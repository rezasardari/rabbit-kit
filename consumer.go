package rabbit_kit

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type consumer struct {
	connectionManager ConnectionManager
	handler           MessageHandler
	opts              *ConsumerOptions
	logger            zerolog.Logger
	isConsuming       bool
	consumeMutex      sync.RWMutex
	shutdownCh        chan struct{}
	wg                sync.WaitGroup
}

func NewConsumer(connectionManager ConnectionManager, handler MessageHandler, opts *ConsumerOptions, logger zerolog.Logger) Consumer {
	return &consumer{
		connectionManager: connectionManager,
		handler:           handler,
		opts:              opts,
		logger:            logger,
		shutdownCh:        make(chan struct{}),
	}
}

func (c *consumer) Consume(ctx context.Context) error {
	c.consumeMutex.Lock()
	if c.isConsuming {
		c.consumeMutex.Unlock()
		return fmt.Errorf("consumer is already consuming")
	}
	c.isConsuming = true
	c.consumeMutex.Unlock()

	defer func() {
		c.consumeMutex.Lock()
		c.isConsuming = false
		c.consumeMutex.Unlock()
	}()

	c.logger.Info().Msgf("starting consumer for queue %s", c.opts.Queue)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().Msgf("stopping consumer for queue %s", c.opts.Queue)
			return ctx.Err()
		case <-c.shutdownCh:
			c.logger.Info().Msgf("stopping consumer for queue %s", c.opts.Queue)
			return nil
		default:
			if err := c.consumeLoop(ctx, c.opts.Queue, c.handler); err != nil {
				c.logger.Error().Msgf("error consuming message for queue %s: %s", c.opts.Queue, err)

				// If it's a connection error, wait and retry
				var connectionError *ConnectionError
				if errors.As(err, &connectionError) {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(c.opts.ReconnectWait):
						continue
					}
				}
				return err
			}
		}
	}
}

func (c *consumer) consumeLoop(ctx context.Context, queue string, handler MessageHandler) error {
	ch, err := c.connectionManager.GetChannel()
	if err != nil {
		return NewConsumeError(queue, err)
	}

	if c.opts.PrefetchCount > 0 {
		err = ch.Qos(
			c.opts.PrefetchCount,
			c.opts.PrefetchSize,
			false,
		)
		if err != nil {
			ch.Close()
			return NewConnectionError("set channel QoS", err)
		}
	}

	defer c.connectionManager.ReturnChannel(ch)

	// Start consuming
	deliveries, err := ch.Consume(
		queue,
		c.opts.ConsumerTag,
		c.opts.AutoAck,
		c.opts.Exclusive,
		c.opts.NoLocal,
		c.opts.NoWait,
		c.opts.Args,
	)
	if err != nil {
		return NewConsumeError(queue, fmt.Errorf("failed to start consuming: %w", err))
	}

	c.logger.Info().Msgf("starting consumer for queue %s", queue)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.shutdownCh:
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				c.logger.Warn().Msgf("stopping consumer for queue %s", queue)
				return NewConsumeError(queue, fmt.Errorf("delivery channel closed"))
			}

			c.wg.Add(1)
			go func(d amqp.Delivery) {
				defer c.wg.Done()
				c.handleDelivery(ctx, queue, d, handler)
			}(delivery)
		}
	}
}

func (c *consumer) handleDelivery(ctx context.Context, queue string, delivery amqp.Delivery, handler MessageHandler) {
	msg := c.deliveryToMessage(delivery)

	c.logger.Debug().Str("queue", queue).
		Str("message_id", msg.ID).
		Uint64("delivery_tag", delivery.DeliveryTag).
		Msg("Processing message")

	handler(ctx, msg)
}

func (c *consumer) deliveryToMessage(delivery amqp.Delivery) *Message {
	headers := make(map[string]interface{})
	for k, v := range delivery.Headers {
		headers[k] = v
	}

	msg := &Message{
		ID:            delivery.MessageId,
		Body:          delivery.Body,
		ContentType:   delivery.ContentType,
		Headers:       headers,
		Timestamp:     delivery.Timestamp,
		Expiration:    delivery.Expiration,
		Priority:      delivery.Priority,
		DeliveryMode:  delivery.DeliveryMode,
		ReplyTo:       delivery.ReplyTo,
		CorrelationID: delivery.CorrelationId,
		delivery:      &delivery, // Attach delivery for acknowledgment
		acknowledged:  false,
	}

	// Set ID from headers if not available in MessageId
	if msg.ID == "" {
		if id, ok := headers["x-message-id"].(string); ok {
			msg.ID = id
		}
	}

	return msg
}

func (c *consumer) Close() error {
	c.logger.Info().Msg("closing consumer")

	// Signal shutdown
	close(c.shutdownCh)

	// Wait for all message handlers to complete
	c.wg.Wait()

	return nil
}
