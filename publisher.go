package rabbit_kit

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type publisher struct {
	connectionManager ConnectionManager
	config            *Config
	logger            zerolog.Logger
	confirmChannels   map[uint64]chan amqp.Confirmation
	confirmMutex      sync.RWMutex
	nextConfirmID     uint64
	confirmMux        sync.Mutex
}

func NewPublisher(connectionManager ConnectionManager, config *Config, logger zerolog.Logger) Publisher {
	return &publisher{
		connectionManager: connectionManager,
		config:            config,
		logger:            logger,
		confirmChannels:   make(map[uint64]chan amqp.Confirmation),
	}
}

func (p *publisher) Publish(ctx context.Context, exchange, routingKey string, msg *Message) error {
	return p.publishWithRetry(ctx, exchange, routingKey, msg, false)
}

func (p *publisher) publishWithRetry(ctx context.Context, exchange, routingKey string, msg *Message, withConfirmation bool) error {
	var lastErr error

	if msg == nil {
		return ErrInvalidMessage
	}

	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	if msg.DeliveryMode == 0 {
		msg.DeliveryMode = DeliveryModePersistent
	}

	maxAttempts := p.config.PublisherConfig.RetryAttempts + 1

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.config.PublisherConfig.RetryDelay):
			}

			p.logger.Info().Str("exchange", exchange).
				Str("routing_key", routingKey).
				Str("message_id", msg.ID).
				Int("attempt", attempt+1).
				Int("max_attempts", maxAttempts).
				Msg("Retrying message publish")
		}

		err := p.doPublish(ctx, exchange, routingKey, msg, withConfirmation)
		if err == nil {
			return nil
		}

		lastErr = err

		if !p.isRetryableError(err) {
			break
		}

		p.logger.Warn().Str("exchange", exchange).
			Str("routing_key", routingKey).
			Str("message_id", msg.ID).
			Int("attempt", attempt+1).
			Int("max_attempts", maxAttempts).
			Err(err).
			Msg("Retrying message publish")
	}

	return NewRetryError(maxAttempts, lastErr)
}

func (p *publisher) doPublish(ctx context.Context, exchange, routingKey string, msg *Message, withConfirmation bool) error {
	ch, err := p.connectionManager.GetChannel()
	if err != nil {
		return NewPublishError(exchange, routingKey, err)
	}
	defer p.connectionManager.ReturnChannel(ch)

	// Convert message to AMQP publishing
	publishing, err := p.messageToPublishing(msg)
	if err != nil {
		return NewPublishError(exchange, routingKey, fmt.Errorf("failed to convert message: %w", err))
	}

	// Publish the message
	err = ch.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		p.config.PublisherConfig.Mandatory,
		p.config.PublisherConfig.Immediate,
		*publishing,
	)

	if err != nil {
		return NewPublishError(exchange, routingKey, fmt.Errorf("failed to publish: %w", err))
	}

	p.logger.Debug().Str("exchange", exchange).
		Str("routing_key", routingKey).
		Str("message_id", msg.ID).
		Bool("confirmation", withConfirmation).
		Msg("Message published successfully")

	return nil
}

func (p *publisher) messageToPublishing(msg *Message) (*amqp.Publishing, error) {
	headers := make(amqp.Table)
	for k, v := range msg.Headers {
		headers[k] = v
	}

	// Add metadata headers
	headers["x-message-id"] = msg.ID
	headers["x-published-at"] = msg.Timestamp.Format(time.RFC3339)

	publishing := &amqp.Publishing{
		Headers:       headers,
		ContentType:   msg.ContentType,
		Body:          msg.Body,
		DeliveryMode:  msg.DeliveryMode,
		Priority:      msg.Priority,
		Timestamp:     msg.Timestamp,
		MessageId:     msg.ID,
		ReplyTo:       msg.ReplyTo,
		CorrelationId: msg.CorrelationID,
	}

	if msg.Expiration != "" {
		publishing.Expiration = msg.Expiration
	}

	return publishing, nil
}

func (p *publisher) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific error types that should not be retried
	switch err {
	case ErrInvalidMessage:
		return false
	case ErrInvalidConfig:
		return false
	}

	// Check for AMQP errors
	if amqpErr, ok := err.(*amqp.Error); ok {
		switch amqpErr.Code {
		case amqp.NotFound: // 404 - Queue/Exchange not found
			return false
		case amqp.AccessRefused: // 403 - Access refused
			return false
		case amqp.InvalidPath: // 402 - Invalid path
			return false
		case amqp.ResourceLocked: // 405 - Resource locked
			return false
		case amqp.PreconditionFailed: // 406 - Precondition failed
			return false
		case amqp.NotImplemented: // 540 - Not implemented
			return false
		default:
			return true
		}
	}

	// Check for connection errors (these are usually retryable)
	if _, ok := err.(*ConnectionError); ok {
		return true
	}

	return true
}

func (p *publisher) Close() error {
	p.logger.Info().Msg("Closing publisher")

	// Close all confirmation channels
	p.confirmMutex.Lock()
	for _, ch := range p.confirmChannels {
		close(ch)
	}
	p.confirmChannels = make(map[uint64]chan amqp.Confirmation)
	p.confirmMutex.Unlock()

	return nil
}
