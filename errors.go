package rabbit_kit

import (
	"errors"
	"fmt"
)

var (
	ErrConnectionLost      = errors.New("rabbitmq connection lost")
	ErrConnectionFailed    = errors.New("failed to connect to rabbitmq")
	ErrChannelClosed       = errors.New("rabbitmq channel closed")
	ErrInvalidConfig       = errors.New("invalid configuration")
	ErrPublishFailed       = errors.New("failed to publish message")
	ErrConsumeFailed       = errors.New("failed to consume message")
	ErrConfirmationTimeout = errors.New("message confirmation timeout")
	ErrSerializationFailed = errors.New("message serialization failed")
	ErrMaxRetriesExceeded  = errors.New("maximum retry attempts exceeded")
	ErrInvalidMessage      = errors.New("invalid message format")
	ErrQueueNotExists      = errors.New("queue does not exist")
	ErrExchangeNotExists   = errors.New("exchange does not exist")
)

type ConnectionError struct {
	Operation string
	Err       error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection error during %s: %v", e.Operation, e.Err)
}

type PublishError struct {
	Exchange   string
	RoutingKey string
	Err        error
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("publish error to exchange '%s' with routing key '%s': %v", e.Exchange, e.RoutingKey, e.Err)
}

type ConsumeError struct {
	Queue string
	Err   error
}

func (e *ConsumeError) Error() string {
	return fmt.Sprintf("consume error from queue '%s': %v", e.Queue, e.Err)
}

type ConfigurationError struct {
	Field  string
	Value  interface{}
	Reason string
}

func (e *ConfigurationError) Error() string {
	return fmt.Sprintf("configuration error: field '%s' with value '%v' - %s", e.Field, e.Value, e.Reason)
}

type RetryError struct {
	Attempts int
	LastErr  error
}

func (e *RetryError) Error() string {
	return fmt.Sprintf("retry failed after %d attempts: %v", e.Attempts, e.LastErr)
}

func NewConnectionError(operation string, err error) *ConnectionError {
	return &ConnectionError{
		Operation: operation,
		Err:       err,
	}
}

func NewPublishError(exchange, routingKey string, err error) *PublishError {
	return &PublishError{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Err:        err,
	}
}

func NewConsumeError(queue string, err error) *ConsumeError {
	return &ConsumeError{
		Queue: queue,
		Err:   err,
	}
}

func NewConfigurationError(field string, value interface{}, reason string) *ConfigurationError {
	return &ConfigurationError{
		Field:  field,
		Value:  value,
		Reason: reason,
	}
}

func NewRetryError(attempts int, lastErr error) *RetryError {
	return &RetryError{
		Attempts: attempts,
		LastErr:  lastErr,
	}
}
