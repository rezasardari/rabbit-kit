package rabbit_kit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Message struct {
	ID            string                 `json:"id"`
	Body          []byte                 `json:"body"`
	ContentType   string                 `json:"content_type"`
	Headers       map[string]interface{} `json:"headers"`
	Timestamp     time.Time              `json:"timestamp"`
	Expiration    string                 `json:"expiration,omitempty"`
	Priority      uint8                  `json:"priority,omitempty"`
	DeliveryMode  uint8                  `json:"delivery_mode"`
	ReplyTo       string                 `json:"reply_to,omitempty"`
	CorrelationID string                 `json:"correlation_id,omitempty"`

	// Internal fields for acknowledgment (not exported in JSON)
	delivery     *amqp.Delivery `json:"-"`
	acknowledged bool           `json:"-"`
	ackMutex     sync.Mutex     `json:"-"`
}

func (m *Message) Ack() error {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.delivery == nil {
		return fmt.Errorf("message delivery is nil - cannot acknowledge")
	}

	if m.acknowledged {
		return fmt.Errorf("message already acknowledged")
	}

	m.acknowledged = true
	return m.delivery.Ack(false)
}

func (m *Message) AckMultiple() error {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.delivery == nil {
		return fmt.Errorf("message delivery is nil - cannot acknowledge")
	}

	if m.acknowledged {
		return fmt.Errorf("message already acknowledged")
	}

	m.acknowledged = true
	return m.delivery.Ack(true)
}

func (m *Message) Nack(requeue bool) error {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.delivery == nil {
		return fmt.Errorf("message delivery is nil - cannot nack")
	}

	if m.acknowledged {
		return fmt.Errorf("message already acknowledged")
	}

	m.acknowledged = true
	return m.delivery.Nack(false, requeue)
}

func (m *Message) NackMultiple(requeue bool) error {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.delivery == nil {
		return fmt.Errorf("message delivery is nil - cannot nack")
	}

	if m.acknowledged {
		return fmt.Errorf("message already acknowledged")
	}

	m.acknowledged = true
	return m.delivery.Nack(true, requeue)
}

func (m *Message) Reject(requeue bool) error {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.delivery == nil {
		return fmt.Errorf("message delivery is nil - cannot reject")
	}

	if m.acknowledged {
		return fmt.Errorf("message already acknowledged")
	}

	m.acknowledged = true
	return m.delivery.Reject(requeue)
}

func (m *Message) IsAcknowledged() bool {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()
	return m.acknowledged
}

func (m *Message) GetRetryCount() int64 {
	if m.Headers == nil {
		return 0
	}

	if retryCount, ok := m.Headers["x-retry-count"]; ok {
		switch v := retryCount.(type) {
		case int:
			return int64(v)
		case int64:
			return v
		case string:
			// Try to parse string as integer
			if count := parseInt(v); count >= 0 {
				return count
			}
		}
	}

	xDeath, exists := m.Headers["x-death"].([]interface{})
	if exists {
		return xDeath[0].(amqp.Table)["count"].(int64)
	}

	return 0
}

func parseInt(s string) int64 {
	var count int64
	_, err := fmt.Sscanf(s, "%d", &count)
	if err != nil {
		return -1
	}
	return count
}
