package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"git.zooket.ir/snapp-express/pkg/rabbitmq/v2"
	"github.com/rs/zerolog"
)

// Define message types for different domains
type UserEvent struct {
	ID     int    `json:"id"`
	Action string `json:"action"`
	Name   string `json:"name"`
	Email  string `json:"email"`
}

type OrderEvent struct {
	ID       int      `json:"id"`
	UserID   int      `json:"user_id"`
	Amount   float64  `json:"amount"`
	Status   string   `json:"status"`
	Products []string `json:"products"`
}

type NotificationEvent struct {
	Type      string                 `json:"type"`
	Recipient string                 `json:"recipient"`
	Message   string                 `json:"message"`
	Data      map[string]interface{} `json:"data"`
}

type EmailEvent struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
	IsHTML  bool   `json:"is_html"`
}

// Statistics to track processed messages
type Stats struct {
	UserEvents         int64
	OrderEvents        int64
	NotificationEvents int64
	EmailEvents        int64
	ErrorEvents        int64
	mu                 sync.RWMutex
}

func (s *Stats) IncrementUser() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.UserEvents++
}

func (s *Stats) IncrementOrder() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.OrderEvents++
}

func (s *Stats) IncrementNotification() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.NotificationEvents++
}

func (s *Stats) IncrementEmail() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.EmailEvents++
}

func (s *Stats) IncrementError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ErrorEvents++
}

func (s *Stats) GetStats() (int64, int64, int64, int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.UserEvents, s.OrderEvents, s.NotificationEvents, s.EmailEvents, s.ErrorEvents
}

func main() {
	// Setup logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create RabbitMQ configuration
	config := &rabbitmq.Config{
		Host:                "localhost",
		Port:                5672,
		Username:            "admin",
		Password:            "admin123",
		MaxConnections:      10,
		MaxChannels:         100,
		ConnectionTimeout:   30 * time.Second,
		HeartbeatInterval:   60 * time.Second,
		ReconnectDelay:      5 * time.Second,
		MaxReconnectDelay:   5 * time.Minute,
		ReconnectAttempts:   10,
		EnableAutoReconnect: true,
		VHost:               "/",
	}

	// Create client
	client, err := rabbitmq.NewClient(config, logger)
	if err != nil {
		log.Fatal("Failed to create RabbitMQ client:", err)
	}
	defer client.Close()

	// Setup queues
	setupQueues(client, logger)

	// Initialize statistics
	stats := &Stats{}

	// Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("🚀 Multi-Consumer Demo Starting")
	fmt.Println("================================")
	fmt.Println("This demo shows how to register and manage multiple consumers on one client:")
	fmt.Println("1. Register multiple consumers for different queues")
	fmt.Println("2. Each consumer handles different message types")
	fmt.Println("3. All consumers run concurrently")
	fmt.Println("4. Proper resource management and cleanup")
	fmt.Println()

	// Register User Events Consumer
	fmt.Println("📝 Registering User Events Consumer")
	userConsumer := client.RegisterConsumer(createUserHandler(stats, logger), &rabbitmq.ConsumerOptions{
		Queue:         "user-events",
		ConsumerTag:   "user-processor",
		AutoAck:       false,
		Exclusive:     false,
		NoLocal:       false,
		NoWait:        false,
		PrefetchCount: 10,
		ReconnectWait: 5 * time.Second,
	})

	// Register Order Events Consumer
	fmt.Println("📝 Registering Order Events Consumer")
	orderConsumer := client.RegisterConsumer(createOrderHandler(stats, logger), &rabbitmq.ConsumerOptions{
		Queue:         "order-events",
		ConsumerTag:   "order-processor",
		AutoAck:       false,
		Exclusive:     false,
		NoLocal:       false,
		NoWait:        false,
		PrefetchCount: 5,
		ReconnectWait: 5 * time.Second,
	})

	// Register Notification Events Consumer
	fmt.Println("📝 Registering Notification Events Consumer")
	notificationConsumer := client.RegisterConsumer(createNotificationHandler(stats, logger), &rabbitmq.ConsumerOptions{
		Queue:         "notification-events",
		ConsumerTag:   "notification-processor",
		AutoAck:       false,
		Exclusive:     false,
		NoLocal:       false,
		NoWait:        false,
		PrefetchCount: 20,
		ReconnectWait: 5 * time.Second,
	})

	// Register Email Events Consumer
	fmt.Println("📝 Registering Email Events Consumer")
	emailConsumer := client.RegisterConsumer(createEmailHandler(stats, logger), &rabbitmq.ConsumerOptions{
		Queue:         "email-events",
		ConsumerTag:   "email-processor",
		AutoAck:       false,
		Exclusive:     false,
		NoLocal:       false,
		NoWait:        false,
		PrefetchCount: 3,
		ReconnectWait: 5 * time.Second,
	})

	// Register Error Events Consumer
	fmt.Println("📝 Registering Error Events Consumer")
	errorConsumer := client.RegisterConsumer(createErrorHandler(stats, logger), &rabbitmq.ConsumerOptions{
		Queue:         "error-events",
		ConsumerTag:   "error-processor",
		AutoAck:       false,
		Exclusive:     false,
		NoLocal:       false,
		NoWait:        false,
		PrefetchCount: 1,
		ReconnectWait: 5 * time.Second,
	})

	// Start all consumers concurrently
	fmt.Println("🚀 Starting all consumers...")
	go func() {
		if err := userConsumer.Consume(ctx); err != nil {
			logger.Error().Err(err).Msg("User consumer error")
		}
	}()

	go func() {
		if err := orderConsumer.Consume(ctx); err != nil {
			logger.Error().Err(err).Msg("Order consumer error")
		}
	}()

	go func() {
		if err := notificationConsumer.Consume(ctx); err != nil {
			logger.Error().Err(err).Msg("Notification consumer error")
		}
	}()

	go func() {
		if err := emailConsumer.Consume(ctx); err != nil {
			logger.Error().Err(err).Msg("Email consumer error")
		}
	}()

	go func() {
		if err := errorConsumer.Consume(ctx); err != nil {
			logger.Error().Err(err).Msg("Error consumer error")
		}
	}()

	fmt.Printf("🔍 Successfully registered and started 5 consumers\n")

	// Start statistics reporter
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				user, order, notification, email, errorCount := stats.GetStats()
				fmt.Printf("📊 Stats - Users: %d, Orders: %d, Notifications: %d, Emails: %d, Errors: %d\n",
					user, order, notification, email, errorCount)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start message publisher to generate test data
	go publishTestMessages(client, logger, ctx)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("\n✅ Multi-consumer demo running. Press Ctrl+C to exit...")
	<-sigChan

	fmt.Println("\n🛑 Shutting down...")
	cancel()

	// Show final stats
	time.Sleep(1 * time.Second)
	user, order, notification, email, errorCount := stats.GetStats()
	fmt.Printf("📈 Final Stats - Users: %d, Orders: %d, Notifications: %d, Emails: %d, Errors: %d\n",
		user, order, notification, email, errorCount)

	fmt.Println("✅ Multi-consumer demo completed!")
}

func setupQueues(client rabbitmq.Client, logger zerolog.Logger) {
	queues := []string{
		"user-events",
		"order-events",
		"notification-events",
		"email-events",
		"error-events",
	}

	for _, queue := range queues {
		err := client.DeclareQueue(queue, rabbitmq.QueueOptions{
			Durable:    true,
			AutoDelete: false,
			Exclusive:  false,
			NoWait:     false,
		})
		if err != nil {
			log.Fatal("Failed to declare queue:", queue, err)
		}
	}

	logger.Info().Msg("All queues declared successfully")
}

func createUserHandler(stats *Stats, logger zerolog.Logger) rabbitmq.MessageHandler {
	return func(ctx context.Context, msg *rabbitmq.Message) {
		var userEvent UserEvent
		if err := json.Unmarshal(msg.Body, &userEvent); err != nil {
			logger.Error().Err(err).Msg("Failed to unmarshal user event")
			msg.Nack(false)
			return
		}

		logger.Info().
			Int("user_id", userEvent.ID).
			Str("action", userEvent.Action).
			Str("name", userEvent.Name).
			Msg("Processing user event")

		// Simulate processing time
		time.Sleep(50 * time.Millisecond)

		stats.IncrementUser()
		msg.Ack()
	}
}

func createOrderHandler(stats *Stats, logger zerolog.Logger) rabbitmq.MessageHandler {
	return func(ctx context.Context, msg *rabbitmq.Message) {
		var orderEvent OrderEvent
		if err := json.Unmarshal(msg.Body, &orderEvent); err != nil {
			logger.Error().Err(err).Msg("Failed to unmarshal order event")
			msg.Nack(false)
			return
		}

		logger.Info().
			Int("order_id", orderEvent.ID).
			Float64("amount", orderEvent.Amount).
			Str("status", orderEvent.Status).
			Msg("Processing order event")

		// Simulate processing time
		time.Sleep(75 * time.Millisecond)

		stats.IncrementOrder()
		msg.Ack()
	}
}

func createNotificationHandler(stats *Stats, logger zerolog.Logger) rabbitmq.MessageHandler {
	return func(ctx context.Context, msg *rabbitmq.Message) {
		var notificationEvent NotificationEvent
		if err := json.Unmarshal(msg.Body, &notificationEvent); err != nil {
			logger.Error().Err(err).Msg("Failed to unmarshal notification event")
			msg.Nack(false)
			return
		}

		logger.Info().
			Str("type", notificationEvent.Type).
			Str("recipient", notificationEvent.Recipient).
			Msg("Processing notification event")

		// Simulate processing time
		time.Sleep(25 * time.Millisecond)

		stats.IncrementNotification()
		msg.Ack()
	}
}

func createEmailHandler(stats *Stats, logger zerolog.Logger) rabbitmq.MessageHandler {
	return func(ctx context.Context, msg *rabbitmq.Message) {
		var emailEvent EmailEvent
		if err := json.Unmarshal(msg.Body, &emailEvent); err != nil {
			logger.Error().Err(err).Msg("Failed to unmarshal email event")
			msg.Nack(false)
			return
		}

		logger.Info().
			Str("to", emailEvent.To).
			Str("subject", emailEvent.Subject).
			Bool("is_html", emailEvent.IsHTML).
			Msg("Processing email event")

		// Simulate processing time
		time.Sleep(100 * time.Millisecond)

		stats.IncrementEmail()
		msg.Ack()
	}
}

func createErrorHandler(stats *Stats, logger zerolog.Logger) rabbitmq.MessageHandler {
	return func(ctx context.Context, msg *rabbitmq.Message) {
		logger.Warn().
			Str("message_id", msg.ID).
			Str("body", string(msg.Body)).
			Msg("Processing error event")

		// Simulate processing time
		time.Sleep(200 * time.Millisecond)

		stats.IncrementError()
		msg.Ack()
	}
}

func publishTestMessages(client rabbitmq.Client, logger zerolog.Logger, ctx context.Context) {
	publisher := client.Publisher()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	counter := 0
	for {
		select {
		case <-ticker.C:
			counter++

			// Publish user events
			userEvent := UserEvent{
				ID:     counter,
				Action: "created",
				Name:   fmt.Sprintf("User-%d", counter),
				Email:  fmt.Sprintf("user%d@example.com", counter),
			}
			publishEvent(publisher, "user-events", userEvent, logger)

			// Publish order events
			orderEvent := OrderEvent{
				ID:       counter,
				UserID:   counter,
				Amount:   float64(counter * 10),
				Status:   "pending",
				Products: []string{fmt.Sprintf("Product-%d", counter)},
			}
			publishEvent(publisher, "order-events", orderEvent, logger)

			// Publish notification events
			notificationEvent := NotificationEvent{
				Type:      "order_confirmation",
				Recipient: fmt.Sprintf("user%d@example.com", counter),
				Message:   fmt.Sprintf("Your order #%d has been confirmed", counter),
				Data:      map[string]interface{}{"order_id": counter},
			}
			publishEvent(publisher, "notification-events", notificationEvent, logger)

			// Publish email events
			emailEvent := EmailEvent{
				To:      fmt.Sprintf("user%d@example.com", counter),
				Subject: fmt.Sprintf("Order Confirmation #%d", counter),
				Body:    fmt.Sprintf("Thank you for your order #%d", counter),
				IsHTML:  false,
			}
			publishEvent(publisher, "email-events", emailEvent, logger)

			// Occasionally publish error events
			if counter%5 == 0 {
				errorMsg := fmt.Sprintf("Error processing batch %d", counter/5)
				publishString(publisher, "error-events", errorMsg, logger)
			}

		case <-ctx.Done():
			return
		}
	}
}

func publishEvent(publisher rabbitmq.Publisher, queue string, event interface{}, logger zerolog.Logger) {
	data, err := json.Marshal(event)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to marshal event")
		return
	}

	msg := &rabbitmq.Message{
		Body:         data,
		ContentType:  "application/json",
		DeliveryMode: rabbitmq.DeliveryModePersistent,
	}

	err = publisher.Publish(context.Background(), "", queue, msg)
	if err != nil {
		logger.Error().Err(err).Str("queue", queue).Msg("Failed to publish event")
	}
}

func publishString(publisher rabbitmq.Publisher, queue string, content string, logger zerolog.Logger) {
	msg := &rabbitmq.Message{
		Body:         []byte(content),
		ContentType:  "text/plain",
		DeliveryMode: rabbitmq.DeliveryModePersistent,
	}

	err := publisher.Publish(context.Background(), "", queue, msg)
	if err != nil {
		logger.Error().Err(err).Str("queue", queue).Msg("Failed to publish message")
	}
}
