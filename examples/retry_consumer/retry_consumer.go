package main

import (
	"context"
	"git.zooket.ir/snapp-express/pkg/rabbitmq/v2"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

func main() {
	// Create logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

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

	client, err := rabbitmq.NewClient(config, logger)
	if err != nil {
		log.Fatal("Failed to create RabbitMQ client:", err)
	}
	defer client.Close()

	queueName := "retry-queue"
	xName := "retry-exchange"
	dlxName := queueName + ".dlx"
	retryQueueName := queueName + ".retry"

	err = client.DeclareExchange(xName, rabbitmq.ExchangeOptions{
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
	})
	if err != nil {
		log.Fatal("Failed to declare main exchange:", err)
	}

	err = client.DeclareExchange(dlxName, rabbitmq.ExchangeOptions{
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
	})
	if err != nil {
		log.Fatal("Failed to declare DLX:", err)
	}

	err = client.DeclareQueue(queueName, rabbitmq.QueueOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: amqp.Table{
			"x-dead-letter-exchange":    dlxName,
			"x-dead-letter-routing-key": "retry", // Route failed messages to retry queue
		},
	})
	if err != nil {
		log.Fatal("Failed to declare main queue:", err)
	}

	err = client.DeclareQueue(retryQueueName, rabbitmq.QueueOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: amqp.Table{
			"x-message-ttl":             5000,  // 5 seconds TTL
			"x-dead-letter-exchange":    xName, // Return to main exchange
			"x-dead-letter-routing-key": "",    // Route back to main queue with empty key
		},
	})
	if err != nil {
		log.Fatal("Failed to declare retry queue:", err)
	}

	err = client.BindQueue(queueName, xName, "")
	if err != nil {
		log.Fatal("Failed to bind main queue to exchange:", err)
	}

	err = client.BindQueue(retryQueueName, dlxName, "retry")
	if err != nil {
		log.Fatal("Failed to bind retry queue:", err)
	}

	messageHandler := func(ctx context.Context, msg *rabbitmq.Message) {
		retryCount := msg.GetRetryCount()

		logger.Info().
			Str("message_id", msg.ID).
			Str("body", string(msg.Body)).
			Int64("retry_count", retryCount).
			Msg("Processing message")

		// Simulate processing
		time.Sleep(1 * time.Millisecond)

		body := string(msg.Body)

		// Simulate failure for messages containing "fail"
		if body == "fail" {
			logger.Warn().Str("message_id", msg.ID).Msg("Simulating processing failure")

			// Check if we've exceeded max retries (3)
			if retryCount >= 3 {
				logger.Error().Str("message_id", msg.ID).
					Int64("retry_count", retryCount).
					Msg("Max retries exceeded, rejecting to final DLQ")
				if err := msg.Ack(); err != nil {
					logger.Error().Err(err).Msg("Failed to reject message")
				}
			} else {
				logger.Info().Str("message_id", msg.ID).
					Int64("retry_count", retryCount).
					Msg("Sending to retry queue")
				if err := msg.Nack(false); err != nil { // Don't requeue directly, let DLX handle it
					logger.Error().Err(err).Msg("Failed to nack message")
				}
			}
			return
		}

		// Simulate intermittent failure for messages containing "retry"
		if body == "retry" {
			// Fail first 2 times, succeed on 3rd+ attempt
			if retryCount < 2 {
				logger.Warn().
					Str("message_id", msg.ID).
					Int64("retry_count", retryCount).
					Msg("Simulating intermittent failure")

				if err := msg.Nack(false); err != nil { // Let DLX handle retry routing
					logger.Error().Err(err).Msg("Failed to nack message for retry")
				}
				return
			}
		}

		// Success case
		logger.Info().
			Str("message_id", msg.ID).
			Int64("retry_count", retryCount).
			Msg("Message processed successfully")

		if err := msg.Ack(); err != nil {
			logger.Error().Err(err).Msg("Failed to ack message")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		logger.Info().Msg("Starting retry consumer...")
		consumer := client.RegisterConsumer(messageHandler, &rabbitmq.ConsumerOptions{
			Queue:         queueName,
			ConsumerTag:   "retry-consumer",
			AutoAck:       false,
			Exclusive:     false,
			NoLocal:       false,
			NoWait:        false,
			PrefetchCount: 10,
			PrefetchSize:  0,
			Args:          nil,
			ReconnectWait: 5 * time.Second,
		})

		consumer.Consume(ctx)
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	cancel()
	time.Sleep(1 * time.Second)
	logger.Info().Msg("Consumer shutdown complete")
}
