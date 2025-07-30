package main

import (
	"context"
	"fmt"
	"git.zooket.ir/snapp-express/pkg/rabbitmq/v2"
	"log"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func main() {
	// Create logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create config
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
		PublisherConfig: rabbitmq.PublisherOptions{
			ConfirmMode:    true,
			Mandatory:      true,
			Immediate:      false,
			RetryAttempts:  3,
			RetryDelay:     1 * time.Second,
			ConfirmTimeout: 10 * time.Second,
		},
	}

	// Create client
	client, err := rabbitmq.NewClient(config, logger)
	if err != nil {
		log.Fatal("Failed to create RabbitMQ client:", err)
	}
	defer client.Close()

	// Get publisher
	publisher := client.Publisher()
	ctx := context.Background()

	// Exchange name for retry queue (must match what retry_consumer expects)
	exchangeName := "retry-exchange"

	fmt.Println("🚀 Starting Retry Publisher Example")
	fmt.Println("=====================================")
	fmt.Println("This publisher will send different types of messages to demonstrate:")
	fmt.Println("- Successful processing")
	fmt.Println("- Retry mechanism (nack → retry queue → main queue)")
	fmt.Println("- Final failure after max retries")
	fmt.Println()

	// Test scenarios
	testScenarios := []struct {
		messageType string
		body        string
		description string
		count       int
	}{
		{
			messageType: "success",
			body:        "success",
			description: "Should be processed successfully (acknowledged)",
			count:       1,
		},
		{
			messageType: "retry",
			body:        "retry",
			description: "Should fail first 2 times, succeed on 3rd attempt",
			count:       2,
		},
		{
			messageType: "fail",
			body:        "fail",
			description: "Should fail all retry attempts and go to final DLQ",
			count:       2,
		},
	}

	// Publish test messages
	messageID := 1
	for _, scenario := range testScenarios {
		fmt.Printf("📨 Publishing %d '%s' messages (%s)...\n",
			scenario.count, scenario.messageType, scenario.description)

		for i := 0; i < scenario.count; i++ {
			msg := &rabbitmq.Message{
				ID:          fmt.Sprintf("retry-test-%d", messageID),
				Body:        []byte(scenario.body),
				ContentType: "text/plain",
				Headers: map[string]interface{}{
					"source":       "retry-publisher",
					"message_type": scenario.messageType,
					"scenario":     scenario.description,
					"attempt":      i + 1,
					"timestamp":    time.Now().Format(time.RFC3339),
				},
				Timestamp:    time.Now(),
				DeliveryMode: rabbitmq.DeliveryModePersistent, // Ensure messages persist
				Priority:     rabbitmq.PriorityNormal,
			}

			err := publisher.Publish(ctx, exchangeName, "", msg) // Empty routing key routes to main queue
			if err != nil {
				logger.Error().Err(err).Str("message_id", msg.ID).Msg("Failed to publish message")
			} else {
				logger.Info().
					Str("message_id", msg.ID).
					Str("message_type", scenario.messageType).
					Str("body", scenario.body).
					Msg("Message published successfully")
			}

			messageID++
			time.Sleep(500 * time.Millisecond) // Small delay between messages
		}

		fmt.Printf("✅ Completed publishing %s messages\n\n", scenario.messageType)
		time.Sleep(1 * time.Second) // Pause between scenarios

	}

	fmt.Println("\n🎯 ALL MESSAGES PUBLISHED!")
	fmt.Println("=====================================")
	fmt.Println("Messages have been sent to the retry-exchange.")
	fmt.Println("Start the retry_consumer to see the retry mechanism in action:")
	fmt.Println("  cd examples/retry_consumer && ./retry_consumer")
	fmt.Println()
	fmt.Println("Expected behavior:")
	fmt.Println("✅ 'success' messages: Immediate acknowledgment")
	fmt.Println("🔄 'retry' messages: Fail 2 times, succeed on 3rd (via retry queue)")
	fmt.Println("❌ 'fail' messages: Exhaust retries, go to final DLQ")
	fmt.Println()
	fmt.Println("Monitor the queues:")
	fmt.Println("- retry-queue: Main processing queue")
	fmt.Println("- retry-queue.retry: Temporary retry queue (5s TTL)")
	fmt.Println("- retry-queue.failed: Final DLQ for permanently failed messages")
	fmt.Println()
	fmt.Println("🔗 RabbitMQ Management UI: http://localhost:15672")
	fmt.Println("   Username: admin, Password: admin123")
}
