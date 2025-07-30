# Retry Publisher Example

This example demonstrates a complete **retry mechanism** using RabbitMQ with Dead Letter Exchanges (DLX) and your simplified ACK/NACK API.

## 🏗️ Queue Architecture

The retry system uses the following queue structure:

```
retry-exchange (Direct Exchange)
    ↓ (routing key: "")
retry-queue (Main Processing Queue)
    ↓ (on NACK/failure)
retry-queue.dlx (Dead Letter Exchange)
    ↓ (routing key: "retry")
retry-queue.retry (Retry Queue with 5s TTL)
    ↓ (after TTL expires)
retry-exchange → retry-queue (back to main queue)
    ↓ (after max retries exceeded)
retry-queue.failed (Final DLQ - permanent failures)
```

## 🔄 Message Flow

1. **Publisher** sends messages to `retry-exchange` → `retry-queue`
2. **Consumer** processes messages from `retry-queue`
3. On **success**: `msg.Ack()` - message removed from queue
4. On **failure**: `msg.Nack(false)` - message goes to `retry-queue.dlx` → `retry-queue.retry`
5. After **TTL (5s)**: message returns to `retry-queue` for retry
6. After **max retries**: message goes to `retry-queue.failed` (permanent failure)

## 📋 Message Types

The publisher sends different message types to demonstrate various scenarios:

| Message Body | Behavior | Expected Result |
|--------------|----------|-----------------|
| `"success"` | Processes successfully | Immediate `Ack()` |
| `"retry"` | Fails 2 times, succeeds on 3rd | `Nack()` → retry → `Ack()` |
| `"fail"` | Always fails | `Nack()` → retry → final DLQ |

## 🚀 Running the Example

### Prerequisites

1. **RabbitMQ running** with docker-compose:
   ```bash
   cd ../../
   docker-compose up -d
   ```

2. **Build the examples**:
   ```bash
   # Build retry consumer
   cd ../retry_consumer
   go build -o retry_consumer retry_consumer.go
   
   # Build retry publisher  
   cd ../retry_publisher
   go build -o retry_publisher retry_publisher.go
   ```

### Option 1: Manual Step-by-Step

1. **Start the retry consumer**:
   ```bash
   cd ../retry_consumer
   ./retry_consumer
   ```

2. **In another terminal, run the publisher**:
   ```bash
   cd ../retry_publisher
   ./retry_publisher
   ```

3. **Monitor the queues**:
   - RabbitMQ Management UI: http://localhost:15672 (admin/admin123)
   - Watch consumer logs for ACK/NACK behavior

### Option 2: Automated Demo Script

Run the complete demonstration:

```bash
cd ../retry_consumer
chmod +x ../../demo_retry_flow.sh
../../demo_retry_flow.sh
```

This script will:
- Clean up existing queues/processes
- Start consumer and publisher automatically  
- Monitor queue states in real-time
- Show comprehensive results and logs

## 📊 Expected Results

After running the publisher, you should see:

### Consumer Logs
```
✅ "success" messages: Immediate acknowledgment
🔄 "retry" messages: Fail → retry queue → success on 3rd attempt  
❌ "fail" messages: Fail → retry queue → final DLQ after max retries
```

### Queue States
```
retry-queue: 0 messages (actively processed)
retry-queue.retry: X messages (temporary, with 5s TTL)
retry-queue.failed: Y messages (permanent failures)
```

## 🔍 Key Features Demonstrated

### 1. **Simplified ACK/NACK API**
```go
func messageHandler(ctx context.Context, msg *rabbitmq.Message) {
    if processSuccessfully(msg) {
        msg.Ack()  // ✅ Success
    } else if shouldRetry(msg) {
        msg.Nack(false)  // 🔄 Retry (don't requeue directly)
    } else {
        msg.Reject(false)  // ❌ Permanent failure
    }
    // No explicit call = auto-ack
}
```

### 2. **Retry Count Tracking**
```go
retryCount := msg.GetRetryCount()
if retryCount >= maxRetries {
    msg.Reject(false)  // Give up
} else {
    msg.Nack(false)   // Try again
}
```

### 3. **Message Persistence**
All messages are published with `DeliveryModePersistent` to survive broker restarts.

### 4. **Rich Metadata**
Messages include headers with:
- Source information
- Test scenario details
- Timestamps
- Expected behavior

## 🛠️ Customization

### Modify Retry Behavior

Edit `retry_consumer.go` to change retry logic:

```go
// Current: fail 2 times, succeed on 3rd
if retryCount < 2 {
    msg.Nack(false)  // Retry
} else {
    msg.Ack()       // Success
}
```

### Change TTL/Max Retries

Edit queue declarations in `retry_consumer.go`:

```go
Args: amqp.Table{
    "x-message-ttl": 10000,  // 10 seconds instead of 5
    // ... other args
}
```

### Add More Message Types

Edit `retry_publisher.go` to add new test scenarios:

```go
{
    messageType: "custom",
    body:        "custom",
    description: "Your custom behavior",
    count:       5,
}
```

## 📈 Monitoring

### Queue Inspection
```bash
# List all retry queues
curl -s -u admin:admin123 http://localhost:15672/api/queues | \
  jq '.[] | select(.name | startswith("retry-"))'

# Check message counts
curl -s -u admin:admin123 http://localhost:15672/api/queues | \
  jq -r '.[] | select(.name | startswith("retry-")) | "\(.name): \(.messages) messages"'
```

### Log Analysis
```bash
# Consumer activity
tail -f ../retry_consumer/consumer.log

# Queue monitoring  
tail -f ../retry_consumer/monitor.log
```

## 🎯 Benefits of This Approach

1. **Simple API**: Direct methods on Message object
2. **Explicit Control**: Client decides exactly how to handle each message
3. **Automatic Fallback**: Auto-ack if no explicit action taken
4. **Retry Visibility**: Built-in retry count tracking
5. **Production Ready**: Persistent messages, proper DLX setup
6. **Flexible**: Easy to customize retry logic per message type

This demonstrates a production-ready retry mechanism with your simplified ACK/NACK API! 🚀 