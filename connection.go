package rabbit_kit

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type connectionManager struct {
	config            *Config
	connection        *amqp.Connection
	channels          []*amqp.Channel
	connectionMutex   sync.RWMutex
	channelMutex      sync.RWMutex
	channelPool       chan *amqp.Channel
	isConnected       int32 // atomic
	isReconnecting    int32 // atomic
	shutdownCh        chan struct{}
	connectionLossCh  chan *amqp.Error
	logger            zerolog.Logger
	reconnectAttempts int
	lastReconnectTime time.Time
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
}

func NewConnectionManager(config *Config, logger zerolog.Logger) (ConnectionManager, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	cm := &connectionManager{
		config:           config,
		shutdownCh:       make(chan struct{}),
		connectionLossCh: make(chan *amqp.Error, 100),
		logger:           logger,
		channelPool:      make(chan *amqp.Channel, config.MaxChannels),
		ctx:              ctx,
		cancel:           cancel,
	}

	if err := cm.connect(); err != nil {
		cancel()
		return nil, NewConnectionError("initial connection", err)
	}

	if config.EnableAutoReconnect {
		cm.wg.Add(1)
		go cm.reconnectLoop()
	}

	if config.HealthCheckInterval > 0 {
		cm.wg.Add(1)
		go cm.healthCheckLoop()
	}

	return cm, nil
}

func (cm *connectionManager) GetConnection() (*amqp.Connection, error) {
	cm.connectionMutex.RLock()
	defer cm.connectionMutex.RUnlock()

	if cm.connection == nil || cm.connection.IsClosed() {
		return nil, ErrConnectionLost
	}

	return cm.connection, nil
}

func (cm *connectionManager) GetChannel() (*amqp.Channel, error) {
	// Try to get from pool first
	select {
	case ch := <-cm.channelPool:
		if ch != nil && !ch.IsClosed() {
			return ch, nil
		}
	default:
	}

	// Create new channel
	conn, err := cm.GetConnection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, NewConnectionError("create channel", err)
	}

	cm.channelMutex.Lock()
	cm.channels = append(cm.channels, ch)
	cm.channelMutex.Unlock()

	return ch, nil
}

func (cm *connectionManager) ReturnChannel(ch *amqp.Channel) {
	if ch == nil || ch.IsClosed() {
		return
	}

	select {
	case cm.channelPool <- ch:
	default:
		ch.Close()
	}
}

func (cm *connectionManager) Close() error {
	cm.logger.Info().Msg("Closing RabbitMQ connection manager...")

	close(cm.shutdownCh)
	cm.cancel()

	cm.wg.Wait()

	// Close all channels
	cm.channelMutex.Lock()
	for _, ch := range cm.channels {
		if ch != nil && !ch.IsClosed() {
			ch.Close()
		}
	}
	cm.channels = nil
	cm.channelMutex.Unlock()

	// Close channel pool
	close(cm.channelPool)
	for ch := range cm.channelPool {
		if ch != nil && !ch.IsClosed() {
			ch.Close()
		}
	}

	// Close connection
	cm.connectionMutex.Lock()
	defer cm.connectionMutex.Unlock()

	if cm.connection != nil && !cm.connection.IsClosed() {
		return cm.connection.Close()
	}

	return nil
}

func (cm *connectionManager) IsConnected() bool {
	return atomic.LoadInt32(&cm.isConnected) == 1
}

func (cm *connectionManager) NotifyConnectionLoss() <-chan *amqp.Error {
	return cm.connectionLossCh
}

func (cm *connectionManager) connect() error {
	cm.logger.Info().Msg("Connecting to RabbitMQ")

	config := amqp.Config{
		Heartbeat: cm.config.HeartbeatInterval,
		Locale:    "en_US",
	}

	if cm.config.ConnectionTimeout > 0 {
		config.Dial = amqp.DefaultDial(cm.config.ConnectionTimeout)
	}

	conn, err := amqp.DialConfig(cm.config.BuildConnectionString(), config)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	cm.connectionMutex.Lock()
	cm.connection = conn
	cm.connectionMutex.Unlock()

	atomic.StoreInt32(&cm.isConnected, 1)
	cm.reconnectAttempts = 0

	// Setup connection close notification
	go cm.handleConnectionClose(conn.NotifyClose(make(chan *amqp.Error)))

	cm.logger.Info().Msg("Connected to RabbitMQ")
	return nil
}

func (cm *connectionManager) handleConnectionClose(closeCh <-chan *amqp.Error) {
	select {
	case err := <-closeCh:
		if err != nil {
			cm.logger.Error().Err(err).Msg("Connection lost")
			atomic.StoreInt32(&cm.isConnected, 0)

			select {
			case cm.connectionLossCh <- err:
			default:
				cm.logger.Error().Err(err).Msg("Connection channel full, dropping notification")
			}

			// Close all channels
			cm.channelMutex.Lock()
			for _, ch := range cm.channels {
				if ch != nil && !ch.IsClosed() {
					ch.Close()
				}
			}
			cm.channels = nil
			cm.channelMutex.Unlock()
		}
	case <-cm.shutdownCh:
		return
	}
}

func (cm *connectionManager) reconnectLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.config.ReconnectDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !cm.IsConnected() && atomic.CompareAndSwapInt32(&cm.isReconnecting, 0, 1) {
				cm.attemptReconnect()
				atomic.StoreInt32(&cm.isReconnecting, 0)
			}
		case <-cm.shutdownCh:
			return
		}
	}
}

func (cm *connectionManager) attemptReconnect() {
	if cm.config.ReconnectAttempts > 0 && cm.reconnectAttempts >= cm.config.ReconnectAttempts {
		cm.logger.Error().Msgf("Max reconnect attempts reached: %d", cm.config.ReconnectAttempts)
		return
	}

	delay := cm.config.ReconnectDelay
	if cm.reconnectAttempts > 0 {
		backoff := time.Duration(cm.reconnectAttempts) * cm.config.ReconnectDelay
		if backoff > cm.config.MaxReconnectDelay {
			delay = cm.config.MaxReconnectDelay
		} else {
			delay = backoff
		}
	}

	if time.Since(cm.lastReconnectTime) < delay {
		time.Sleep(delay - time.Since(cm.lastReconnectTime))
	}

	cm.reconnectAttempts++
	cm.lastReconnectTime = time.Now()

	cm.logger.Info().Msgf("Attempting to reconnect (attempt %d, delay %s)", cm.reconnectAttempts, delay)

	if err := cm.connect(); err != nil {
		//cm.logger.WithError(err).WithField("attempt", cm.reconnectAttempts).Error("Reconnection failed")
		cm.logger.Error().Err(err).Msgf("Reconnection failed (attempt %d)", cm.reconnectAttempts)
	} else {
		cm.logger.Info().Msgf("Reconnected successfully (attempt %d)", cm.reconnectAttempts)
	}
}

func (cm *connectionManager) healthCheckLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := cm.healthCheck(); err != nil {
				cm.logger.Error().Err(err).Msg("Health check failed")
				atomic.StoreInt32(&cm.isConnected, 0)
			}
		case <-cm.shutdownCh:
			return
		}
	}
}

func (cm *connectionManager) healthCheck() error {
	conn, err := cm.GetConnection()
	if err != nil {
		return err
	}

	if conn.IsClosed() {
		return ErrConnectionLost
	}

	// Try to create and close a channel to verify connection health
	ch, err := conn.Channel()
	if err != nil {
		return NewConnectionError("health check channel creation", err)
	}
	defer ch.Close()

	return nil
}
