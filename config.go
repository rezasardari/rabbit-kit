package rabbit_kit

import (
	"fmt"
	"net/url"
	"time"
)

type Config struct {
	// Connection settings
	URL      string `json:"url"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	VHost    string `json:"vhost"`
	UseTLS   bool   `json:"use_tls"`

	// Connection pool settings
	MaxConnections    int           `json:"max_connections"`
	MaxChannels       int           `json:"max_channels"`
	ConnectionTimeout time.Duration `json:"connection_timeout"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	// Reconnection settings
	ReconnectDelay      time.Duration `json:"reconnect_delay"`
	MaxReconnectDelay   time.Duration `json:"max_reconnect_delay"`
	ReconnectAttempts   int           `json:"reconnect_attempts"`
	EnableAutoReconnect bool          `json:"enable_auto_reconnect"`

	// Publisher settings
	PublisherConfig PublisherOptions `json:"publisher_config"`

	// Health check settings
	HealthCheckInterval time.Duration `json:"health_check_interval"`
}

func DefaultConfig() *Config {
	return &Config{
		Host:                "localhost",
		Port:                5672,
		Username:            "guest",
		Password:            "guest",
		VHost:               "/",
		UseTLS:              false,
		MaxConnections:      10,
		MaxChannels:         100,
		ConnectionTimeout:   30 * time.Second,
		HeartbeatInterval:   60 * time.Second,
		ReconnectDelay:      5 * time.Second,
		MaxReconnectDelay:   5 * time.Minute,
		ReconnectAttempts:   10,
		EnableAutoReconnect: true,
		PublisherConfig: PublisherOptions{
			ConfirmMode:    true,
			Mandatory:      false,
			Immediate:      false,
			RetryAttempts:  3,
			RetryDelay:     1 * time.Second,
			ConfirmTimeout: 10 * time.Second,
		},
		HealthCheckInterval: 30 * time.Second,
	}
}

func (c *Config) BuildConnectionString() string {
	if c.URL != "" {
		return c.URL
	}

	scheme := "amqp"
	if c.UseTLS {
		scheme = "amqps"
	}

	// Build URL
	u := &url.URL{
		Scheme: scheme,
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.VHost,
	}

	if c.Username != "" && c.Password != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}

	return u.String()
}

func (c *Config) Validate() error {
	if c.URL == "" {
		if c.Host == "" {
			return NewConfigurationError("host", c.Host, "host cannot be empty when URL is not provided")
		}
		if c.Port <= 0 || c.Port > 65535 {
			return NewConfigurationError("port", c.Port, "port must be between 1 and 65535")
		}
	} else {
		if _, err := url.Parse(c.URL); err != nil {
			return NewConfigurationError("url", c.URL, fmt.Sprintf("invalid URL format: %v", err))
		}
	}

	if c.MaxConnections <= 0 {
		return NewConfigurationError("max_connections", c.MaxConnections, "max_connections must be greater than 0")
	}

	if c.MaxChannels <= 0 {
		return NewConfigurationError("max_channels", c.MaxChannels, "max_channels must be greater than 0")
	}

	if c.ConnectionTimeout <= 0 {
		return NewConfigurationError("connection_timeout", c.ConnectionTimeout, "connection_timeout must be greater than 0")
	}

	if c.HeartbeatInterval < 0 {
		return NewConfigurationError("heartbeat_interval", c.HeartbeatInterval, "heartbeat_interval cannot be negative")
	}

	if c.ReconnectDelay <= 0 {
		return NewConfigurationError("reconnect_delay", c.ReconnectDelay, "reconnect_delay must be greater than 0")
	}

	if c.MaxReconnectDelay < c.ReconnectDelay {
		return NewConfigurationError("max_reconnect_delay", c.MaxReconnectDelay, "max_reconnect_delay must be greater than or equal to reconnect_delay")
	}

	if c.ReconnectAttempts < 0 {
		return NewConfigurationError("reconnect_attempts", c.ReconnectAttempts, "reconnect_attempts cannot be negative")
	}

	if c.HealthCheckInterval < 0 {
		return NewConfigurationError("health_check_interval", c.HealthCheckInterval, "health_check_interval cannot be negative")
	}

	return nil
}

func (c *Config) validatePublisherConfig() error {
	if c.PublisherConfig.RetryAttempts < 0 {
		return NewConfigurationError("publisher.retry_attempts", c.PublisherConfig.RetryAttempts, "retry_attempts cannot be negative")
	}

	if c.PublisherConfig.RetryDelay < 0 {
		return NewConfigurationError("publisher.retry_delay", c.PublisherConfig.RetryDelay, "retry_delay cannot be negative")
	}

	if c.PublisherConfig.ConfirmTimeout <= 0 {
		return NewConfigurationError("publisher.confirm_timeout", c.PublisherConfig.ConfirmTimeout, "confirm_timeout must be greater than 0")
	}

	return nil
}

func (c *Config) ApplyDefaults() {
	defaults := DefaultConfig()

	if c.Host == "" && c.URL == "" {
		c.Host = defaults.Host
	}
	if c.Port == 0 {
		c.Port = defaults.Port
	}
	if c.Username == "" {
		c.Username = defaults.Username
	}
	if c.Password == "" {
		c.Password = defaults.Password
	}
	if c.VHost == "" {
		c.VHost = defaults.VHost
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = defaults.MaxConnections
	}
	if c.MaxChannels == 0 {
		c.MaxChannels = defaults.MaxChannels
	}
	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = defaults.ConnectionTimeout
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if c.ReconnectDelay == 0 {
		c.ReconnectDelay = defaults.ReconnectDelay
	}
	if c.MaxReconnectDelay == 0 {
		c.MaxReconnectDelay = defaults.MaxReconnectDelay
	}
	if c.ReconnectAttempts == 0 {
		c.ReconnectAttempts = defaults.ReconnectAttempts
	}

	if c.HealthCheckInterval == 0 {
		c.HealthCheckInterval = defaults.HealthCheckInterval
	}

	// Apply publisher defaults
	if c.PublisherConfig.RetryAttempts == 0 {
		c.PublisherConfig.RetryAttempts = defaults.PublisherConfig.RetryAttempts
	}
	if c.PublisherConfig.RetryDelay == 0 {
		c.PublisherConfig.RetryDelay = defaults.PublisherConfig.RetryDelay
	}
	if c.PublisherConfig.ConfirmTimeout == 0 {
		c.PublisherConfig.ConfirmTimeout = defaults.PublisherConfig.ConfirmTimeout
	}

}

func (c *Config) Clone() *Config {
	clone := *c

	// Deep copy publisher config
	clone.PublisherConfig = c.PublisherConfig
	if c.PublisherConfig.Args != nil {
		clone.PublisherConfig.Args = make(map[string]interface{})
		for k, v := range c.PublisherConfig.Args {
			clone.PublisherConfig.Args[k] = v
		}
	}

	return &clone
}
