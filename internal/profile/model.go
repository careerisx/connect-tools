// Package profile defines persistent, transport-independent connection
// profiles.
package profile

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"connect-tools/internal/secrets"
)

const (
	// MaxFrameBytes is the largest fixed frame or delimiter accepted by a
	// profile. It matches the application's single-payload safety limit.
	MaxFrameBytes = 16 << 20
	// MaxDelimiterBytes bounds delimiter matching state.
	MaxDelimiterBytes = 64 << 10
	// MaxTCPServerConnections bounds one server profile's peer capacity.
	MaxTCPServerConnections = 10_000
	maxDurationMilliseconds = 1<<32 - 1
)

var (
	// ErrInvalidProfile indicates that a profile failed validation.
	ErrInvalidProfile = errors.New("invalid profile")
	// ErrInvalidID indicates that a profile ID is empty.
	ErrInvalidID = errors.New("invalid profile ID")
	// ErrInvalidName indicates that a profile name is empty.
	ErrInvalidName = errors.New("invalid profile name")
	// ErrInvalidType indicates an unknown or not-yet-supported profile type.
	ErrInvalidType = errors.New("invalid profile type")
	// ErrInvalidConfiguration indicates a missing or mismatched transport configuration.
	ErrInvalidConfiguration = errors.New("invalid profile configuration")
	// ErrInvalidHost indicates an empty TCP host.
	ErrInvalidHost = errors.New("invalid host")
	// ErrInvalidPort indicates a port outside 1..65535.
	ErrInvalidPort = errors.New("invalid port")
	// ErrInvalidTimeout indicates a timeout outside the protobuf uint32 range.
	ErrInvalidTimeout = errors.New("invalid timeout")
	// ErrInvalidKeepAlive indicates a keep-alive period outside the protobuf uint32 range.
	ErrInvalidKeepAlive = errors.New("invalid keep-alive period")
	// ErrInvalidMaxConnections indicates an unsafe server peer limit.
	ErrInvalidMaxConnections = errors.New("invalid maximum connections")
	// ErrInvalidFraming indicates inconsistent framing fields or an unknown mode.
	ErrInvalidFraming = errors.New("invalid framing configuration")
	// ErrFrameTooLarge indicates framing state above its safety limit.
	ErrFrameTooLarge = errors.New("frame exceeds maximum size")
	// ErrInvalidMQTTVersion indicates a version other than MQTT 3.1.1 or 5.0.
	ErrInvalidMQTTVersion = errors.New("invalid MQTT version")
	// ErrInvalidClientID indicates an invalid MQTT UTF-8 client identifier.
	ErrInvalidClientID = errors.New("invalid MQTT client ID")
	// ErrInvalidTopic indicates an invalid publish or Will topic.
	ErrInvalidTopic = errors.New("invalid MQTT topic")
	// ErrInvalidTopicFilter indicates a malformed MQTT subscription filter.
	ErrInvalidTopicFilter = errors.New("invalid MQTT topic filter")
	// ErrInvalidQoS indicates a QoS outside 0, 1, or 2.
	ErrInvalidQoS = errors.New("invalid MQTT QoS")
	// ErrInvalidMQTTTLS indicates an inconsistent MQTT TLS configuration.
	ErrInvalidMQTTTLS = errors.New("invalid MQTT TLS configuration")
	// ErrInvalidMQTTProperties indicates properties incompatible with the selected MQTT version.
	ErrInvalidMQTTProperties = errors.New("invalid MQTT properties")
	// ErrInvalidReconnectPolicy indicates inconsistent reconnect settings.
	ErrInvalidReconnectPolicy = errors.New("invalid MQTT reconnect policy")
)

// Type identifies the transport represented by a profile.
type Type string

const (
	// TypeTCPClient identifies a TCP client profile.
	TypeTCPClient Type = "tcp-client"
	// TypeTCPServer identifies a TCP server profile.
	TypeTCPServer Type = "tcp-server"
	// TypeMQTTClient identifies an MQTT client profile.
	TypeMQTTClient Type = "mqtt-client"
)

// MQTTVersion selects the MQTT wire protocol revision.
type MQTTVersion string

const (
	// MQTTVersion311 selects MQTT 3.1.1.
	MQTTVersion311 MQTTVersion = "3.1.1"
	// MQTTVersion5 selects MQTT 5.0.
	MQTTVersion5 MQTTVersion = "5.0"
)

// MQTTQoS is the MQTT quality-of-service level.
type MQTTQoS uint8

const (
	// MQTTQoSAtMostOnce selects QoS 0.
	MQTTQoSAtMostOnce MQTTQoS = iota
	// MQTTQoSAtLeastOnce selects QoS 1.
	MQTTQoSAtLeastOnce
	// MQTTQoSExactlyOnce selects QoS 2.
	MQTTQoSExactlyOnce
)

// MQTTPayloadFormat describes the optional MQTT 5 payload-format indicator.
type MQTTPayloadFormat string

const (
	// MQTTPayloadFormatBytes indicates unspecified binary bytes.
	MQTTPayloadFormatBytes MQTTPayloadFormat = "bytes"
	// MQTTPayloadFormatUTF8 indicates UTF-8 application data.
	MQTTPayloadFormatUTF8 MQTTPayloadFormat = "utf8"
)

// MQTTRetainHandling controls delivery of retained messages for MQTT 5 subscriptions.
type MQTTRetainHandling string

const (
	// MQTTRetainSendAtSubscribe sends retained messages whenever subscribing.
	MQTTRetainSendAtSubscribe MQTTRetainHandling = "send-at-subscribe"
	// MQTTRetainSendIfNew sends retained messages only for a new subscription.
	MQTTRetainSendIfNew MQTTRetainHandling = "send-if-new"
	// MQTTRetainDoNotSend suppresses retained messages when subscribing.
	MQTTRetainDoNotSend MQTTRetainHandling = "do-not-send"
)

// MQTTUserProperty is one ordered MQTT 5 user-property pair. A slice is used
// because the wire protocol permits duplicate names.
type MQTTUserProperty struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// MQTTMessageProperties contains MQTT 5 properties shared by Will and publish messages.
type MQTTMessageProperties struct {
	ResponseTopic                *string            `json:"response_topic,omitempty"`
	CorrelationData              []byte             `json:"correlation_data,omitempty"`
	ContentType                  *string            `json:"content_type,omitempty"`
	PayloadFormat                *MQTTPayloadFormat `json:"payload_format,omitempty"`
	MessageExpiryIntervalSeconds *uint32            `json:"message_expiry_interval_seconds,omitempty"`
	UserProperties               []MQTTUserProperty `json:"user_properties,omitempty"`
}

// MQTTWillProperties contains MQTT 5 properties specific to a Will message.
type MQTTWillProperties struct {
	MQTTMessageProperties
	WillDelayIntervalSeconds *uint32 `json:"will_delay_interval_seconds,omitempty"`
}

// MQTTTLSConfig contains verified TLS and mutual-TLS inputs. Credential fields
// contain only managed references, never plaintext passphrases.
type MQTTTLSConfig struct {
	Enabled                 bool   `json:"enabled"`
	CAFile                  string `json:"ca_file,omitempty"`
	ClientCertFile          string `json:"client_cert_file,omitempty"`
	ClientKeyFile           string `json:"client_key_file,omitempty"`
	ServerName              string `json:"server_name,omitempty"`
	PrivateKeyPassphraseRef string `json:"private_key_passphrase_ref,omitempty"`
}

// MQTTWill describes the message published by the broker after an ungraceful disconnect.
type MQTTWill struct {
	Topic      string              `json:"topic"`
	Payload    []byte              `json:"payload,omitempty"`
	QoS        MQTTQoS             `json:"qos"`
	Retain     bool                `json:"retain,omitempty"`
	Properties *MQTTWillProperties `json:"properties,omitempty"`
}

// MQTTSubscription describes one desired initial subscription.
type MQTTSubscription struct {
	Filter            string             `json:"filter"`
	QoS               MQTTQoS            `json:"qos"`
	NoLocal           bool               `json:"no_local,omitempty"`
	RetainAsPublished bool               `json:"retain_as_published,omitempty"`
	RetainHandling    MQTTRetainHandling `json:"retain_handling,omitempty"`
}

// MQTTSubscriptionBatch preserves one SUBSCRIBE packet's filters and MQTT 5
// packet-scoped properties for deterministic reconnect replay.
type MQTTSubscriptionBatch struct {
	Subscriptions          []MQTTSubscription `json:"subscriptions"`
	SubscriptionIdentifier *uint32            `json:"subscription_identifier,omitempty"`
	UserProperties         []MQTTUserProperty `json:"user_properties,omitempty"`
}

// ReconnectPolicy controls bounded exponential reconnect attempts. Zero delay
// and multiplier fields select runtime defaults; zero MaxAttempts is unlimited.
type ReconnectPolicy struct {
	Enabled        bool    `json:"enabled"`
	InitialDelayMS uint32  `json:"initial_delay_ms,omitempty"`
	MaxDelayMS     uint32  `json:"max_delay_ms,omitempty"`
	Multiplier     float64 `json:"multiplier,omitempty"`
	JitterRatio    float64 `json:"jitter_ratio,omitempty"`
	MaxAttempts    uint32  `json:"max_attempts,omitempty"`
}

// MQTTClient contains persistent MQTT settings. Password and private-key
// passphrase fields are opaque references into the secret store.
type MQTTClient struct {
	Version              MQTTVersion             `json:"version"`
	Host                 string                  `json:"host"`
	Port                 int                     `json:"port"`
	ClientID             string                  `json:"client_id,omitempty"`
	Username             string                  `json:"username,omitempty"`
	PasswordRef          string                  `json:"password_ref,omitempty"`
	KeepAliveSeconds     uint32                  `json:"keep_alive_seconds,omitempty"`
	CleanStart           bool                    `json:"clean_start"`
	SessionExpirySeconds *uint32                 `json:"session_expiry_seconds,omitempty"`
	TLS                  MQTTTLSConfig           `json:"tls"`
	Will                 *MQTTWill               `json:"will,omitempty"`
	InitialSubscriptions []MQTTSubscriptionBatch `json:"initial_subscriptions,omitempty"`
	Reconnect            ReconnectPolicy         `json:"reconnect"`
}

// FrameMode determines how received TCP bytes become debug records.
type FrameMode string

const (
	// FrameModeRawRead records each network read as a receive batch.
	FrameModeRawRead FrameMode = "raw-read"
	// FrameModeFixedLength emits equal-sized frames.
	FrameModeFixedLength FrameMode = "fixed-length"
	// FrameModeDelimiter emits frames separated by a byte sequence.
	FrameModeDelimiter FrameMode = "delimiter"
)

// FramingConfig describes TCP receive framing. Delimiter is copied whenever a
// profile crosses a package boundary.
type FramingConfig struct {
	Mode             FrameMode `json:"mode"`
	FixedLength      int       `json:"fixed_length,omitempty"`
	Delimiter        []byte    `json:"delimiter,omitempty"`
	IncludeDelimiter bool      `json:"include_delimiter,omitempty"`
}

// TCPClient contains persistent TCP client settings. Durations are stored in
// milliseconds to match the protobuf contract and avoid ambiguous JSON units.
type TCPClient struct {
	Host              string        `json:"host"`
	Port              int           `json:"port"`
	Framing           FramingConfig `json:"framing"`
	AutoReconnect     bool          `json:"auto_reconnect,omitempty"`
	ConnectTimeoutMS  int64         `json:"connect_timeout_ms,omitempty"`
	ReadTimeoutMS     int64         `json:"read_timeout_ms,omitempty"`
	WriteTimeoutMS    int64         `json:"write_timeout_ms,omitempty"`
	KeepAlive         bool          `json:"keep_alive,omitempty"`
	KeepAlivePeriodMS int64         `json:"keep_alive_period_ms,omitempty"`
	NoDelay           bool          `json:"no_delay,omitempty"`
}

// TCPServer contains persistent TCP server settings.
type TCPServer struct {
	BindHost       string        `json:"bind_host"`
	Port           int           `json:"port"`
	MaxConnections int           `json:"max_connections"`
	Framing        FramingConfig `json:"framing"`
	KeepAlive      bool          `json:"keep_alive,omitempty"`
	NoDelay        bool          `json:"no_delay,omitempty"`
}

// Profile is a saved connection definition. Exactly one configuration must be
// present and it must match Type.
type Profile struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Type        Type        `json:"type"`
	TCPClient   *TCPClient  `json:"tcp_client,omitempty"`
	TCPServer   *TCPServer  `json:"tcp_server,omitempty"`
	MQTT        *MQTTClient `json:"mqtt,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

// Validate verifies all profile identity, transport, timeout and framing
// constraints.
func (p Profile) Validate() error {
	if strings.TrimSpace(p.ID) == "" {
		return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidID)
	}
	if strings.TrimSpace(p.Name) == "" {
		return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidName)
	}

	switch p.Type {
	case TypeTCPClient:
		if p.TCPClient == nil || p.TCPServer != nil || p.MQTT != nil {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidConfiguration)
		}
		if strings.TrimSpace(p.TCPClient.Host) == "" {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidHost)
		}
		if err := validatePort(p.TCPClient.Port); err != nil {
			return err
		}
		if !validDurationMilliseconds(p.TCPClient.ConnectTimeoutMS) ||
			!validDurationMilliseconds(p.TCPClient.ReadTimeoutMS) ||
			!validDurationMilliseconds(p.TCPClient.WriteTimeoutMS) {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidTimeout)
		}
		if !validDurationMilliseconds(p.TCPClient.KeepAlivePeriodMS) {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidKeepAlive)
		}
		if err := p.TCPClient.Framing.Validate(); err != nil {
			return err
		}
	case TypeTCPServer:
		if p.TCPServer == nil || p.TCPClient != nil || p.MQTT != nil {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidConfiguration)
		}
		if strings.TrimSpace(p.TCPServer.BindHost) == "" {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidHost)
		}
		if err := validatePort(p.TCPServer.Port); err != nil {
			return err
		}
		if p.TCPServer.MaxConnections < 1 || p.TCPServer.MaxConnections > MaxTCPServerConnections {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidMaxConnections)
		}
		if err := p.TCPServer.Framing.Validate(); err != nil {
			return err
		}
	case TypeMQTTClient:
		if p.MQTT == nil || p.TCPClient != nil || p.TCPServer != nil {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidConfiguration)
		}
		if err := p.MQTT.Validate(p.ID); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidProfile, err)
		}
	default:
		return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidType)
	}
	return nil
}

// Validate verifies MQTT endpoint, version, TLS, Will, subscription, and reconnect settings.
func (m MQTTClient) Validate(profileID string) error {
	if m.Version != MQTTVersion311 && m.Version != MQTTVersion5 {
		return ErrInvalidMQTTVersion
	}
	if !validHost(m.Host) {
		return ErrInvalidHost
	}
	if err := validatePort(m.Port); err != nil {
		return err
	}
	if !validMQTTUTF8(m.ClientID) || len(m.ClientID) > 65535 || (m.ClientID == "" && !m.CleanStart) {
		return ErrInvalidClientID
	}
	if !validMQTTString(m.Username) {
		return ErrInvalidMQTTProperties
	}
	if m.KeepAliveSeconds > 65535 {
		return ErrInvalidKeepAlive
	}
	if m.Version == MQTTVersion311 && m.SessionExpirySeconds != nil {
		return ErrInvalidMQTTProperties
	}
	if m.PasswordRef != "" {
		if err := secrets.ValidateProfileReference(m.PasswordRef, profileID, secrets.FieldPassword); err != nil {
			return err
		}
	}
	if err := m.TLS.validate(profileID); err != nil {
		return err
	}
	if m.Will != nil {
		if err := m.Will.validate(m.Version); err != nil {
			return err
		}
	}
	for _, batch := range m.InitialSubscriptions {
		if err := batch.Validate(m.Version); err != nil {
			return err
		}
	}
	return m.Reconnect.Validate()
}

func (t MQTTTLSConfig) validate(profileID string) error {
	if !t.Enabled {
		if t.CAFile != "" || t.ClientCertFile != "" || t.ClientKeyFile != "" || t.ServerName != "" || t.PrivateKeyPassphraseRef != "" {
			return ErrInvalidMQTTTLS
		}
		return nil
	}
	if strings.TrimSpace(t.CAFile) != t.CAFile || strings.TrimSpace(t.ClientCertFile) != t.ClientCertFile ||
		strings.TrimSpace(t.ClientKeyFile) != t.ClientKeyFile || strings.TrimSpace(t.ServerName) != t.ServerName {
		return ErrInvalidMQTTTLS
	}
	if strings.ContainsRune(t.CAFile, 0) || strings.ContainsRune(t.ClientCertFile, 0) || strings.ContainsRune(t.ClientKeyFile, 0) ||
		(t.ServerName != "" && (!validHost(t.ServerName) || strings.ContainsRune(t.ServerName, '*'))) {
		return ErrInvalidMQTTTLS
	}
	if (t.CAFile != "" && !filepath.IsAbs(t.CAFile)) || (t.ClientCertFile != "" && !filepath.IsAbs(t.ClientCertFile)) ||
		(t.ClientKeyFile != "" && !filepath.IsAbs(t.ClientKeyFile)) {
		return ErrInvalidMQTTTLS
	}
	if (t.ClientCertFile == "") != (t.ClientKeyFile == "") {
		return ErrInvalidMQTTTLS
	}
	if t.PrivateKeyPassphraseRef != "" {
		if t.ClientKeyFile == "" {
			return ErrInvalidMQTTTLS
		}
		if err := secrets.ValidateProfileReference(t.PrivateKeyPassphraseRef, profileID, secrets.FieldPrivateKeyPassphrase); err != nil {
			return err
		}
	}
	return nil
}

func (w MQTTWill) validate(version MQTTVersion) error {
	if err := ValidatePublishTopic(w.Topic); err != nil {
		return err
	}
	if len(w.Payload) > MaxFrameBytes {
		return ErrFrameTooLarge
	}
	if err := ValidateMQTTQoS(w.QoS); err != nil {
		return err
	}
	if w.Properties != nil {
		if version != MQTTVersion5 {
			return ErrInvalidMQTTProperties
		}
		if w.Properties.PayloadFormat != nil && *w.Properties.PayloadFormat == MQTTPayloadFormatUTF8 && !utf8.Valid(w.Payload) {
			return ErrInvalidMQTTProperties
		}
		return w.Properties.validate()
	}
	return nil
}

func (p MQTTWillProperties) validate() error {
	if err := p.MQTTMessageProperties.validate(); err != nil {
		return err
	}
	return nil
}

func (p MQTTMessageProperties) validate() error {
	if len(p.CorrelationData) > 65535 {
		return ErrInvalidMQTTProperties
	}
	if p.ResponseTopic != nil {
		if err := ValidatePublishTopic(*p.ResponseTopic); err != nil {
			return ErrInvalidMQTTProperties
		}
	}
	if p.ContentType != nil && !validMQTTString(*p.ContentType) {
		return ErrInvalidMQTTProperties
	}
	if p.PayloadFormat != nil && *p.PayloadFormat != MQTTPayloadFormatBytes && *p.PayloadFormat != MQTTPayloadFormatUTF8 {
		return ErrInvalidMQTTProperties
	}
	for _, property := range p.UserProperties {
		if !validMQTTString(property.Name) || !validMQTTString(property.Value) {
			return ErrInvalidMQTTProperties
		}
	}
	return nil
}

// Validate verifies one subscription against the selected protocol version.
func (s MQTTSubscription) Validate(version MQTTVersion) error {
	if err := ValidateSubscriptionFilter(s.Filter); err != nil {
		return err
	}
	if err := ValidateMQTTQoS(s.QoS); err != nil {
		return err
	}
	if version == MQTTVersion311 {
		if s.NoLocal || s.RetainAsPublished || s.RetainHandling != "" {
			return ErrInvalidMQTTProperties
		}
		return nil
	}
	if version != MQTTVersion5 {
		return ErrInvalidMQTTVersion
	}
	switch s.RetainHandling {
	case "", MQTTRetainSendAtSubscribe, MQTTRetainSendIfNew, MQTTRetainDoNotSend:
		return nil
	default:
		return ErrInvalidMQTTProperties
	}
}

// Validate verifies a persistent SUBSCRIBE batch and its version-specific properties.
func (b MQTTSubscriptionBatch) Validate(version MQTTVersion) error {
	if len(b.Subscriptions) == 0 {
		return ErrInvalidTopicFilter
	}
	for _, subscription := range b.Subscriptions {
		if err := subscription.Validate(version); err != nil {
			return err
		}
	}
	if version == MQTTVersion311 {
		if b.SubscriptionIdentifier != nil || len(b.UserProperties) != 0 {
			return ErrInvalidMQTTProperties
		}
		return nil
	}
	if version != MQTTVersion5 {
		return ErrInvalidMQTTVersion
	}
	if b.SubscriptionIdentifier != nil && (*b.SubscriptionIdentifier == 0 || *b.SubscriptionIdentifier > 268435455) {
		return ErrInvalidMQTTProperties
	}
	for _, property := range b.UserProperties {
		if !validMQTTString(property.Name) || !validMQTTString(property.Value) {
			return ErrInvalidMQTTProperties
		}
	}
	return nil
}

// Validate verifies reconnect delay, multiplier, and jitter relationships.
func (r ReconnectPolicy) Validate() error {
	if math.IsNaN(r.Multiplier) || math.IsInf(r.Multiplier, 0) || math.IsNaN(r.JitterRatio) || math.IsInf(r.JitterRatio, 0) {
		return ErrInvalidReconnectPolicy
	}
	if !r.Enabled {
		if r.InitialDelayMS != 0 || r.MaxDelayMS != 0 || r.Multiplier != 0 || r.JitterRatio != 0 || r.MaxAttempts != 0 {
			return ErrInvalidReconnectPolicy
		}
		return nil
	}
	if r.InitialDelayMS != 0 && r.MaxDelayMS != 0 && r.MaxDelayMS < r.InitialDelayMS {
		return ErrInvalidReconnectPolicy
	}
	if r.Multiplier != 0 && (r.Multiplier < 1 || r.Multiplier > 100) {
		return ErrInvalidReconnectPolicy
	}
	if r.JitterRatio < 0 || r.JitterRatio > 1 {
		return ErrInvalidReconnectPolicy
	}
	return nil
}

// ValidateMQTTQoS accepts MQTT QoS levels 0, 1, and 2.
func ValidateMQTTQoS(qos MQTTQoS) error {
	if qos > MQTTQoSExactlyOnce {
		return ErrInvalidQoS
	}
	return nil
}

// ValidatePublishTopic validates a non-empty topic name and rejects wildcards.
func ValidatePublishTopic(topic string) error {
	if !validMQTTString(topic) || topic == "" || strings.ContainsAny(topic, "+#") {
		return ErrInvalidTopic
	}
	return nil
}

// ValidateSubscriptionFilter validates wildcard placement by MQTT topic level.
func ValidateSubscriptionFilter(filter string) error {
	if !validMQTTString(filter) || filter == "" {
		return ErrInvalidTopicFilter
	}
	levels := strings.Split(filter, "/")
	for index, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || index != len(levels)-1) {
			return ErrInvalidTopicFilter
		}
		if strings.Contains(level, "+") && level != "+" {
			return ErrInvalidTopicFilter
		}
	}
	if strings.HasPrefix(filter, "$share/") {
		if len(levels) < 3 || levels[1] == "" || strings.ContainsAny(levels[1], "+#") || strings.Join(levels[2:], "/") == "" {
			return ErrInvalidTopicFilter
		}
	}
	return nil
}

func validHost(host string) bool {
	return host != "" && validMQTTString(host) && strings.TrimSpace(host) == host && !strings.Contains(host, "://") && !strings.ContainsAny(host, "/\\\t\r\n ")
}

func validMQTTString(value string) bool {
	return len(value) <= 65535 && validMQTTUTF8(value)
}

func validMQTTUTF8(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character == 0 || (character >= 0x01 && character <= 0x1f) || (character >= 0x7f && character <= 0x9f) ||
			(character >= 0xd800 && character <= 0xdfff) || (character >= 0xfdd0 && character <= 0xfdef) || character&0xffff == 0xfffe || character&0xffff == 0xffff {
			return false
		}
	}
	return true
}

// Validate verifies that the framing mode has exactly the fields it needs and
// remains within the application safety limits.
func (f FramingConfig) Validate() error {
	switch f.Mode {
	case FrameModeRawRead:
		if f.FixedLength != 0 || len(f.Delimiter) != 0 || f.IncludeDelimiter {
			return ErrInvalidFraming
		}
	case FrameModeFixedLength:
		if f.FixedLength < 1 || len(f.Delimiter) != 0 || f.IncludeDelimiter {
			return ErrInvalidFraming
		}
		if f.FixedLength > MaxFrameBytes {
			return fmt.Errorf("%w: %w", ErrInvalidFraming, ErrFrameTooLarge)
		}
	case FrameModeDelimiter:
		if f.FixedLength != 0 || len(f.Delimiter) == 0 {
			return ErrInvalidFraming
		}
		if len(f.Delimiter) > MaxDelimiterBytes {
			return fmt.Errorf("%w: %w", ErrInvalidFraming, ErrFrameTooLarge)
		}
	default:
		return ErrInvalidFraming
	}
	return nil
}

func validatePort(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("%w: %w", ErrInvalidProfile, ErrInvalidPort)
	}
	return nil
}

func validDurationMilliseconds(value int64) bool {
	return value >= 0 && value <= maxDurationMilliseconds
}

// Clone returns a deep copy of p.
func (p Profile) Clone() Profile {
	clone := p
	if p.TCPClient != nil {
		client := *p.TCPClient
		client.Framing = cloneFraming(p.TCPClient.Framing)
		clone.TCPClient = &client
	}
	if p.TCPServer != nil {
		server := *p.TCPServer
		server.Framing = cloneFraming(p.TCPServer.Framing)
		clone.TCPServer = &server
	}
	if p.MQTT != nil {
		mqtt := *p.MQTT
		mqtt.SessionExpirySeconds = cloneUint32(p.MQTT.SessionExpirySeconds)
		mqtt.InitialSubscriptions = make([]MQTTSubscriptionBatch, len(p.MQTT.InitialSubscriptions))
		for index, batch := range p.MQTT.InitialSubscriptions {
			mqtt.InitialSubscriptions[index] = batch
			mqtt.InitialSubscriptions[index].Subscriptions = append([]MQTTSubscription(nil), batch.Subscriptions...)
			mqtt.InitialSubscriptions[index].SubscriptionIdentifier = cloneUint32(batch.SubscriptionIdentifier)
			mqtt.InitialSubscriptions[index].UserProperties = append([]MQTTUserProperty(nil), batch.UserProperties...)
		}
		if p.MQTT.Will != nil {
			will := *p.MQTT.Will
			will.Payload = append([]byte(nil), p.MQTT.Will.Payload...)
			will.Properties = cloneWillProperties(p.MQTT.Will.Properties)
			mqtt.Will = &will
		}
		clone.MQTT = &mqtt
	}
	return clone
}

func cloneWillProperties(properties *MQTTWillProperties) *MQTTWillProperties {
	if properties == nil {
		return nil
	}
	clone := *properties
	clone.MQTTMessageProperties = cloneMessageProperties(properties.MQTTMessageProperties)
	clone.WillDelayIntervalSeconds = cloneUint32(properties.WillDelayIntervalSeconds)
	return &clone
}

func cloneMessageProperties(properties MQTTMessageProperties) MQTTMessageProperties {
	clone := properties
	clone.ResponseTopic = cloneString(properties.ResponseTopic)
	clone.CorrelationData = append([]byte(nil), properties.CorrelationData...)
	clone.ContentType = cloneString(properties.ContentType)
	if properties.PayloadFormat != nil {
		value := *properties.PayloadFormat
		clone.PayloadFormat = &value
	}
	clone.MessageExpiryIntervalSeconds = cloneUint32(properties.MessageExpiryIntervalSeconds)
	clone.UserProperties = append([]MQTTUserProperty(nil), properties.UserProperties...)
	return clone
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneUint32(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneFraming(framing FramingConfig) FramingConfig {
	clone := framing
	clone.Delimiter = append([]byte(nil), framing.Delimiter...)
	return clone
}
