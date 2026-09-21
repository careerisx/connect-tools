package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"
)

const (
	// Version311 selects MQTT 3.1.1.
	Version311 Version = "3.1.1"
	// Version5 selects MQTT 5.0.
	Version5 Version = "5.0"
)

var (
	// ErrUnsupportedVersion indicates a protocol version other than MQTT 3.1.1 or 5.0.
	ErrUnsupportedVersion = errors.New("unsupported MQTT version")
	// ErrInvalidConfig indicates an unusable adapter configuration.
	ErrInvalidConfig = errors.New("invalid MQTT client configuration")
	// ErrInvalidOperation indicates an operation that cannot be represented by the selected protocol version.
	ErrInvalidOperation = errors.New("invalid MQTT operation")
	// ErrClientClosed indicates an operation attempted after shutdown started.
	ErrClientClosed = errors.New("MQTT client is closed")
	// ErrClientNotConnected indicates an operation that requires an established connection.
	ErrClientNotConnected = errors.New("MQTT client is not connected")
)

// Version is the MQTT wire protocol revision used by an adapter.
type Version string

// Client is the protocol-neutral MQTT client contract used by the runtime.
type Client interface {
	Connect(context.Context) (ConnAck, error)
	Publish(context.Context, Publish) (PublishAck, error)
	Subscribe(context.Context, SubscriptionBatch) (SubscribeAck, error)
	Unsubscribe(context.Context, []string) (UnsubscribeAck, error)
	Disconnect(context.Context) error
	Events() <-chan Event
}

// ClientFactory creates independent MQTT clients from owned configuration snapshots.
type ClientFactory interface {
	New(Config) (Client, error)
}

// Config contains connection inputs shared by the MQTT 3.1.1 and MQTT 5 adapters.
// New clients deep-copy all reference-backed fields before returning.
type Config struct {
	Version               Version
	Host                  string
	Port                  int
	ClientID              string
	CleanStart            bool
	KeepAlive             time.Duration
	SessionExpiryInterval *uint32
	TLS                   *tls.Config
	Auth                  Auth
	Will                  *Will
}

// Auth contains MQTT username/password authentication material.
type Auth struct {
	Username string
	Password []byte
}

// Will is the publication the broker sends following an ungraceful disconnect.
type Will struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties *WillProperties
}

// UserProperty is one ordered MQTT 5 user-property pair.
type UserProperty struct {
	Name  string
	Value string
}

// MessageProperties contains MQTT 5 properties shared by Will and PUBLISH packets.
type MessageProperties struct {
	ResponseTopic         *string
	CorrelationData       []byte
	ContentType           *string
	PayloadFormat         *byte
	MessageExpiryInterval *uint32
	UserProperties        []UserProperty
}

// WillProperties contains MQTT 5 Will properties.
type WillProperties struct {
	MessageProperties
	WillDelayInterval *uint32
}

// Publish is a protocol-neutral outbound MQTT publication.
type Publish struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties *MessageProperties
}

// Subscription describes one topic filter in a SUBSCRIBE packet.
type Subscription struct {
	Filter            string
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

// SubscriptionBatch preserves filter order and packet-scoped MQTT 5 properties.
type SubscriptionBatch struct {
	Subscriptions          []Subscription
	SubscriptionIdentifier *uint32
	UserProperties         []UserProperty
}

// ConnAck is the protocol-neutral result of a completed MQTT CONNECT exchange.
type ConnAck struct {
	SessionPresent bool
	ReasonCode     byte
	ReasonString   string
	Properties     ServerProperties
}

// ServerProperties contains commonly used MQTT 5 CONNACK properties.
type ServerProperties struct {
	SessionExpiryInterval   *uint32
	AssignedClientID        string
	ServerKeepAlive         *uint16
	ServerReference         string
	ReceiveMaximum          *uint16
	TopicAliasMaximum       *uint16
	MaximumPacketSize       *uint32
	MaximumQoS              *byte
	RetainAvailable         bool
	WildcardSubAvailable    bool
	SubscriptionIDAvailable bool
	SharedSubAvailable      bool
	UserProperties          []UserProperty
}

// PublishAck describes acceptance and, for QoS 1/2, broker acknowledgment.
type PublishAck struct {
	Accepted        bool
	Acknowledged    bool
	PacketID        uint16
	PacketIDPresent bool
	ReasonCode      *byte
	ReasonString    string
	UserProperties  []UserProperty
}

// SubscriptionAck is aligned with one requested subscription.
type SubscriptionAck struct {
	Filter     string
	ReasonCode byte
	GrantedQoS *byte
	Success    bool
}

// SubscribeAck contains ordered per-filter acknowledgment results.
type SubscribeAck struct {
	Results        []SubscriptionAck
	ReasonString   string
	UserProperties []UserProperty
}

// TopicAck is aligned with one requested unsubscribe topic.
type TopicAck struct {
	Topic      string
	ReasonCode *byte
	Success    bool
}

// UnsubscribeAck contains ordered per-topic acknowledgment results.
type UnsubscribeAck struct {
	Results        []TopicAck
	ReasonString   string
	UserProperties []UserProperty
}

func validateConfig(config Config) error {
	if config.Version != Version311 && config.Version != Version5 {
		return fmt.Errorf("%w: %s", ErrUnsupportedVersion, config.Version)
	}
	if config.Host == "" || config.Port < 1 || config.Port > 65535 {
		return ErrInvalidConfig
	}
	if config.KeepAlive < 0 || config.KeepAlive%time.Second != 0 || config.KeepAlive/time.Second > 65535 {
		return ErrInvalidConfig
	}
	if config.Version == Version311 {
		if config.SessionExpiryInterval != nil || (config.Will != nil && config.Will.Properties != nil) {
			return fmt.Errorf("%w: MQTT 5 properties are not valid for MQTT 3.1.1", ErrInvalidConfig)
		}
		if len(config.Auth.Password) != 0 && config.Auth.Username == "" {
			return fmt.Errorf("%w: MQTT 3.1.1 password requires a username", ErrInvalidConfig)
		}
	}
	if config.Will != nil {
		if config.Will.Topic == "" || config.Will.QoS > 2 {
			return ErrInvalidConfig
		}
	}
	return nil
}

func validatePublish(message Publish, version Version) error {
	if message.Topic == "" || message.QoS > 2 {
		return ErrInvalidOperation
	}
	if version == Version311 && message.Properties != nil {
		return fmt.Errorf("%w: MQTT 5 publish properties are not valid for MQTT 3.1.1", ErrInvalidOperation)
	}
	return nil
}

func validateSubscriptions(batch SubscriptionBatch, version Version) error {
	if len(batch.Subscriptions) == 0 {
		return ErrInvalidOperation
	}
	seen := make(map[string]struct{}, len(batch.Subscriptions))
	for _, subscription := range batch.Subscriptions {
		if subscription.Filter == "" || subscription.QoS > 2 || subscription.RetainHandling > 2 {
			return ErrInvalidOperation
		}
		if _, duplicate := seen[subscription.Filter]; duplicate {
			return fmt.Errorf("%w: duplicate subscription filter", ErrInvalidOperation)
		}
		seen[subscription.Filter] = struct{}{}
		if version == Version311 && (subscription.NoLocal || subscription.RetainAsPublished || subscription.RetainHandling != 0) {
			return fmt.Errorf("%w: MQTT 5 subscription options are not valid for MQTT 3.1.1", ErrInvalidOperation)
		}
	}
	if version == Version311 && (batch.SubscriptionIdentifier != nil || len(batch.UserProperties) != 0) {
		return fmt.Errorf("%w: MQTT 5 subscribe properties are not valid for MQTT 3.1.1", ErrInvalidOperation)
	}
	if batch.SubscriptionIdentifier != nil && (*batch.SubscriptionIdentifier == 0 || *batch.SubscriptionIdentifier > 268435455) {
		return ErrInvalidOperation
	}
	return nil
}

func cloneConfig(config Config) Config {
	clone := config
	clone.Auth.Password = cloneBytes(config.Auth.Password)
	clone.SessionExpiryInterval = cloneUint32(config.SessionExpiryInterval)
	if config.TLS != nil {
		clone.TLS = config.TLS.Clone()
	}
	if config.Will != nil {
		will := *config.Will
		will.Payload = cloneBytes(config.Will.Payload)
		will.Properties = cloneWillProperties(config.Will.Properties)
		clone.Will = &will
	}
	return clone
}

func clonePublish(message Publish) Publish {
	clone := message
	clone.Payload = cloneBytes(message.Payload)
	clone.Properties = cloneMessageProperties(message.Properties)
	return clone
}

func cloneSubscriptionBatch(batch SubscriptionBatch) SubscriptionBatch {
	clone := batch
	clone.Subscriptions = append([]Subscription(nil), batch.Subscriptions...)
	clone.SubscriptionIdentifier = cloneUint32(batch.SubscriptionIdentifier)
	clone.UserProperties = append([]UserProperty(nil), batch.UserProperties...)
	return clone
}

func cloneMessageProperties(properties *MessageProperties) *MessageProperties {
	if properties == nil {
		return nil
	}
	clone := *properties
	clone.ResponseTopic = cloneString(properties.ResponseTopic)
	clone.CorrelationData = cloneBytes(properties.CorrelationData)
	clone.ContentType = cloneString(properties.ContentType)
	clone.PayloadFormat = cloneByte(properties.PayloadFormat)
	clone.MessageExpiryInterval = cloneUint32(properties.MessageExpiryInterval)
	clone.UserProperties = append([]UserProperty(nil), properties.UserProperties...)
	return &clone
}

func cloneWillProperties(properties *WillProperties) *WillProperties {
	if properties == nil {
		return nil
	}
	clone := *properties
	message := cloneMessageProperties(&properties.MessageProperties)
	clone.MessageProperties = *message
	clone.WillDelayInterval = cloneUint32(properties.WillDelayInterval)
	return &clone
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneByte(value *byte) *byte {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneUint16(value *uint16) *uint16 {
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

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func zero(value []byte) {
	clear(value)
}
