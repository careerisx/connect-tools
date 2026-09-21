package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	pahov5 "github.com/eclipse/paho.golang/paho"
	paholog "github.com/eclipse/paho.golang/paho/log"
)

type v5Client struct {
	config Config
	sink   *eventSink

	mu            sync.Mutex
	state         clientState
	password      []byte
	manager       *autopaho.ConnectionManager
	managerCancel context.CancelFunc
	contextStop   func() bool
	closeOnce     sync.Once
	closeDone     chan struct{}

	disconnectMu sync.Mutex
	disconnect   DisconnectInfo
}

func newV5Client(config Config) *v5Client {
	owned := cloneConfig(config)
	return &v5Client{
		config:    owned,
		sink:      newEventSink(),
		state:     clientStateReady,
		password:  owned.Auth.Password,
		closeDone: make(chan struct{}),
	}
}

// Connect starts one MQTT 5 connection attempt and waits for its CONNACK.
func (c *v5Client) Connect(ctx context.Context) (ConnAck, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.state != clientStateReady {
		c.mu.Unlock()
		return ConnAck{}, ErrClientClosed
	}
	c.state = clientStateConnecting
	c.mu.Unlock()

	managerContext, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.managerCancel = cancel
	c.mu.Unlock()
	connectionUp := make(chan *pahov5.Connack, 1)
	connectionError := make(chan error, 1)
	config := c.v5Config(cancel, connectionUp, connectionError)
	manager, err := autopaho.NewConnection(managerContext, config)
	if err != nil {
		cancel()
		c.clearPassword()
		c.mu.Lock()
		if c.state != clientStateClosing && c.state != clientStateClosed {
			c.state = clientStateDisconnected
		}
		c.mu.Unlock()
		return ConnAck{}, fmt.Errorf("MQTT 5 connection manager failed: %w", err)
	}
	c.mu.Lock()
	if c.state == clientStateClosing || c.state == clientStateClosed {
		c.mu.Unlock()
		<-manager.Done()
		return ConnAck{}, ErrClientClosed
	}
	c.manager = manager
	c.mu.Unlock()

	select {
	case connack := <-connectionUp:
		c.clearPassword()
		ack := mapV5ConnAck(connack)
		if err := ctx.Err(); err != nil {
			c.beginShutdown()
			<-c.closeDone
			return ack, err
		}
		c.mu.Lock()
		if c.state != clientStateConnecting {
			c.mu.Unlock()
			return ack, ErrClientNotConnected
		}
		c.state = clientStateConnected
		c.contextStop = context.AfterFunc(ctx, c.beginShutdown)
		c.mu.Unlock()
		return ack, nil
	case connectErr := <-connectionError:
		<-manager.Done()
		c.clearPassword()
		c.setDisconnected()
		if err := ctx.Err(); err != nil {
			return ConnAck{}, err
		}
		ack := ConnAck{}
		var connackError *autopaho.ConnackError
		if errors.As(connectErr, &connackError) {
			ack.ReasonCode = connackError.ReasonCode
			ack.ReasonString = connackError.Reason
		}
		return ack, fmt.Errorf("MQTT 5 connect failed: %w", connectErr)
	case <-ctx.Done():
		c.beginShutdown()
		<-c.closeDone
		c.clearPassword()
		return ConnAck{}, ctx.Err()
	}
}

func (c *v5Client) v5Config(cancel context.CancelFunc, connectionUp chan<- *pahov5.Connack, connectionError chan<- error) autopaho.ClientConfig {
	serverURL := &url.URL{Scheme: "mqtt", Host: net.JoinHostPort(c.config.Host, fmt.Sprintf("%d", c.config.Port))}
	if c.config.TLS != nil {
		serverURL.Scheme = "tls"
	}
	noop := paholog.NOOPLogger{}
	config := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{serverURL},
		TlsCfg:                        cloneTLS(c.config.TLS),
		KeepAlive:                     uint16(c.config.KeepAlive / time.Second),
		CleanStartOnInitialConnection: c.config.CleanStart,
		ReconnectBackoff:              func(int) time.Duration { return 0 },
		Debug:                         noop,
		Errors:                        noop,
		PahoDebug:                     noop,
		PahoErrors:                    noop,
		ConnectUsername:               c.config.Auth.Username,
		ConnectPassword:               c.password,
		OnConnectionUp: func(_ *autopaho.ConnectionManager, connack *pahov5.Connack) {
			select {
			case connectionUp <- connack:
			default:
			}
		},
		OnConnectError: func(err error) {
			select {
			case connectionError <- err:
			default:
			}
			cancel()
		},
		OnConnectionDown: func() bool {
			shouldReconnect := c.onConnectionDown()
			cancel()
			return shouldReconnect
		},
		ClientConfig: pahov5.ClientConfig{
			ClientID: c.config.ClientID,
			OnPublishReceived: []func(pahov5.PublishReceived) (bool, error){
				c.onPublishReceived,
			},
			OnClientError:      c.onClientError,
			OnServerDisconnect: c.onServerDisconnect,
		},
	}
	if c.config.SessionExpiryInterval != nil {
		config.SessionExpiryInterval = *c.config.SessionExpiryInterval
		expiry := *c.config.SessionExpiryInterval
		config.ConnectPacketBuilder = func(packet *pahov5.Connect, _ *url.URL) (*pahov5.Connect, error) {
			if packet.Properties == nil {
				packet.Properties = &pahov5.ConnectProperties{}
			}
			packet.Properties.SessionExpiryInterval = &expiry
			return packet, nil
		}
	}
	if c.config.Will != nil {
		config.WillMessage = &pahov5.WillMessage{
			Retain:  c.config.Will.Retain,
			Payload: cloneBytes(c.config.Will.Payload),
			Topic:   c.config.Will.Topic,
			QoS:     c.config.Will.QoS,
		}
		config.WillProperties = mapV5WillProperties(c.config.Will.Properties)
	}
	return config
}

// Publish maps MQTT 5 properties and returns the broker result available from Paho.
func (c *v5Client) Publish(ctx context.Context, message Publish) (PublishAck, error) {
	if err := validatePublish(message, Version5); err != nil {
		return PublishAck{}, err
	}
	manager, err := c.connectedManager()
	if err != nil {
		return PublishAck{}, err
	}
	message = clonePublish(message)
	response, publishErr := manager.Publish(normalizeContext(ctx), &pahov5.Publish{
		QoS:        message.QoS,
		Retain:     message.Retain,
		Topic:      message.Topic,
		Payload:    message.Payload,
		Properties: mapV5MessageProperties(message.Properties),
	})
	ack := PublishAck{Accepted: publishErr == nil, Acknowledged: publishErr == nil && message.QoS > 0}
	if response != nil && message.QoS > 0 {
		reason := response.ReasonCode
		ack.ReasonCode = &reason
		if response.Properties != nil {
			ack.ReasonString = response.Properties.ReasonString
			ack.UserProperties = mapV5UserProperties(response.Properties.User)
		}
	}
	if publishErr != nil {
		c.stopOnContextCancellation(ctx)
		return ack, fmt.Errorf("MQTT 5 publish failed: %w", publishErr)
	}
	return ack, nil
}

// Subscribe sends one ordered MQTT 5 SUBSCRIBE packet.
func (c *v5Client) Subscribe(ctx context.Context, batch SubscriptionBatch) (SubscribeAck, error) {
	if err := validateSubscriptions(batch, Version5); err != nil {
		return SubscribeAck{}, err
	}
	manager, err := c.connectedManager()
	if err != nil {
		return SubscribeAck{}, err
	}
	batch = cloneSubscriptionBatch(batch)
	subscriptions := make([]pahov5.SubscribeOptions, len(batch.Subscriptions))
	for index, subscription := range batch.Subscriptions {
		subscriptions[index] = pahov5.SubscribeOptions{
			Topic:             subscription.Filter,
			QoS:               subscription.QoS,
			NoLocal:           subscription.NoLocal,
			RetainAsPublished: subscription.RetainAsPublished,
			RetainHandling:    subscription.RetainHandling,
		}
	}
	properties := &pahov5.SubscribeProperties{User: toV5UserProperties(batch.UserProperties)}
	if batch.SubscriptionIdentifier != nil {
		identifier := int(*batch.SubscriptionIdentifier)
		properties.SubscriptionIdentifier = &identifier
	}
	response, subscribeErr := manager.Subscribe(normalizeContext(ctx), &pahov5.Subscribe{
		Subscriptions: subscriptions,
		Properties:    properties,
	})
	ack := mapV5SubscribeAck(batch, response)
	if subscribeErr != nil {
		c.stopOnContextCancellation(ctx)
		return ack, fmt.Errorf("MQTT 5 subscribe failed: %w", subscribeErr)
	}
	return ack, nil
}

// Unsubscribe sends one ordered MQTT 5 UNSUBSCRIBE packet.
func (c *v5Client) Unsubscribe(ctx context.Context, topics []string) (UnsubscribeAck, error) {
	if len(topics) == 0 {
		return UnsubscribeAck{}, ErrInvalidOperation
	}
	manager, err := c.connectedManager()
	if err != nil {
		return UnsubscribeAck{}, err
	}
	ownedTopics := append([]string(nil), topics...)
	for _, topic := range ownedTopics {
		if topic == "" {
			return UnsubscribeAck{}, ErrInvalidOperation
		}
	}
	response, unsubscribeErr := manager.Unsubscribe(normalizeContext(ctx), &pahov5.Unsubscribe{Topics: ownedTopics})
	ack := mapV5UnsubscribeAck(ownedTopics, response)
	if unsubscribeErr != nil {
		c.stopOnContextCancellation(ctx)
		return ack, fmt.Errorf("MQTT 5 unsubscribe failed: %w", unsubscribeErr)
	}
	return ack, nil
}

// Disconnect idempotently stops autopaho and closes Events after its manager exits.
func (c *v5Client) Disconnect(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.beginShutdown()
	select {
	case <-c.closeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Events returns the adapter's bounded event stream.
func (c *v5Client) Events() <-chan Event {
	return c.sink.channel()
}

func (c *v5Client) connectedManager() (*autopaho.ConnectionManager, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == clientStateClosing || c.state == clientStateClosed {
		return nil, ErrClientClosed
	}
	if c.state != clientStateConnected || c.manager == nil {
		return nil, ErrClientNotConnected
	}
	return c.manager, nil
}

func (c *v5Client) beginShutdown() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.state = clientStateClosing
		manager := c.manager
		cancel := c.managerCancel
		stop := c.contextStop
		c.contextStop = nil
		c.mu.Unlock()
		if stop != nil {
			stop()
		}
		if cancel != nil {
			cancel()
		}
		go func() {
			if manager != nil {
				_ = manager.Disconnect(context.Background())
			}
			c.clearPassword()
			c.setState(clientStateClosed)
			c.sink.close()
			close(c.closeDone)
		}()
	})
}

func (c *v5Client) clearPassword() {
	c.mu.Lock()
	defer c.mu.Unlock()
	zero(c.password)
	c.password = nil
	c.config.Auth.Password = nil
}

func (c *v5Client) setState(state clientState) {
	c.mu.Lock()
	c.state = state
	c.mu.Unlock()
}

func (c *v5Client) setDisconnected() {
	c.mu.Lock()
	if c.state != clientStateClosing && c.state != clientStateClosed {
		c.state = clientStateDisconnected
	}
	c.mu.Unlock()
}

func (c *v5Client) stopOnContextCancellation(ctx context.Context) {
	if ctx != nil && ctx.Err() != nil {
		c.beginShutdown()
		<-c.closeDone
	}
}

func (c *v5Client) onPublishReceived(received pahov5.PublishReceived) (bool, error) {
	if received.Packet == nil {
		return false, nil
	}
	packet := received.Packet
	message := &InboundMessage{
		Topic:           packet.Topic,
		Payload:         cloneBytes(packet.Payload),
		QoS:             packet.QoS,
		Retain:          packet.Retain,
		Duplicate:       packet.Duplicate(),
		PacketID:        packet.PacketID,
		PacketIDPresent: packet.PacketID != 0,
	}
	if packet.Properties != nil {
		message.Properties = MessageProperties{
			ResponseTopic:         stringPresence(packet.Properties.ResponseTopic),
			CorrelationData:       cloneBytes(packet.Properties.CorrelationData),
			ContentType:           stringPresence(packet.Properties.ContentType),
			PayloadFormat:         cloneByte(packet.Properties.PayloadFormat),
			MessageExpiryInterval: cloneUint32(packet.Properties.MessageExpiry),
			UserProperties:        mapV5UserProperties(packet.Properties.User),
		}
		if packet.Properties.SubscriptionIdentifier != nil && *packet.Properties.SubscriptionIdentifier > 0 {
			message.SubscriptionIdentifiers = []uint32{uint32(*packet.Properties.SubscriptionIdentifier)}
		}
	}
	c.sink.emit(Event{Kind: EventInboundMessage, Message: message})
	return true, nil
}

func (c *v5Client) onClientError(err error) {
	c.disconnectMu.Lock()
	c.disconnect = DisconnectInfo{Unexpected: true, Cause: safeErrorMessage(err)}
	c.disconnectMu.Unlock()
}

func (c *v5Client) onServerDisconnect(packet *pahov5.Disconnect) {
	info := DisconnectInfo{Unexpected: true}
	if packet != nil {
		reason := packet.ReasonCode
		info.ReasonCode = &reason
		if packet.Properties != nil {
			info.ReasonString = packet.Properties.ReasonString
			info.ServerReference = packet.Properties.ServerReference
			info.UserProperties = mapV5UserProperties(packet.Properties.User)
		}
	}
	c.disconnectMu.Lock()
	c.disconnect = info
	c.disconnectMu.Unlock()
}

func (c *v5Client) onConnectionDown() bool {
	c.mu.Lock()
	if c.state == clientStateClosing || c.state == clientStateClosed {
		c.mu.Unlock()
		return false
	}
	c.state = clientStateDisconnected
	c.mu.Unlock()
	c.disconnectMu.Lock()
	info := c.disconnect
	c.disconnect = DisconnectInfo{}
	c.disconnectMu.Unlock()
	info.Unexpected = true
	if info.Cause == "" && info.ReasonCode == nil {
		info.Cause = "MQTT connection closed"
	}
	c.sink.emit(Event{Kind: EventDisconnected, Disconnect: &info})
	return false
}

func mapV5ConnAck(connack *pahov5.Connack) ConnAck {
	if connack == nil {
		return ConnAck{}
	}
	ack := ConnAck{SessionPresent: connack.SessionPresent, ReasonCode: connack.ReasonCode}
	if connack.Properties == nil {
		return ack
	}
	properties := connack.Properties
	ack.ReasonString = properties.ReasonString
	ack.Properties = ServerProperties{
		SessionExpiryInterval:   cloneUint32(properties.SessionExpiryInterval),
		AssignedClientID:        properties.AssignedClientID,
		ServerKeepAlive:         cloneUint16(properties.ServerKeepAlive),
		ServerReference:         properties.ServerReference,
		ReceiveMaximum:          cloneUint16(properties.ReceiveMaximum),
		TopicAliasMaximum:       cloneUint16(properties.TopicAliasMaximum),
		MaximumPacketSize:       cloneUint32(properties.MaximumPacketSize),
		MaximumQoS:              cloneByte(properties.MaximumQoS),
		RetainAvailable:         properties.RetainAvailable,
		WildcardSubAvailable:    properties.WildcardSubAvailable,
		SubscriptionIDAvailable: properties.SubIDAvailable,
		SharedSubAvailable:      properties.SharedSubAvailable,
		UserProperties:          mapV5UserProperties(properties.User),
	}
	return ack
}

func mapV5SubscribeAck(batch SubscriptionBatch, response *pahov5.Suback) SubscribeAck {
	ack := SubscribeAck{Results: make([]SubscriptionAck, len(batch.Subscriptions))}
	if response != nil && response.Properties != nil {
		ack.ReasonString = response.Properties.ReasonString
		ack.UserProperties = mapV5UserProperties(response.Properties.User)
	}
	for index, subscription := range batch.Subscriptions {
		result := SubscriptionAck{Filter: subscription.Filter, ReasonCode: 0x80}
		if response != nil && index < len(response.Reasons) {
			result.ReasonCode = response.Reasons[index]
			result.Success = result.ReasonCode < 0x80
			if result.Success && result.ReasonCode <= 2 {
				qos := result.ReasonCode
				result.GrantedQoS = &qos
			}
		}
		ack.Results[index] = result
	}
	return ack
}

func mapV5UnsubscribeAck(topics []string, response *pahov5.Unsuback) UnsubscribeAck {
	ack := UnsubscribeAck{Results: make([]TopicAck, len(topics))}
	if response != nil && response.Properties != nil {
		ack.ReasonString = response.Properties.ReasonString
		ack.UserProperties = mapV5UserProperties(response.Properties.User)
	}
	for index, topic := range topics {
		result := TopicAck{Topic: topic}
		if response != nil && index < len(response.Reasons) {
			reason := response.Reasons[index]
			result.ReasonCode = &reason
			result.Success = reason < 0x80
		}
		ack.Results[index] = result
	}
	return ack
}

func mapV5MessageProperties(properties *MessageProperties) *pahov5.PublishProperties {
	if properties == nil {
		return nil
	}
	return &pahov5.PublishProperties{
		CorrelationData: cloneBytes(properties.CorrelationData),
		ContentType:     stringValue(properties.ContentType),
		ResponseTopic:   stringValue(properties.ResponseTopic),
		PayloadFormat:   cloneByte(properties.PayloadFormat),
		MessageExpiry:   cloneUint32(properties.MessageExpiryInterval),
		User:            toV5UserProperties(properties.UserProperties),
	}
}

func mapV5WillProperties(properties *WillProperties) *pahov5.WillProperties {
	if properties == nil {
		return nil
	}
	return &pahov5.WillProperties{
		WillDelayInterval: cloneUint32(properties.WillDelayInterval),
		PayloadFormat:     cloneByte(properties.PayloadFormat),
		MessageExpiry:     cloneUint32(properties.MessageExpiryInterval),
		ContentType:       stringValue(properties.ContentType),
		ResponseTopic:     stringValue(properties.ResponseTopic),
		CorrelationData:   cloneBytes(properties.CorrelationData),
		User:              toV5UserProperties(properties.UserProperties),
	}
}

func toV5UserProperties(properties []UserProperty) pahov5.UserProperties {
	result := make(pahov5.UserProperties, len(properties))
	for index, property := range properties {
		result[index] = pahov5.UserProperty{Key: property.Name, Value: property.Value}
	}
	return result
}

func mapV5UserProperties(properties pahov5.UserProperties) []UserProperty {
	result := make([]UserProperty, len(properties))
	for index, property := range properties {
		result[index] = UserProperty{Name: property.Key, Value: property.Value}
	}
	return result
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func stringPresence(value string) *string {
	if value == "" {
		return nil
	}
	clone := value
	return &clone
}

func cloneTLS(config *tls.Config) *tls.Config {
	if config == nil {
		return nil
	}
	return config.Clone()
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
