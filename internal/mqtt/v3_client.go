package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"sync"

	pahov3 "github.com/eclipse/paho.mqtt.golang"
)

const v3DisconnectQuiesce = 1000

type v3Client struct {
	config Config
	client pahov3.Client
	sink   *eventSink

	mu            sync.Mutex
	state         clientState
	password      []byte
	contextStop   func() bool
	connectCtx    context.Context
	connectCancel context.CancelFunc
	connCtxStop   func() bool
	closeOnce     sync.Once
	closeDone     chan struct{}

	callbackMu      sync.Mutex
	callbackWG      sync.WaitGroup
	callbacksClosed bool
}

func newV3Client(config Config) *v3Client {
	owned := cloneConfig(config)
	client := &v3Client{
		config:    owned,
		sink:      newEventSink(),
		state:     clientStateReady,
		password:  owned.Auth.Password,
		closeDone: make(chan struct{}),
	}

	options := pahov3.NewClientOptions()
	options.AddBroker(v3BrokerURL(owned))
	options.SetClientID(owned.ClientID)
	options.SetCleanSession(owned.CleanStart)
	options.SetKeepAlive(owned.KeepAlive)
	options.SetProtocolVersion(4)
	options.SetAutoReconnect(false)
	options.SetConnectRetry(false)
	options.SetOrderMatters(false)
	options.SetCustomOpenConnectionFn(client.openConnection)
	if owned.TLS != nil {
		options.SetTLSConfig(owned.TLS.Clone())
	}
	if owned.Will != nil {
		options.SetWill(owned.Will.Topic, string(owned.Will.Payload), owned.Will.QoS, owned.Will.Retain)
	}
	options.SetCredentialsProvider(client.credentials)
	options.SetDefaultPublishHandler(client.onMessage)
	options.SetConnectionLostHandler(client.onConnectionLost)
	client.client = pahov3.NewClient(options)
	return client
}

func v3BrokerURL(config Config) string {
	scheme := "tcp"
	if config.TLS != nil {
		scheme = "ssl"
	}
	return (&url.URL{Scheme: scheme, Host: net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port))}).String()
}

func (c *v3Client) credentials() (string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.Auth.Username, string(c.password)
}

// Connect waits for the MQTT 3.1.1 CONNACK or a definitive initial failure.
func (c *v3Client) Connect(ctx context.Context) (ConnAck, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.state != clientStateReady {
		c.mu.Unlock()
		return ConnAck{}, ErrClientClosed
	}
	connectContext, cancel := context.WithCancel(ctx)
	c.state = clientStateConnecting
	c.connectCtx = connectContext
	c.connectCancel = cancel
	c.mu.Unlock()

	token := c.client.Connect()
	select {
	case <-token.Done():
		c.clearPassword()
		ack := ConnAck{}
		if connectToken, ok := token.(*pahov3.ConnectToken); ok {
			ack.SessionPresent = connectToken.SessionPresent()
			ack.ReasonCode = connectToken.ReturnCode()
		}
		if err := token.Error(); err != nil {
			c.releaseConnectContext()
			c.setDisconnected()
			return ack, fmt.Errorf("MQTT 3.1.1 connect failed: %w", err)
		}
		if err := ctx.Err(); err != nil {
			c.beginShutdown()
			<-c.closeDone
			return ack, err
		}
		c.mu.Lock()
		if c.state == clientStateClosing || c.state == clientStateClosed {
			c.mu.Unlock()
			return ack, ErrClientClosed
		}
		c.state = clientStateConnected
		c.contextStop = context.AfterFunc(ctx, c.beginShutdown)
		c.mu.Unlock()
		return ack, nil
	case <-ctx.Done():
		c.beginShutdown()
		<-c.closeDone
		c.clearPassword()
		return ConnAck{}, ctx.Err()
	}
}

// Publish waits until Paho has accepted QoS 0 or completed the QoS 1/2 exchange.
func (c *v3Client) Publish(ctx context.Context, message Publish) (PublishAck, error) {
	if err := validatePublish(message, Version311); err != nil {
		return PublishAck{}, err
	}
	if err := c.requireConnected(); err != nil {
		return PublishAck{}, err
	}
	message = clonePublish(message)
	token := c.client.Publish(message.Topic, message.QoS, message.Retain, message.Payload)
	waitErr := c.waitToken(ctx, token)
	ack := PublishAck{Accepted: waitErr == nil, Acknowledged: waitErr == nil && message.QoS > 0}
	if delivery, ok := token.(*pahov3.PublishToken); ok {
		ack.PacketID = delivery.MessageID()
		ack.PacketIDPresent = ack.PacketID != 0
	}
	if waitErr != nil {
		return ack, waitErr
	}
	return ack, nil
}

// Subscribe sends one MQTT 3.1.1 SUBSCRIBE packet and preserves request order in the result.
func (c *v3Client) Subscribe(ctx context.Context, batch SubscriptionBatch) (SubscribeAck, error) {
	if err := validateSubscriptions(batch, Version311); err != nil {
		return SubscribeAck{}, err
	}
	if err := c.requireConnected(); err != nil {
		return SubscribeAck{}, err
	}
	batch = cloneSubscriptionBatch(batch)
	filters := make(map[string]byte, len(batch.Subscriptions))
	for _, subscription := range batch.Subscriptions {
		filters[subscription.Filter] = subscription.QoS
	}
	token := c.client.SubscribeMultiple(filters, nil)
	waitErr := c.waitToken(ctx, token)
	results := make([]SubscriptionAck, len(batch.Subscriptions))
	granted := map[string]byte{}
	if subscribeToken, ok := token.(*pahov3.SubscribeToken); ok {
		granted = subscribeToken.Result()
	}
	for index, subscription := range batch.Subscriptions {
		reason, present := granted[subscription.Filter]
		result := SubscriptionAck{Filter: subscription.Filter, ReasonCode: reason, Success: present && reason < 0x80}
		if result.Success {
			qos := reason
			result.GrantedQoS = &qos
		}
		results[index] = result
	}
	ack := SubscribeAck{Results: results}
	if waitErr != nil {
		return ack, waitErr
	}
	return ack, nil
}

// Unsubscribe sends one MQTT 3.1.1 UNSUBSCRIBE packet.
func (c *v3Client) Unsubscribe(ctx context.Context, topics []string) (UnsubscribeAck, error) {
	if len(topics) == 0 {
		return UnsubscribeAck{}, ErrInvalidOperation
	}
	if err := c.requireConnected(); err != nil {
		return UnsubscribeAck{}, err
	}
	ownedTopics := append([]string(nil), topics...)
	for _, topic := range ownedTopics {
		if topic == "" {
			return UnsubscribeAck{}, ErrInvalidOperation
		}
	}
	token := c.client.Unsubscribe(ownedTopics...)
	waitErr := c.waitToken(ctx, token)
	results := make([]TopicAck, len(ownedTopics))
	for index, topic := range ownedTopics {
		results[index] = TopicAck{Topic: topic, Success: waitErr == nil}
	}
	ack := UnsubscribeAck{Results: results}
	if waitErr != nil {
		return ack, waitErr
	}
	return ack, nil
}

// Disconnect idempotently stops the Paho client and closes Events after shutdown.
func (c *v3Client) Disconnect(ctx context.Context) error {
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
func (c *v3Client) Events() <-chan Event {
	return c.sink.channel()
}

func (c *v3Client) waitToken(ctx context.Context, token pahov3.Token) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			return fmt.Errorf("MQTT 3.1.1 operation failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		c.beginShutdown()
		<-c.closeDone
		return ctx.Err()
	}
}

func (c *v3Client) requireConnected() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == clientStateClosing || c.state == clientStateClosed {
		return ErrClientClosed
	}
	if c.state != clientStateConnected || !c.client.IsConnectionOpen() {
		return ErrClientNotConnected
	}
	return nil
}

func (c *v3Client) beginShutdown() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		previousState := c.state
		c.state = clientStateClosing
		stop := c.contextStop
		c.contextStop = nil
		cancel := c.connectCancel
		c.connectCancel = nil
		connStop := c.connCtxStop
		c.connCtxStop = nil
		c.mu.Unlock()
		if stop != nil {
			stop()
		}
		if previousState == clientStateConnecting && cancel != nil {
			cancel()
		}
		go func() {
			c.client.Disconnect(v3DisconnectQuiesce)
			if cancel != nil {
				cancel()
			}
			if connStop != nil {
				connStop()
			}
			c.closeCallbacks()
			c.clearPassword()
			c.setState(clientStateClosed)
			c.sink.close()
			close(c.closeDone)
		}()
	})
}

func (c *v3Client) clearPassword() {
	c.mu.Lock()
	defer c.mu.Unlock()
	zero(c.password)
	c.password = nil
	c.config.Auth.Password = nil
}

func (c *v3Client) releaseConnectContext() {
	c.mu.Lock()
	cancel := c.connectCancel
	c.connectCancel = nil
	stop := c.connCtxStop
	c.connCtxStop = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if stop != nil {
		stop()
	}
}

func (c *v3Client) setState(state clientState) {
	c.mu.Lock()
	c.state = state
	c.mu.Unlock()
}

func (c *v3Client) setDisconnected() {
	c.mu.Lock()
	if c.state != clientStateClosing && c.state != clientStateClosed {
		c.state = clientStateDisconnected
	}
	c.mu.Unlock()
}

func (c *v3Client) onMessage(_ pahov3.Client, message pahov3.Message) {
	if !c.enterCallback() {
		return
	}
	defer c.callbackWG.Done()
	packetID := message.MessageID()
	c.sink.emit(Event{
		Kind: EventInboundMessage,
		Message: &InboundMessage{
			Topic:           message.Topic(),
			Payload:         cloneBytes(message.Payload()),
			QoS:             message.Qos(),
			Retain:          message.Retained(),
			Duplicate:       message.Duplicate(),
			PacketID:        packetID,
			PacketIDPresent: packetID != 0,
		},
	})
}

func (c *v3Client) onConnectionLost(_ pahov3.Client, err error) {
	if !c.enterCallback() {
		return
	}
	defer c.callbackWG.Done()
	c.mu.Lock()
	if c.state == clientStateClosing || c.state == clientStateClosed {
		c.mu.Unlock()
		return
	}
	c.state = clientStateDisconnected
	c.mu.Unlock()
	c.sink.emit(Event{
		Kind: EventDisconnected,
		Disconnect: &DisconnectInfo{
			Unexpected: true,
			Cause:      safeErrorMessage(err),
		},
	})
}

func (c *v3Client) enterCallback() bool {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	if c.callbacksClosed {
		return false
	}
	c.callbackWG.Add(1)
	return true
}

func (c *v3Client) closeCallbacks() {
	c.callbackMu.Lock()
	c.callbacksClosed = true
	c.callbackMu.Unlock()
	c.callbackWG.Wait()
}

func (c *v3Client) openConnection(uri *url.URL, options pahov3.ClientOptions) (net.Conn, error) {
	c.mu.Lock()
	ctx := c.connectCtx
	c.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	dialer := net.Dialer{Timeout: options.ConnectTimeout}
	connection, err := dialer.DialContext(ctx, "tcp", uri.Host)
	if err != nil {
		return nil, err
	}
	if uri.Scheme == "ssl" {
		tlsConfig := options.TLSConfig
		if tlsConfig == nil {
			_ = connection.Close()
			return nil, ErrInvalidConfig
		}
		tlsConnection := tls.Client(connection, tlsConfig.Clone())
		if err := tlsConnection.HandshakeContext(ctx); err != nil {
			_ = connection.Close()
			return nil, err
		}
		connection = tlsConnection
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	c.mu.Lock()
	c.connCtxStop = stop
	c.mu.Unlock()
	return connection, nil
}

func safeErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type clientState uint8

const (
	clientStateReady clientState = iota
	clientStateConnecting
	clientStateConnected
	clientStateDisconnected
	clientStateClosing
	clientStateClosed
)
