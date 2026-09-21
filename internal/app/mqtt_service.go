package app

import (
	"context"
	"errors"
	"time"

	"connect-tools/internal/mqtt"
	"connect-tools/internal/profile"
	"connect-tools/internal/records"
	"connect-tools/internal/session"
)

var ErrProfileNotMQTT = errors.New("profile is not an MQTT profile")

type MQTTService struct {
	profiles profile.Store
	sessions *SessionService
	runtime  *mqtt.Runtime
}

func NewMQTTService(profiles profile.Store, sessions *SessionService, runtime *mqtt.Runtime) *MQTTService {
	return &MQTTService{profiles: profiles, sessions: sessions, runtime: runtime}
}

func (s *MQTTService) Start(ctx context.Context, profileID, displayName string) (session.Session, error) {
	saved, err := s.profiles.Get(ctx, profileID)
	if err != nil {
		return session.Session{}, err
	}
	if saved.Type != profile.TypeMQTTClient || saved.MQTT == nil {
		return session.Session{}, ErrProfileNotMQTT
	}
	if err := saved.Validate(); err != nil {
		return session.Session{}, err
	}
	created, err := s.sessions.Create(ctx, profileID, displayName)
	if err != nil {
		return session.Session{}, err
	}
	return s.runtime.Start(ctx, created.ID, mqttRuntimeConfig(*saved.MQTT))
}

func (s *MQTTService) Stop(ctx context.Context, sessionID string) (session.Session, error) {
	return s.runtime.Stop(ctx, sessionID)
}

func (s *MQTTService) Publish(ctx context.Context, sessionID string, message mqtt.Publish, actor records.Actor) (mqtt.PublishAck, error) {
	return s.runtime.Publish(ctx, sessionID, message, actor)
}

func (s *MQTTService) Subscribe(ctx context.Context, sessionID string, batch mqtt.SubscriptionBatch) (mqtt.SubscribeAck, error) {
	return s.runtime.Subscribe(ctx, sessionID, batch)
}

func (s *MQTTService) Unsubscribe(ctx context.Context, sessionID string, filters []string) (mqtt.UnsubscribeAck, error) {
	return s.runtime.Unsubscribe(ctx, sessionID, filters)
}

func (s *MQTTService) ListSubscriptions(sessionID string) ([]mqtt.SubscriptionState, error) {
	return s.runtime.ListSubscriptions(sessionID)
}

func mqttRuntimeConfig(saved profile.MQTTClient) mqtt.RuntimeConfig {
	config := mqtt.RuntimeConfig{
		Adapter:     mqtt.Config{Version: mqttVersion(saved.Version), Host: saved.Host, Port: saved.Port, ClientID: saved.ClientID, CleanStart: saved.CleanStart, KeepAlive: time.Duration(saved.KeepAliveSeconds) * time.Second, SessionExpiryInterval: saved.SessionExpirySeconds, Auth: mqtt.Auth{Username: saved.Username}},
		PasswordRef: saved.PasswordRef,
		TLSProfile:  mqtt.TLSProfile{Enabled: saved.TLS.Enabled, BrokerHost: saved.Host, CAFile: saved.TLS.CAFile, ClientCertFile: saved.TLS.ClientCertFile, ClientKeyFile: saved.TLS.ClientKeyFile, ServerName: saved.TLS.ServerName, PrivateKeyPassphraseRef: saved.TLS.PrivateKeyPassphraseRef},
		Reconnect:   mqtt.ReconnectPolicy{Enabled: saved.Reconnect.Enabled, InitialDelay: time.Duration(saved.Reconnect.InitialDelayMS) * time.Millisecond, MaxDelay: time.Duration(saved.Reconnect.MaxDelayMS) * time.Millisecond, Multiplier: saved.Reconnect.Multiplier, JitterRatio: saved.Reconnect.JitterRatio, MaxAttempts: saved.Reconnect.MaxAttempts},
	}
	if saved.Will != nil {
		config.Adapter.Will = &mqtt.Will{Topic: saved.Will.Topic, Payload: append([]byte(nil), saved.Will.Payload...), QoS: byte(saved.Will.QoS), Retain: saved.Will.Retain, Properties: mqttWillProperties(saved.Will.Properties)}
	}
	for _, batch := range saved.InitialSubscriptions {
		config.InitialSubscriptions = append(config.InitialSubscriptions, mqttSubscriptionBatch(batch))
	}
	return config
}

func mqttVersion(value profile.MQTTVersion) mqtt.Version {
	if value == profile.MQTTVersion5 {
		return mqtt.Version5
	}
	return mqtt.Version311
}

func mqttSubscriptionBatch(value profile.MQTTSubscriptionBatch) mqtt.SubscriptionBatch {
	result := mqtt.SubscriptionBatch{SubscriptionIdentifier: value.SubscriptionIdentifier}
	for _, item := range value.Subscriptions {
		result.Subscriptions = append(result.Subscriptions, mqtt.Subscription{Filter: item.Filter, QoS: byte(item.QoS), NoLocal: item.NoLocal, RetainAsPublished: item.RetainAsPublished, RetainHandling: mqttRetainHandling(item.RetainHandling)})
	}
	for _, item := range value.UserProperties {
		result.UserProperties = append(result.UserProperties, mqtt.UserProperty{Name: item.Name, Value: item.Value})
	}
	return result
}

func mqttRetainHandling(value profile.MQTTRetainHandling) byte {
	switch value {
	case profile.MQTTRetainSendIfNew:
		return 1
	case profile.MQTTRetainDoNotSend:
		return 2
	default:
		return 0
	}
}

func mqttWillProperties(value *profile.MQTTWillProperties) *mqtt.WillProperties {
	if value == nil {
		return nil
	}
	result := &mqtt.WillProperties{MessageProperties: *mqttMessageProperties(&value.MQTTMessageProperties), WillDelayInterval: value.WillDelayIntervalSeconds}
	return result
}

func mqttMessageProperties(value *profile.MQTTMessageProperties) *mqtt.MessageProperties {
	if value == nil {
		return nil
	}
	result := &mqtt.MessageProperties{ResponseTopic: value.ResponseTopic, CorrelationData: append([]byte(nil), value.CorrelationData...), ContentType: value.ContentType, MessageExpiryInterval: value.MessageExpiryIntervalSeconds}
	if value.PayloadFormat != nil {
		mapped := byte(0)
		if *value.PayloadFormat == profile.MQTTPayloadFormatUTF8 {
			mapped = 1
		}
		result.PayloadFormat = &mapped
	}
	for _, item := range value.UserProperties {
		result.UserProperties = append(result.UserProperties, mqtt.UserProperty{Name: item.Name, Value: item.Value})
	}
	return result
}
