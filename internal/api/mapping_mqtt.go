package api

import (
	"errors"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/profile"
)

func mqttProfileFromProto(value *connecttoolsv1.MQTTProfile) (*profile.MQTTClient, error) {
	if value == nil {
		return nil, errors.New("MQTT profile is required")
	}
	version, err := mqttVersionFromProto(value.Version)
	if err != nil {
		return nil, err
	}
	result := &profile.MQTTClient{
		Version: version, Host: value.Host, Port: int(value.Port), ClientID: value.ClientId,
		Username: value.Username, PasswordRef: value.PasswordRef, KeepAliveSeconds: value.KeepAliveSeconds,
		CleanStart: value.CleanStart, SessionExpirySeconds: cloneUint32(value.SessionExpirySeconds),
	}
	if value.Tls != nil {
		result.TLS = profile.MQTTTLSConfig{
			Enabled: value.Tls.Enabled, CAFile: value.Tls.CaFile, ClientCertFile: value.Tls.ClientCertFile,
			ClientKeyFile: value.Tls.ClientKeyFile, ServerName: value.Tls.ServerName,
			PrivateKeyPassphraseRef: value.Tls.PrivateKeyPassphraseRef,
		}
	}
	if value.Will != nil {
		will, err := mqttWillFromProto(value.Will)
		if err != nil {
			return nil, err
		}
		result.Will = will
	}
	result.InitialSubscriptions = make([]profile.MQTTSubscriptionBatch, len(value.InitialSubscriptions))
	for index, batch := range value.InitialSubscriptions {
		mapped, err := mqttSubscriptionBatchFromProto(batch)
		if err != nil {
			return nil, err
		}
		result.InitialSubscriptions[index] = mapped
	}
	if value.Reconnect != nil {
		result.Reconnect = profile.ReconnectPolicy{
			Enabled: value.Reconnect.Enabled, InitialDelayMS: value.Reconnect.InitialDelayMs,
			MaxDelayMS: value.Reconnect.MaxDelayMs, Multiplier: value.Reconnect.Multiplier,
			JitterRatio: value.Reconnect.JitterRatio, MaxAttempts: value.Reconnect.MaxAttempts,
		}
	}
	return result, nil
}

func mqttProfileToProto(value profile.MQTTClient) *connecttoolsv1.MQTTProfile {
	result := &connecttoolsv1.MQTTProfile{
		Version: mqttVersionToProto(value.Version), Host: value.Host, Port: uint32(value.Port), ClientId: value.ClientID,
		Username: value.Username, PasswordRef: value.PasswordRef, KeepAliveSeconds: value.KeepAliveSeconds,
		CleanStart: value.CleanStart, SessionExpirySeconds: cloneUint32(value.SessionExpirySeconds),
		Tls: &connecttoolsv1.MQTTTLSConfig{
			Enabled: value.TLS.Enabled, CaFile: value.TLS.CAFile, ClientCertFile: value.TLS.ClientCertFile,
			ClientKeyFile: value.TLS.ClientKeyFile, ServerName: value.TLS.ServerName,
			PrivateKeyPassphraseRef: value.TLS.PrivateKeyPassphraseRef,
		},
		Reconnect: &connecttoolsv1.ReconnectPolicy{
			Enabled: value.Reconnect.Enabled, InitialDelayMs: value.Reconnect.InitialDelayMS,
			MaxDelayMs: value.Reconnect.MaxDelayMS, Multiplier: value.Reconnect.Multiplier,
			JitterRatio: value.Reconnect.JitterRatio, MaxAttempts: value.Reconnect.MaxAttempts,
		},
	}
	if value.Will != nil {
		result.Will = mqttWillToProto(*value.Will)
	}
	result.InitialSubscriptions = make([]*connecttoolsv1.MQTTSubscriptionBatch, len(value.InitialSubscriptions))
	for index, batch := range value.InitialSubscriptions {
		result.InitialSubscriptions[index] = mqttSubscriptionBatchToProto(batch)
	}
	return result
}

func mqttWillFromProto(value *connecttoolsv1.MQTTWill) (*profile.MQTTWill, error) {
	qos, err := mqttQoSFromProto(value.Qos)
	if err != nil {
		return nil, err
	}
	result := &profile.MQTTWill{Topic: value.Topic, Payload: append([]byte(nil), value.Payload...), QoS: qos, Retain: value.Retain}
	if value.Properties != nil {
		properties, err := mqttWillPropertiesFromProto(value.Properties)
		if err != nil {
			return nil, err
		}
		result.Properties = properties
	}
	return result, nil
}

func mqttWillToProto(value profile.MQTTWill) *connecttoolsv1.MQTTWill {
	result := &connecttoolsv1.MQTTWill{Topic: value.Topic, Payload: append([]byte(nil), value.Payload...), Qos: mqttQoSToProto(value.QoS), Retain: value.Retain}
	if value.Properties != nil {
		result.Properties = mqttWillPropertiesToProto(*value.Properties)
	}
	return result
}

func mqttWillPropertiesFromProto(value *connecttoolsv1.MQTTWillProperties) (*profile.MQTTWillProperties, error) {
	common, err := mqttMessagePropertiesFromFields(value.ResponseTopic, value.CorrelationData, value.ContentType, value.PayloadFormat, value.MessageExpiryIntervalSeconds, value.UserProperties)
	if err != nil {
		return nil, err
	}
	return &profile.MQTTWillProperties{MQTTMessageProperties: common, WillDelayIntervalSeconds: cloneUint32(value.WillDelayIntervalSeconds)}, nil
}

func mqttWillPropertiesToProto(value profile.MQTTWillProperties) *connecttoolsv1.MQTTWillProperties {
	result := &connecttoolsv1.MQTTWillProperties{WillDelayIntervalSeconds: cloneUint32(value.WillDelayIntervalSeconds)}
	fillMQTTMessageProperties(&result.ResponseTopic, &result.CorrelationData, &result.ContentType, &result.PayloadFormat, &result.MessageExpiryIntervalSeconds, &result.UserProperties, value.MQTTMessageProperties)
	return result
}

func mqttMessagePropertiesFromFields(responseTopic *string, correlationData []byte, contentType *string, payloadFormat *connecttoolsv1.MQTTPayloadFormat, expiry *uint32, userProperties []*connecttoolsv1.MQTTUserProperty) (profile.MQTTMessageProperties, error) {
	result := profile.MQTTMessageProperties{
		ResponseTopic: cloneString(responseTopic), CorrelationData: append([]byte(nil), correlationData...),
		ContentType: cloneString(contentType), MessageExpiryIntervalSeconds: cloneUint32(expiry),
		UserProperties: mqttUserPropertiesFromProto(userProperties),
	}
	if payloadFormat != nil {
		mapped, err := mqttPayloadFormatFromProto(*payloadFormat)
		if err != nil {
			return profile.MQTTMessageProperties{}, err
		}
		result.PayloadFormat = &mapped
	}
	return result, nil
}

func fillMQTTMessageProperties(responseTopic **string, correlationData *[]byte, contentType **string, payloadFormat **connecttoolsv1.MQTTPayloadFormat, expiry **uint32, userProperties *[]*connecttoolsv1.MQTTUserProperty, value profile.MQTTMessageProperties) {
	*responseTopic = cloneString(value.ResponseTopic)
	*correlationData = append([]byte(nil), value.CorrelationData...)
	*contentType = cloneString(value.ContentType)
	if value.PayloadFormat != nil {
		mapped := mqttPayloadFormatToProto(*value.PayloadFormat)
		*payloadFormat = &mapped
	}
	*expiry = cloneUint32(value.MessageExpiryIntervalSeconds)
	*userProperties = mqttUserPropertiesToProto(value.UserProperties)
}

func mqttSubscriptionBatchFromProto(value *connecttoolsv1.MQTTSubscriptionBatch) (profile.MQTTSubscriptionBatch, error) {
	if value == nil {
		return profile.MQTTSubscriptionBatch{}, errors.New("MQTT subscription batch is required")
	}
	result := profile.MQTTSubscriptionBatch{SubscriptionIdentifier: cloneUint32(value.SubscriptionIdentifier), UserProperties: mqttUserPropertiesFromProto(value.UserProperties)}
	result.Subscriptions = make([]profile.MQTTSubscription, len(value.Subscriptions))
	for index, item := range value.Subscriptions {
		if item == nil {
			return profile.MQTTSubscriptionBatch{}, errors.New("MQTT subscription is required")
		}
		qos, err := mqttQoSFromProto(item.Qos)
		if err != nil {
			return profile.MQTTSubscriptionBatch{}, err
		}
		retainHandling, err := mqttRetainHandlingFromProto(item.RetainHandling)
		if err != nil {
			return profile.MQTTSubscriptionBatch{}, err
		}
		result.Subscriptions[index] = profile.MQTTSubscription{
			Filter: item.Filter, QoS: qos, NoLocal: item.NoLocal, RetainAsPublished: item.RetainAsPublished, RetainHandling: retainHandling,
		}
	}
	return result, nil
}

func mqttSubscriptionBatchToProto(value profile.MQTTSubscriptionBatch) *connecttoolsv1.MQTTSubscriptionBatch {
	result := &connecttoolsv1.MQTTSubscriptionBatch{SubscriptionIdentifier: cloneUint32(value.SubscriptionIdentifier), UserProperties: mqttUserPropertiesToProto(value.UserProperties)}
	result.Subscriptions = make([]*connecttoolsv1.MQTTSubscriptionOptions, len(value.Subscriptions))
	for index, item := range value.Subscriptions {
		result.Subscriptions[index] = &connecttoolsv1.MQTTSubscriptionOptions{
			Filter: item.Filter, Qos: mqttQoSToProto(item.QoS), NoLocal: item.NoLocal,
			RetainAsPublished: item.RetainAsPublished, RetainHandling: mqttRetainHandlingToProto(item.RetainHandling),
		}
	}
	return result
}

func mqttVersionFromProto(value connecttoolsv1.MQTTVersion) (profile.MQTTVersion, error) {
	switch value {
	case connecttoolsv1.MQTTVersion_MQTT_VERSION_3_1_1:
		return profile.MQTTVersion311, nil
	case connecttoolsv1.MQTTVersion_MQTT_VERSION_5_0:
		return profile.MQTTVersion5, nil
	default:
		return "", errors.New("MQTT version is required")
	}
}

func mqttVersionToProto(value profile.MQTTVersion) connecttoolsv1.MQTTVersion {
	if value == profile.MQTTVersion311 {
		return connecttoolsv1.MQTTVersion_MQTT_VERSION_3_1_1
	}
	if value == profile.MQTTVersion5 {
		return connecttoolsv1.MQTTVersion_MQTT_VERSION_5_0
	}
	return connecttoolsv1.MQTTVersion_MQTT_VERSION_UNSPECIFIED
}

func mqttQoSFromProto(value connecttoolsv1.MQTTQoS) (profile.MQTTQoS, error) {
	switch value {
	case connecttoolsv1.MQTTQoS_MQTT_QOS_AT_MOST_ONCE:
		return profile.MQTTQoSAtMostOnce, nil
	case connecttoolsv1.MQTTQoS_MQTT_QOS_AT_LEAST_ONCE:
		return profile.MQTTQoSAtLeastOnce, nil
	case connecttoolsv1.MQTTQoS_MQTT_QOS_EXACTLY_ONCE:
		return profile.MQTTQoSExactlyOnce, nil
	default:
		return 0, errors.New("MQTT QoS is required")
	}
}

func mqttQoSToProto(value profile.MQTTQoS) connecttoolsv1.MQTTQoS {
	switch value {
	case profile.MQTTQoSAtMostOnce:
		return connecttoolsv1.MQTTQoS_MQTT_QOS_AT_MOST_ONCE
	case profile.MQTTQoSAtLeastOnce:
		return connecttoolsv1.MQTTQoS_MQTT_QOS_AT_LEAST_ONCE
	case profile.MQTTQoSExactlyOnce:
		return connecttoolsv1.MQTTQoS_MQTT_QOS_EXACTLY_ONCE
	default:
		return connecttoolsv1.MQTTQoS_MQTT_QOS_UNSPECIFIED
	}
}

func mqttPayloadFormatFromProto(value connecttoolsv1.MQTTPayloadFormat) (profile.MQTTPayloadFormat, error) {
	switch value {
	case connecttoolsv1.MQTTPayloadFormat_MQTT_PAYLOAD_FORMAT_BYTES:
		return profile.MQTTPayloadFormatBytes, nil
	case connecttoolsv1.MQTTPayloadFormat_MQTT_PAYLOAD_FORMAT_UTF8:
		return profile.MQTTPayloadFormatUTF8, nil
	default:
		return "", errors.New("MQTT payload format is invalid")
	}
}

func mqttPayloadFormatToProto(value profile.MQTTPayloadFormat) connecttoolsv1.MQTTPayloadFormat {
	if value == profile.MQTTPayloadFormatUTF8 {
		return connecttoolsv1.MQTTPayloadFormat_MQTT_PAYLOAD_FORMAT_UTF8
	}
	if value == profile.MQTTPayloadFormatBytes {
		return connecttoolsv1.MQTTPayloadFormat_MQTT_PAYLOAD_FORMAT_BYTES
	}
	return connecttoolsv1.MQTTPayloadFormat_MQTT_PAYLOAD_FORMAT_UNSPECIFIED
}

func mqttRetainHandlingFromProto(value connecttoolsv1.MQTTRetainHandling) (profile.MQTTRetainHandling, error) {
	switch value {
	case connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_UNSPECIFIED:
		return "", nil
	case connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_SEND_AT_SUBSCRIBE:
		return profile.MQTTRetainSendAtSubscribe, nil
	case connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_SEND_IF_NEW:
		return profile.MQTTRetainSendIfNew, nil
	case connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_DO_NOT_SEND:
		return profile.MQTTRetainDoNotSend, nil
	default:
		return "", errors.New("MQTT retain handling is invalid")
	}
}

func mqttRetainHandlingToProto(value profile.MQTTRetainHandling) connecttoolsv1.MQTTRetainHandling {
	switch value {
	case profile.MQTTRetainSendAtSubscribe:
		return connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_SEND_AT_SUBSCRIBE
	case profile.MQTTRetainSendIfNew:
		return connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_SEND_IF_NEW
	case profile.MQTTRetainDoNotSend:
		return connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_DO_NOT_SEND
	default:
		return connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_UNSPECIFIED
	}
}

func mqttUserPropertiesFromProto(values []*connecttoolsv1.MQTTUserProperty) []profile.MQTTUserProperty {
	result := make([]profile.MQTTUserProperty, 0, len(values))
	for _, value := range values {
		if value != nil {
			result = append(result, profile.MQTTUserProperty{Name: value.Name, Value: value.Value})
		}
	}
	return result
}

func mqttUserPropertiesToProto(values []profile.MQTTUserProperty) []*connecttoolsv1.MQTTUserProperty {
	result := make([]*connecttoolsv1.MQTTUserProperty, len(values))
	for index, value := range values {
		result[index] = &connecttoolsv1.MQTTUserProperty{Name: value.Name, Value: value.Value}
	}
	return result
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
