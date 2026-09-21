package api

import (
	"context"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/app"
	"connect-tools/internal/convert"
	"connect-tools/internal/mqtt"
	"connect-tools/internal/profile"
	"connect-tools/internal/records"
	"connectrpc.com/connect"
)

type MQTTHandler struct{ service *app.MQTTService }

func NewMQTTHandler(service *app.MQTTService) *MQTTHandler { return &MQTTHandler{service: service} }

func (h *MQTTHandler) Publish(ctx context.Context, request *connect.Request[connecttoolsv1.PublishRequest]) (*connect.Response[connecttoolsv1.PublishResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and MQTT service are required")
	}
	payloadInput, err := payloadFromProto(request.Msg.Payload)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	payload, err := convert.Decode(payloadInput)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	if len(payload) > mqtt.MaxPayloadBytes {
		return nil, connect.NewError(connect.CodeResourceExhausted, mqtt.ErrInvalidOperation)
	}
	qos, err := mqttQoSByte(request.Msg.Qos)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	properties, err := mqttPublishPropertiesFromProto(request.Msg.Properties)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	actor := records.ActorCLI
	if credential, ok := CredentialFromContext(ctx); ok && credential == CredentialBrowserSession {
		actor = records.ActorUser
	}
	ack, err := h.service.Publish(ctx, request.Msg.SessionId, mqtt.Publish{Topic: request.Msg.Topic, Payload: payload, QoS: qos, Retain: request.Msg.Retain, Properties: properties}, actor)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &connecttoolsv1.PublishResponse{Accepted: ack.Accepted, Acknowledged: ack.Acknowledged, ReasonMessage: ack.ReasonString}
	if ack.PacketIDPresent {
		value := uint32(ack.PacketID)
		response.PacketId = &value
	}
	if ack.ReasonCode != nil {
		value := uint32(*ack.ReasonCode)
		response.ReasonCode = &value
	}
	return connect.NewResponse(response), nil
}

func (h *MQTTHandler) Subscribe(ctx context.Context, request *connect.Request[connecttoolsv1.SubscribeRequest]) (*connect.Response[connecttoolsv1.SubscribeResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil || len(request.Msg.Subscriptions) == 0 {
		return nil, invalidArgument("at least one MQTT subscription is required")
	}
	batch := mqtt.SubscriptionBatch{SubscriptionIdentifier: cloneUint32(request.Msg.SubscriptionIdentifier)}
	for _, item := range request.Msg.UserProperties {
		if item != nil {
			batch.UserProperties = append(batch.UserProperties, mqtt.UserProperty{Name: item.Name, Value: item.Value})
		}
	}
	for _, item := range request.Msg.Subscriptions {
		if item == nil {
			return nil, invalidArgument("subscription is required")
		}
		qos, err := mqttQoSByte(item.Qos)
		if err != nil {
			return nil, invalidArgument(err.Error())
		}
		batch.Subscriptions = append(batch.Subscriptions, mqtt.Subscription{Filter: item.Filter, QoS: qos, NoLocal: item.NoLocal, RetainAsPublished: item.RetainAsPublished, RetainHandling: mqttRetainHandlingByte(item.RetainHandling)})
	}
	ack, err := h.service.Subscribe(ctx, request.Msg.SessionId, batch)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &connecttoolsv1.SubscribeResponse{}
	for index, result := range ack.Results {
		item := &connecttoolsv1.MQTTSubscribeResult{Filter: result.Filter, Accepted: result.Success, ReasonMessage: ack.ReasonString}
		code := uint32(result.ReasonCode)
		item.ReasonCode = &code
		if index < len(request.Msg.Subscriptions) {
			item.Subscription = &connecttoolsv1.MQTTSubscription{Options: request.Msg.Subscriptions[index], Desired: true, Acknowledged: result.Success, ReasonCode: &code, FailureReason: ack.ReasonString, SubscriptionIdentifier: cloneUint32(request.Msg.SubscriptionIdentifier), UserProperties: request.Msg.UserProperties}
		}
		response.Results = append(response.Results, item)
	}
	return connect.NewResponse(response), nil
}

func (h *MQTTHandler) Unsubscribe(ctx context.Context, request *connect.Request[connecttoolsv1.UnsubscribeRequest]) (*connect.Response[connecttoolsv1.UnsubscribeResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil || len(request.Msg.Filters) == 0 {
		return nil, invalidArgument("at least one MQTT filter is required")
	}
	ack, err := h.service.Unsubscribe(ctx, request.Msg.SessionId, request.Msg.Filters)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &connecttoolsv1.UnsubscribeResponse{}
	for _, result := range ack.Results {
		item := &connecttoolsv1.MQTTUnsubscribeResult{Filter: result.Topic, Accepted: result.Success, Acknowledged: result.Success, ReasonMessage: ack.ReasonString}
		if result.ReasonCode != nil {
			code := uint32(*result.ReasonCode)
			item.ReasonCode = &code
		}
		response.Results = append(response.Results, item)
	}
	return connect.NewResponse(response), nil
}

func (h *MQTTHandler) ListSubscriptions(ctx context.Context, request *connect.Request[connecttoolsv1.ListSubscriptionsRequest]) (*connect.Response[connecttoolsv1.ListSubscriptionsResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and MQTT service are required")
	}
	states, err := h.service.ListSubscriptions(request.Msg.SessionId)
	if err != nil {
		return nil, rpcError(err)
	}
	start, end, next, err := pageBounds(len(states), request.Msg.PageSize, request.Msg.PageToken)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	response := &connecttoolsv1.ListSubscriptionsResponse{NextPageToken: next}
	for _, state := range states[start:end] {
		for _, item := range state.Batch.Subscriptions {
			mapped := &connecttoolsv1.MQTTSubscription{Options: &connecttoolsv1.MQTTSubscriptionOptions{Filter: item.Filter, Qos: mqttQoSProto(item.QoS), NoLocal: item.NoLocal, RetainAsPublished: item.RetainAsPublished, RetainHandling: mqttRetainHandlingProto(item.RetainHandling)}, Desired: true, Acknowledged: state.Acknowledged, FailureReason: state.LastError, SubscriptionIdentifier: cloneUint32(state.Batch.SubscriptionIdentifier)}
			for _, property := range state.Batch.UserProperties {
				mapped.UserProperties = append(mapped.UserProperties, &connecttoolsv1.MQTTUserProperty{Name: property.Name, Value: property.Value})
			}
			response.Subscriptions = append(response.Subscriptions, mapped)
		}
	}
	return connect.NewResponse(response), nil
}

func mqttPublishPropertiesFromProto(value *connecttoolsv1.MQTTPublishProperties) (*mqtt.MessageProperties, error) {
	if value == nil {
		return nil, nil
	}
	result := &mqtt.MessageProperties{ResponseTopic: cloneString(value.ResponseTopic), CorrelationData: append([]byte(nil), value.CorrelationData...), ContentType: cloneString(value.ContentType), MessageExpiryInterval: cloneUint32(value.MessageExpiryIntervalSeconds)}
	if value.PayloadFormat != nil {
		mapped := byte(0)
		switch *value.PayloadFormat {
		case connecttoolsv1.MQTTPayloadFormat_MQTT_PAYLOAD_FORMAT_BYTES:
		case connecttoolsv1.MQTTPayloadFormat_MQTT_PAYLOAD_FORMAT_UTF8:
			mapped = 1
		default:
			return nil, profile.ErrInvalidMQTTProperties
		}
		result.PayloadFormat = &mapped
	}
	for _, item := range value.UserProperties {
		if item != nil {
			result.UserProperties = append(result.UserProperties, mqtt.UserProperty{Name: item.Name, Value: item.Value})
		}
	}
	return result, nil
}

func mqttQoSByte(value connecttoolsv1.MQTTQoS) (byte, error) {
	mapped, err := mqttQoSFromProto(value)
	return byte(mapped), err
}

func mqttQoSProto(value byte) connecttoolsv1.MQTTQoS { return mqttQoSToProto(profile.MQTTQoS(value)) }

func mqttRetainHandlingByte(value connecttoolsv1.MQTTRetainHandling) byte {
	switch value {
	case connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_SEND_IF_NEW:
		return 1
	case connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_DO_NOT_SEND:
		return 2
	default:
		return 0
	}
}

func mqttRetainHandlingProto(value byte) connecttoolsv1.MQTTRetainHandling {
	switch value {
	case 1:
		return connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_SEND_IF_NEW
	case 2:
		return connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_DO_NOT_SEND
	default:
		return connecttoolsv1.MQTTRetainHandling_MQTT_RETAIN_HANDLING_SEND_AT_SUBSCRIBE
	}
}
