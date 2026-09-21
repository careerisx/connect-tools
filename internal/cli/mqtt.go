package cli

import (
	"strings"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connectrpc.com/connect"
	"github.com/spf13/cobra"
)

func newMQTTCommand(opts *runtimeOptions) *cobra.Command {
	command := &cobra.Command{Use: "mqtt", Short: "Publish and manage MQTT subscriptions", Args: noArgs}
	command.AddCommand(newMQTTPublishCommand(opts), newMQTTSubscribeCommand(opts), newMQTTUnsubscribeCommand(opts), newMQTTSubscriptionsCommand(opts))
	return command
}

func newMQTTPublishCommand(opts *runtimeOptions) *cobra.Command {
	var flags payloadFlags
	var topic string
	var qos uint32
	var retain bool
	command := &cobra.Command{Use: "publish SESSION_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if topic == "" || strings.ContainsAny(topic, "+#") {
			return usageErrorf("publish topic must be non-empty and cannot contain wildcards")
		}
		qosValue, err := mqttQoSFlag(qos)
		if err != nil {
			return err
		}
		payload, err := payloadInputFromFlags(cmd.Context(), cmd.Flags(), opts, flags)
		if err != nil {
			return err
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.mqtt.Publish(cmd.Context(), connect.NewRequest(&connecttoolsv1.PublishRequest{SessionId: args[0], Topic: topic, Payload: payload, Qos: qosValue, Retain: retain}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	bindPayloadFlags(command, &flags)
	command.Flags().StringVar(&topic, "topic", "", "publish topic")
	command.Flags().Uint32Var(&qos, "qos", 0, "QoS 0, 1, or 2")
	command.Flags().BoolVar(&retain, "retain", false, "set retain flag")
	_ = command.MarkFlagRequired("topic")
	return command
}

func newMQTTSubscribeCommand(opts *runtimeOptions) *cobra.Command {
	var filter string
	var qos uint32
	command := &cobra.Command{Use: "subscribe SESSION_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if filter == "" {
			return usageErrorf("--filter is required")
		}
		qosValue, err := mqttQoSFlag(qos)
		if err != nil {
			return err
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.mqtt.Subscribe(cmd.Context(), connect.NewRequest(&connecttoolsv1.SubscribeRequest{SessionId: args[0], Subscriptions: []*connecttoolsv1.MQTTSubscriptionOptions{{Filter: filter, Qos: qosValue}}}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	command.Flags().StringVar(&filter, "filter", "", "topic filter")
	command.Flags().Uint32Var(&qos, "qos", 0, "QoS 0, 1, or 2")
	return command
}

func newMQTTUnsubscribeCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{Use: "unsubscribe SESSION_ID FILTER [FILTER...]", Args: cobra.MinimumNArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.mqtt.Unsubscribe(cmd.Context(), connect.NewRequest(&connecttoolsv1.UnsubscribeRequest{SessionId: args[0], Filters: args[1:]}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
}

func newMQTTSubscriptionsCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{Use: "subscriptions SESSION_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.mqtt.ListSubscriptions(cmd.Context(), connect.NewRequest(&connecttoolsv1.ListSubscriptionsRequest{SessionId: args[0], PageSize: 500}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
}

func mqttQoSFlag(value uint32) (connecttoolsv1.MQTTQoS, error) {
	switch value {
	case 0:
		return connecttoolsv1.MQTTQoS_MQTT_QOS_AT_MOST_ONCE, nil
	case 1:
		return connecttoolsv1.MQTTQoS_MQTT_QOS_AT_LEAST_ONCE, nil
	case 2:
		return connecttoolsv1.MQTTQoS_MQTT_QOS_EXACTLY_ONCE, nil
	default:
		return 0, usageErrorf("qos must be 0, 1, or 2")
	}
}
