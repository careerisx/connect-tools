package api

import (
	"context"
	"errors"
	"strings"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/app"
	"connect-tools/internal/profile"
	"connect-tools/internal/session"
	"connectrpc.com/connect"
)

// ProfileHandler maps ProfileService RPCs to the profile application service.
type ProfileHandler struct {
	service *app.ProfileService
}

// NewProfileHandler creates a ProfileService handler.
func NewProfileHandler(service *app.ProfileService) *ProfileHandler {
	return &ProfileHandler{service: service}
}

// ListProfiles returns one deterministic filtered profile page.
func (h *ProfileHandler) ListProfiles(ctx context.Context, request *connect.Request[connecttoolsv1.ListProfilesRequest]) (*connect.Response[connecttoolsv1.ListProfilesResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and profile service are required")
	}
	filterKinds := make(map[profile.Type]struct{})
	nameContains := ""
	if request.Msg.Filter != nil {
		nameContains = strings.ToLower(request.Msg.Filter.NameContains)
		for _, kind := range request.Msg.Filter.Kinds {
			mapped, err := profileTypeFromSessionKind(kind)
			if err != nil {
				return nil, invalidArgument(err.Error())
			}
			filterKinds[mapped] = struct{}{}
		}
	}
	values, err := h.service.List(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	filtered := make([]profile.Profile, 0, len(values))
	for _, value := range values {
		if len(filterKinds) > 0 {
			if _, ok := filterKinds[value.Type]; !ok {
				continue
			}
		}
		if nameContains != "" && !strings.Contains(strings.ToLower(value.Name), nameContains) {
			continue
		}
		filtered = append(filtered, value)
	}
	start, end, next, err := pageBounds(len(filtered), request.Msg.PageSize, request.Msg.PageToken)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	response := &connecttoolsv1.ListProfilesResponse{Profiles: make([]*connecttoolsv1.Profile, 0, end-start), NextPageToken: next}
	for _, value := range filtered[start:end] {
		mapped, err := profileToProto(value)
		if err != nil {
			return nil, rpcError(err)
		}
		response.Profiles = append(response.Profiles, mapped)
	}
	return connect.NewResponse(response), nil
}

// GetProfile returns one saved profile by ID.
func (h *ProfileHandler) GetProfile(ctx context.Context, request *connect.Request[connecttoolsv1.GetProfileRequest]) (*connect.Response[connecttoolsv1.GetProfileResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and profile service are required")
	}
	id, err := requireID(request.Msg.ProfileId, "profile ID")
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	value, err := h.service.Get(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	mapped, err := profileToProto(value)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&connecttoolsv1.GetProfileResponse{Profile: mapped}), nil
}

// CreateProfile validates and persists a new server-identified profile.
func (h *ProfileHandler) CreateProfile(ctx context.Context, request *connect.Request[connecttoolsv1.CreateProfileRequest]) (*connect.Response[connecttoolsv1.CreateProfileResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and profile service are required")
	}
	input, err := profileFromProto(request.Msg.Profile, false)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	updates := app.ProfileSecretUpdates{}
	if len(request.Msg.MqttPassword) > 0 {
		updates.Password = app.SecretUpdate{Action: app.SecretSet, Value: request.Msg.MqttPassword}
		defer clear(request.Msg.MqttPassword)
	}
	if len(request.Msg.MqttPrivateKeyPassphrase) > 0 {
		updates.PrivateKeyPassphrase = app.SecretUpdate{Action: app.SecretSet, Value: request.Msg.MqttPrivateKeyPassphrase}
		defer clear(request.Msg.MqttPrivateKeyPassphrase)
	}
	created, err := h.service.CreateWithSecrets(ctx, input, updates)
	if err != nil {
		return nil, rpcError(err)
	}
	mapped, err := profileToProto(created)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&connecttoolsv1.CreateProfileResponse{Profile: mapped}), nil
}

// UpdateProfile fully replaces mutable fields of an existing profile.
func (h *ProfileHandler) UpdateProfile(ctx context.Context, request *connect.Request[connecttoolsv1.UpdateProfileRequest]) (*connect.Response[connecttoolsv1.UpdateProfileResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and profile service are required")
	}
	input, err := profileFromProto(request.Msg.Profile, true)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	updates := app.ProfileSecretUpdates{}
	if request.Msg.ClearMqttPassword {
		updates.Password.Action = app.SecretDelete
	} else if len(request.Msg.MqttPassword) > 0 {
		updates.Password = app.SecretUpdate{Action: app.SecretSet, Value: request.Msg.MqttPassword}
		defer clear(request.Msg.MqttPassword)
	}
	if request.Msg.ClearMqttPrivateKeyPassphrase {
		updates.PrivateKeyPassphrase.Action = app.SecretDelete
	} else if len(request.Msg.MqttPrivateKeyPassphrase) > 0 {
		updates.PrivateKeyPassphrase = app.SecretUpdate{Action: app.SecretSet, Value: request.Msg.MqttPrivateKeyPassphrase}
		defer clear(request.Msg.MqttPrivateKeyPassphrase)
	}
	updated, err := h.service.UpdateWithSecrets(ctx, input, updates)
	if err != nil {
		return nil, rpcError(err)
	}
	mapped, err := profileToProto(updated)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&connecttoolsv1.UpdateProfileResponse{Profile: mapped}), nil
}

// DeleteProfile deletes one saved profile and its managed credentials.
func (h *ProfileHandler) DeleteProfile(ctx context.Context, request *connect.Request[connecttoolsv1.DeleteProfileRequest]) (*connect.Response[connecttoolsv1.DeleteProfileResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and profile service are required")
	}
	id, err := requireID(request.Msg.ProfileId, "profile ID")
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	if err := h.service.Delete(ctx, id); err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&connecttoolsv1.DeleteProfileResponse{Deleted: true}), nil
}

func profileTypeFromSessionKind(kind connecttoolsv1.SessionKind) (profile.Type, error) {
	mapped, err := sessionKindFromProto(kind)
	if err != nil {
		return "", err
	}
	switch mapped {
	case session.KindTCPClient:
		return profile.TypeTCPClient, nil
	case session.KindTCPServer:
		return profile.TypeTCPServer, nil
	case session.KindMQTTClient:
		return profile.TypeMQTTClient, nil
	default:
		return "", errors.New("TCP peers do not have profiles")
	}
}
