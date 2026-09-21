package mqtt

// Factory creates the adapter selected by Config.Version.
type Factory struct{}

// NewFactory returns a protocol-neutral MQTT client factory.
func NewFactory() Factory {
	return Factory{}
}

// New validates config and creates an MQTT 3.1.1 or MQTT 5 adapter.
func (Factory) New(config Config) (Client, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	switch config.Version {
	case Version311:
		return newV3Client(config), nil
	case Version5:
		return newV5Client(config), nil
	default:
		return nil, ErrUnsupportedVersion
	}
}
