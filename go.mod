module connect-tools

go 1.27

require (
	connectrpc.com/connect v1.21.0
	github.com/eclipse/paho.golang v0.23.0
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/gorilla/websocket v1.5.3
	github.com/spf13/cobra v1.10.2
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78
	github.com/zalando/go-keyring v0.2.8
	golang.org/x/text v0.41.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/danieljoos/wincred v1.2.3 // indirect
	github.com/godbus/dbus/v5 v5.2.2 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	golang.org/x/crypto v0.42.0 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
)

tool (
	connectrpc.com/connect/cmd/protoc-gen-connect-go
	google.golang.org/protobuf/cmd/protoc-gen-go
)
