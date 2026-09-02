module github.com/OpenLinker-ai/openlinker-go/example

go 1.25.0

require (
	github.com/OpenLinker-ai/openlinker-go v0.0.0
	github.com/gorilla/websocket v1.5.3
	google.golang.org/grpc v1.83.1
)

require (
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/OpenLinker-ai/openlinker-go => ../
