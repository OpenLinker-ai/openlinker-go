module native-llm-worker

go 1.25.0

require (
	github.com/go-kratos/blades v0.5.0
	github.com/go-kratos/blades/contrib/openai v0.3.0
)

require github.com/OpenLinker-ai/openlinker-go v0.1.4 // indirect

require (
	github.com/OpenLinker-ai/openlinker-go/contrib/blades v0.0.0
	github.com/go-kratos/kit v0.0.0-20251121083925-65298ad2aa44 // indirect
	github.com/google/jsonschema-go v0.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/openai/openai-go/v3 v3.8.1 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/OpenLinker-ai/openlinker-go v0.1.4 => ../../..

replace github.com/OpenLinker-ai/openlinker-go/contrib/blades => ../../../contrib/blades
