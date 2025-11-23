module github.com/robertodantas/lnpay/rpc

go 1.25.4

require (
	google.golang.org/grpc v1.66.0
	google.golang.org/protobuf v1.34.1
)

// If your actual repository is at github.com/7robertodantas/lnpay,
// you can use a replace directive to map the module to the actual repo:
// replace github.com/robertodantas/lnpay/rpc => github.com/7robertodantas/lnpay/rpc v0.0.0
