package gateway

import (
	"crypto/tls"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	dialOpts  []grpc.DialOption
	muxOpts   []runtime.ServeMuxOption
	secure    bool
	tlsConfig *tls.Config
}

// Option configures the gateway handler.
type Option func(*options)

// WithDialOptions appends additional gRPC dial options.
// Only used with NewHandler (not NewInProcessHandler).
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOpts = append(o.dialOpts, opts...)
	}
}

// WithMuxOptions sets the ServeMux options for the gateway.
func WithMuxOptions(opts ...runtime.ServeMuxOption) Option {
	return func(o *options) {
		o.muxOpts = append(o.muxOpts, opts...)
	}
}

// WithTLS enables TLS for the gRPC connection to the backend.
// If config is nil, uses system default TLS config.
func WithTLS(config *tls.Config) Option {
	return func(o *options) {
		o.secure = true
		o.tlsConfig = config
	}
}

// WithInsecure explicitly enables insecure connections (no TLS).
// This should only be used for development/testing.
func WithInsecure() Option {
	return func(o *options) {
		o.secure = false
		o.tlsConfig = nil
	}
}

// buildDialOpts constructs the gRPC dial options from configuration.
func (o *options) buildDialOpts() []grpc.DialOption {
	opts := make([]grpc.DialOption, 0, len(o.dialOpts)+1)

	if o.secure {
		if o.tlsConfig != nil {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(o.tlsConfig)))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
		}
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	opts = append(opts, o.dialOpts...)

	return opts
}
