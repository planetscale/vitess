package drpc

import (
	"crypto/tls"
	"crypto/x509"
)

// RecommendedCiphers provides a default list of secure/modern ciphers.
var recommendedCiphers = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
}

// RecommendedCurves provides a sane list of curves with assembly implementations
// for performance and constant time to protect against side-channel attacks.
var recommendedCurves = []tls.CurveID{
	tls.CurveP521,
	tls.CurveP384,
	tls.CurveP256,
}

// ServerTLS provides available settings to enable secure TLS communications.
type ServerTLS struct {
	// Server certificate, PEM-encoded.
	Cert []byte

	// Server private key, PEM-encoded.
	PrivateKey []byte

	// List of ciphers to allow.
	SupportedCiphers []uint16

	// Server preferred curves configuration.
	PreferredCurves []tls.CurveID

	// Whether to include system CAs.
	IncludeSystemCAs bool

	// Custom certificate authorities to include when accepting TLS connections.
	CustomCAs [][]byte
}

// Generate a proper TLS configuration to use on the server.
func (opts ServerTLS) conf() (*tls.Config, error) {
	// Load key/pair
	cert, err := tls.X509KeyPair(opts.Cert, opts.PrivateKey)
	if err != nil {
		return nil, err
	}

	// Prepare cert pool
	var cp *x509.CertPool
	if opts.IncludeSystemCAs {
		cp, err = x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
	} else {
		cp = x509.NewCertPool()
	}

	// Append custom CA certs
	if len(opts.CustomCAs) > 0 {
		for _, c := range opts.CustomCAs {
			if !cp.AppendCertsFromPEM(c) {
				return nil, err
			}
		}
	}

	// Setup ciphers and curves
	if opts.SupportedCiphers == nil {
		opts.SupportedCiphers = recommendedCiphers
	}
	if opts.PreferredCurves == nil {
		opts.PreferredCurves = recommendedCurves
	}

	// Base TLS configuration
	conf := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		CipherSuites:     opts.SupportedCiphers,
		CurvePreferences: opts.PreferredCurves,
		RootCAs:          cp,
		MinVersion:       tls.VersionTLS12,
	}
	return conf, nil
}
