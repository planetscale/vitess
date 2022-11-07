package drpc

import (
	"crypto/tls"
	"crypto/x509"
)

// ClientTLS defines the configuration options available when establishing
// a secure communication channel with a server.
type ClientTLS struct {
	// Whether to include system CAs.
	IncludeSystemCAs bool

	// Custom certificate authorities to include when accepting TLS connections.
	CustomCAs [][]byte

	// Name used to verify the hostname on the returned certificates.
	ServerName string

	// Don't verify the server name on the certificate when establishing a secure
	// TLS channel. THIS IS HIGHLY DANGEROUS, INTENDED FOR TESTING/DEV ONLY.
	SkipVerify bool
}

// Generate a proper TLS configuration to use on the client side.
func (opts ClientTLS) conf() (*tls.Config, error) {
	conf := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Prepare cert pool
	var err error
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

	conf.RootCAs = cp
	conf.ServerName = opts.ServerName
	conf.InsecureSkipVerify = opts.SkipVerify
	return conf, nil
}
