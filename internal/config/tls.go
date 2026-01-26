package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// SetupTLSConfig creates a *tls.Config for a TLS client or server.
// Set Server=true for mTLS server config, false for client config.
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}

	// Load cert/key pair to prove this entity's identity to peers.
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}

	// Load CA to verify peer certificates.
	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM(b)
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %q", cfg.CAFile)
		}

		if cfg.Server {
			// Server: verify client certs using CA.
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			// Client: verify server cert using CA.
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}

// TLSConfig holds paths and options for TLS configuration.
type TLSConfig struct {
	CertFile      string // Path to PEM-encoded certificate
	KeyFile       string // Path to PEM-encoded private key
	CAFile        string // Path to CA certificate for peer verification
	ServerAddress string // Expected server hostname (client only)
	Server        bool   // True for server config (enables mTLS), false for client
}
