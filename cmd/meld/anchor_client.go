package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/crapthings/meldbase/integrations/anchorhttp"
)

type anchorReplicaFlags []string

func (values *anchorReplicaFlags) String() string { return strings.Join(*values, ",") }
func (values *anchorReplicaFlags) Set(value string) error {
	if value == "" {
		return errors.New("rollback-anchor replica specification is empty")
	}
	*values = append(*values, value)
	return nil
}

type remoteAnchorConfig struct {
	clusterID        string
	replicaSpecs     []string
	anchorName       string
	keyID            string
	keyFile          string
	serverCAFile     string
	clientCertFile   string
	clientKeyFile    string
	operationTimeout time.Duration
}

func (config remoteAnchorConfig) enabled() bool {
	return config.clusterID != "" || len(config.replicaSpecs) != 0 || config.anchorName != "" || config.keyID != "" || config.keyFile != "" || config.serverCAFile != "" || config.clientCertFile != "" || config.clientKeyFile != ""
}

func newRemoteAnchorStore(config remoteAnchorConfig) (*anchorhttp.QuorumStore, *http.Transport, error) {
	if config.clusterID == "" || len(config.replicaSpecs) == 0 || config.anchorName == "" || config.keyID == "" || config.keyFile == "" || config.serverCAFile == "" || config.clientCertFile == "" || config.clientKeyFile == "" {
		return nil, nil, errors.New("remote rollback anchor requires cluster, replica, name, key ID/file, server CA and client certificate/key")
	}
	if config.operationTimeout <= 0 {
		return nil, nil, errors.New("remote rollback-anchor timeout must be positive")
	}
	replicas := make([]anchorhttp.Replica, len(config.replicaSpecs))
	for index, specification := range config.replicaSpecs {
		memberID, endpoint, ok := strings.Cut(specification, "=")
		if !ok || memberID == "" || endpoint == "" {
			return nil, nil, errors.New("each rollback-anchor replica must be member-id=https://endpoint")
		}
		replicas[index] = anchorhttp.Replica{MemberID: memberID, Endpoint: endpoint}
	}
	keys, err := loadAnchorKeys([]string{config.keyID + "=" + config.keyFile})
	if err != nil {
		return nil, nil, err
	}
	clientCertificate, err := loadAnchorClientCertificate(config.clientCertFile, config.clientKeyFile)
	if err != nil {
		return nil, nil, err
	}
	serverRoots, err := loadAnchorClientCAs(config.serverCAFile)
	if err != nil {
		return nil, nil, err
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: min(config.operationTimeout, 5*time.Second), KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   min(config.operationTimeout, 5*time.Second),
		ResponseHeaderTimeout: config.operationTimeout,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          len(replicas) * 2,
		MaxIdleConnsPerHost:   2,
		DisableCompression:    true,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS13, RootCAs: serverRoots,
			Certificates: []tls.Certificate{clientCertificate},
		},
	}
	store, err := anchorhttp.NewQuorumStore(anchorhttp.QuorumOptions{
		ClusterID: config.clusterID, Replicas: replicas, AnchorName: config.anchorName,
		KeyID: config.keyID, SharedKey: keys[config.keyID], Client: &http.Client{Transport: transport},
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, err
	}
	return store, transport, nil
}

func loadAnchorClientCertificate(certificatePath, privateKeyPath string) (tls.Certificate, error) {
	return loadAnchorCertificateForUsage(certificatePath, privateKeyPath, x509.ExtKeyUsageClientAuth, "client")
}

func loadAnchorCertificateForUsage(certificatePath, privateKeyPath string, requiredUsage x509.ExtKeyUsage, label string) (tls.Certificate, error) {
	certificatePEM, err := readAnchorFile(certificatePath, anchorTLSFileMaximum, false)
	if err != nil {
		return tls.Certificate{}, err
	}
	privateKeyPEM, err := readAnchorFile(privateKeyPath, anchorTLSFileMaximum, true)
	if err != nil {
		return tls.Certificate{}, err
	}
	pair, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("invalid anchor TLS %s certificate/key pair: %w", label, err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("invalid anchor TLS %s leaf certificate: %w", label, err)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return tls.Certificate{}, fmt.Errorf("anchor TLS %s certificate is not currently valid", label)
	}
	usageAllowed := false
	for _, usage := range leaf.ExtKeyUsage {
		usageAllowed = usageAllowed || usage == requiredUsage
	}
	if !usageAllowed || leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return tls.Certificate{}, fmt.Errorf("anchor TLS %s certificate lacks its explicit authentication or digital-signature usage", label)
	}
	pair.Leaf = leaf
	return pair, nil
}
