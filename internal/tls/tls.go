// Package tls issues the self-signed certificate that DolphinDB uses for its
// native HTTPS endpoint.
//
// # Why self-signed
//
// DolphinDB terminates TLS itself, reading `server.crt` and `serverPrivate.key`
// from the node's `<home>/keys` directory. That makes it the only HTTPS option
// that works without a registered domain:
//
//   - AWS ACM certificates require a domain you control, and the account-level
//     SCP denies `iam:UploadServerCertificate`, so a self-signed certificate
//     cannot be attached to an AWS load balancer either;
//   - CloudFront can serve HTTPS without a domain, but it does not support
//     WebSocket, and the DolphinDB web console needs it.
//
// The generated certificate mirrors what `openssl req -x509` produces - a
// self-signed certificate that is also its own CA - so the user can silence the
// browser warning by importing it as a trusted authority, which is exactly the
// procedure the DolphinDB documentation describes.
package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sort"
	"strings"
	"time"
)

// DefaultValidityDays matches the 825-day maximum that Apple and Chrome accept
// for a newly issued certificate.
const DefaultValidityDays = 825

// Material is a PEM encoded keypair.
type Material struct {
	CertPEM []byte
	KeyPEM  []byte
}

// DesiredHosts builds the SAN list for a cluster, sorted and de-duplicated so
// that the resulting hash is stable.
//
// publicName is the domain DolphinDB advertises and the one users type in the
// browser; the extra entries make the certificate usable from inside the
// cluster as well (health probes, the operator, other nodes).
func DesiredHosts(publicName string, extra ...string) []string {
	set := map[string]struct{}{"localhost": {}, "127.0.0.1": {}}
	if h := strings.TrimSpace(publicName); h != "" {
		set[h] = struct{}{}
	}
	for _, h := range extra {
		h = strings.TrimSpace(h)
		if h != "" {
			set[h] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for h := range set {
		// A trailing dot is a valid FQDN but would produce a distinct hash.
		out = append(out, strings.TrimSuffix(h, "."))
	}
	sort.Strings(out)
	return out
}

// HostsHash returns a short stable digest of a host set. It is stored on the
// certificate Secret and on the pod template so the operator can tell when the
// certificate must be reissued and the nodes rolled.
func HostsHash(hosts []string) string {
	sorted := append([]string(nil), hosts...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:])[:16]
}

// Generate issues a self-signed certificate valid for the given hosts.
//
// The certificate is a CA (`IsCA` + `KeyUsageCertSign`) so that importing it
// into a trust store is meaningful, and it carries `ServerAuth` so that it is
// accepted as a server certificate.
func Generate(hosts []string, validityDays int) (*Material, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("no hosts supplied for certificate")
	}
	if validityDays <= 0 {
		validityDays = DefaultValidityDays
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}

	// Backdate slightly: a client whose clock is a few minutes behind must still
	// accept the certificate.
	now := time.Now()
	tmpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName(hosts), Organization: []string{"DolphinDB Operator"}},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(0, 0, validityDays),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
			continue
		}
		tmpl.DNSNames = append(tmpl.DNSNames, h)
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal key: %w", err)
	}

	return &Material{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// commonName picks a meaningful subject for the certificate. The host list is
// sorted for hashing stability, which would otherwise make "127.0.0.1" the
// common name; the domain users actually type is far more useful to show in a
// browser's certificate viewer.
func commonName(hosts []string) string {
	for _, h := range hosts {
		if net.ParseIP(h) == nil && h != "localhost" {
			return h
		}
	}
	return hosts[0]
}

// PublicNameIsDomain reports whether s is usable as DolphinDB's publicName.
// DolphinDB rejects an IP address when enableHTTPS is on, so the operator has to
// derive a domain instead of falling back to the exposed IP.
func PublicNameIsDomain(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || net.ParseIP(s) != nil {
		return false
	}
	// A dotted name is required: a bare label is not a domain.
	return strings.Contains(s, ".")
}
