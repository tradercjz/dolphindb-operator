package tls

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestDesiredHostsIsSortedAndDeduplicated(t *testing.T) {
	got := DesiredHosts("ddb.example.com", "localhost", "a.svc", "", "a.svc", "b.example.com.")
	want := []string{"127.0.0.1", "a.svc", "b.example.com", "ddb.example.com", "localhost"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DesiredHosts = %v, want %v", got, want)
	}
}

func TestDesiredHostsAlwaysIncludesLoopback(t *testing.T) {
	got := DesiredHosts("")
	set := map[string]bool{}
	for _, h := range got {
		set[h] = true
	}
	if !set["localhost"] || !set["127.0.0.1"] {
		t.Fatalf("loopback entries missing: %v", got)
	}
}

func TestHostsHashIsOrderIndependent(t *testing.T) {
	a := HostsHash([]string{"b", "a", "c"})
	b := HostsHash([]string{"c", "b", "a"})
	if a != b {
		t.Fatalf("hash must not depend on order: %s != %s", a, b)
	}
	if HostsHash([]string{"a", "b"}) == a {
		t.Fatal("different host sets must produce different hashes")
	}
}

func TestGenerateProducesUsableCertificate(t *testing.T) {
	hosts := []string{"ddb.example.com", "127.0.0.1"}
	mat, err := Generate(hosts, 30)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	// The pair must load as a TLS certificate, which is what DolphinDB needs.
	pair, err := tls.X509KeyPair(mat.CertPEM, mat.KeyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}

	// The common name must be a domain, not the loopback address that sorting
	// happens to place first.
	if leaf.Subject.CommonName != "ddb.example.com" {
		t.Errorf("CommonName = %q, want the domain rather than an IP", leaf.Subject.CommonName)
	}
	if err := leaf.VerifyHostname("ddb.example.com"); err != nil {
		t.Errorf("VerifyHostname(ddb.example.com): %v", err)
	}
	if err := leaf.VerifyHostname("127.0.0.1"); err != nil {
		t.Errorf("VerifyHostname(127.0.0.1): %v", err)
	}
	if err := leaf.VerifyHostname("other.example.com"); err == nil {
		t.Error("an unlisted host must not verify")
	}

	// Self-signed and importable as a trust anchor: this is what lets a user
	// silence the browser warning by importing server.crt.
	if !leaf.IsCA {
		t.Error("certificate should be its own CA so it can be imported as a trust anchor")
	}
	if leaf.Issuer.CommonName != leaf.Subject.CommonName {
		t.Error("certificate should be self-signed")
	}
	var hasServerAuth bool
	for _, u := range leaf.ExtKeyUsage {
		if u == x509.ExtKeyUsageServerAuth {
			hasServerAuth = true
		}
	}
	if !hasServerAuth {
		t.Error("certificate must carry ServerAuth")
	}

	// Backdated to tolerate clock skew, and long enough for the requested lifetime.
	if !leaf.NotBefore.Before(time.Now()) {
		t.Error("NotBefore should be backdated to tolerate clock skew")
	}
	if leaf.NotAfter.Before(time.Now().AddDate(0, 0, 29)) {
		t.Errorf("NotAfter = %v, expected about 30 days out", leaf.NotAfter)
	}
}

func TestGenerateDefaultsValidity(t *testing.T) {
	mat, err := Generate([]string{"ddb.example.com"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(mat.CertPEM)
	if block == nil {
		t.Fatal("certificate is not PEM encoded")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if leaf.NotAfter.Before(time.Now().AddDate(0, 0, DefaultValidityDays-1)) {
		t.Errorf("default validity too short: %v", leaf.NotAfter)
	}
}

func TestGenerateRejectsEmptyHostList(t *testing.T) {
	if _, err := Generate(nil, 30); err == nil {
		t.Fatal("expected an error for an empty host list")
	}
}

func TestGenerateProducesDistinctCertificates(t *testing.T) {
	a, err := Generate([]string{"ddb.example.com"}, 30)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate([]string{"ddb.example.com"}, 30)
	if err != nil {
		t.Fatal(err)
	}
	if string(a.CertPEM) == string(b.CertPEM) {
		t.Fatal("each issue must produce a fresh certificate")
	}
}

func TestPublicNameIsDomain(t *testing.T) {
	cases := map[string]bool{
		"ddb.example.com":                            true,
		"a4a6.elb.ap-southeast-2.amazonaws.com":      true,
		"ddb-min.dolphindb-system.svc.cluster.local": true,
		"":          false,
		"10.0.0.1":  false,
		"localhost": false, // DolphinDB requires a domain, not a bare label
	}
	for in, want := range cases {
		if got := PublicNameIsDomain(in); got != want {
			t.Errorf("PublicNameIsDomain(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestGenerateHandlesIPv6(t *testing.T) {
	mat, err := Generate([]string{"ddb.example.com", "::1"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(mat.CertPEM)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ip := range leaf.IPAddresses {
		if ip.Equal(net.ParseIP("::1")) {
			found = true
		}
	}
	if !found {
		t.Errorf("IPv6 SAN missing, got %v", leaf.IPAddresses)
	}
}
