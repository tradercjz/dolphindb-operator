package tls

import "testing"

func TestCommonNamePrefersDomain(t *testing.T) {
	if got := commonName([]string{"127.0.0.1", "localhost", "a.example.com", "b.example.com"}); got != "a.example.com" {
		t.Fatalf("commonName = %q", got)
	}
	if got := commonName([]string{"127.0.0.1", "localhost"}); got != "127.0.0.1" {
		t.Fatalf("commonName fallback = %q", got)
	}
}
