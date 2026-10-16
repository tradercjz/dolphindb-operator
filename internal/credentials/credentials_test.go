package credentials

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newClient(objs ...*corev1.Secret) *fake.ClientBuilder {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	b := fake.NewClientBuilder().WithScheme(s)
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return b
}

func TestResolveUsesSecretWhenPresent(t *testing.T) {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "dolphindb-admin", Namespace: "ddb"},
		Data:       map[string][]byte{"password": []byte("s3cr3t!")},
	}
	c := newClient(sec).Build()

	user, pass := Resolve(context.Background(), c, "ddb", "dolphindb-admin")
	if user != "admin" {
		t.Errorf("user = %q", user)
	}
	if pass != "s3cr3t!" {
		t.Errorf("password = %q, want the Secret value", pass)
	}
}

func TestResolveFallsBackToFactoryDefault(t *testing.T) {
	cases := []struct {
		name       string
		secretName string
	}{
		{"no secret configured", ""},
		{"secret missing", "does-not-exist"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient().Build()
			user, pass := Resolve(context.Background(), c, "ddb", tc.secretName)
			if user != AdminUser || pass != DefaultAdminPassword {
				t.Fatalf("got %q/%q, want the factory default", user, pass)
			}
		})
	}
}

func TestResolveIgnoresEmptyPasswordAndNilClient(t *testing.T) {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "dolphindb-admin", Namespace: "ddb"},
		Data:       map[string][]byte{"password": []byte("   ")},
	}
	c := newClient(sec).Build()
	if _, pass := Resolve(context.Background(), c, "ddb", "dolphindb-admin"); pass != DefaultAdminPassword {
		t.Fatalf("blank password should fall back, got %q", pass)
	}
	if _, pass := Resolve(context.Background(), nil, "ddb", "dolphindb-admin"); pass != DefaultAdminPassword {
		t.Fatalf("nil client should fall back, got %q", pass)
	}
}

// The generated script must not be breakable by a password containing quotes.
func TestLoginScriptEscapesQuotes(t *testing.T) {
	got := LoginScript("admin", `pa"ss\word`)
	want := `login("admin", "pa\"ss\\word");`
	if got != want {
		t.Fatalf("LoginScript = %s, want %s", got, want)
	}
}
