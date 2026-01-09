package oauth

import (
	"testing"
)

func TestSignAndVerify(t *testing.T) {
	m := NewMiddleware(Config{
		ClientID:     "test-id",
		ClientSecret: "test-secret",
		RedirectURL:  "http://localhost/callback",
	})

	t.Run("valid signature", func(t *testing.T) {
		payload := "eyJpZCI6IjEyMyIsImVtYWlsIjoidGVzdEB0ZXN0LmNvbSJ9"
		signed := m.signValue(payload)

		verified, ok := m.verifyValue(signed)
		if !ok {
			t.Fatal("expected signature to be valid")
		}
		if verified != payload {
			t.Fatalf("payload mismatch: got %q, want %q", verified, payload)
		}
	})

	t.Run("tampered payload rejected", func(t *testing.T) {
		payload := "eyJpZCI6IjEyMyIsImVtYWlsIjoidGVzdEB0ZXN0LmNvbSJ9"
		signed := m.signValue(payload)

		// Tamper with the payload
		tampered := "eyJpZCI6IjQ1NiIsImVtYWlsIjoiZXZpbEB0ZXN0LmNvbSJ9" + signed[len(payload):]

		_, ok := m.verifyValue(tampered)
		if ok {
			t.Fatal("expected tampered signature to be rejected")
		}
	})

	t.Run("tampered signature rejected", func(t *testing.T) {
		payload := "eyJpZCI6IjEyMyIsImVtYWlsIjoidGVzdEB0ZXN0LmNvbSJ9"
		signed := m.signValue(payload)

		// Tamper with the signature
		tampered := signed[:len(signed)-4] + "XXXX"

		_, ok := m.verifyValue(tampered)
		if ok {
			t.Fatal("expected tampered signature to be rejected")
		}
	})

	t.Run("missing signature rejected", func(t *testing.T) {
		_, ok := m.verifyValue("just-payload-no-signature")
		if ok {
			t.Fatal("expected missing signature to be rejected")
		}
	})

	t.Run("different key rejects", func(t *testing.T) {
		payload := "eyJpZCI6IjEyMyIsImVtYWlsIjoidGVzdEB0ZXN0LmNvbSJ9"
		signed := m.signValue(payload)

		// Different middleware with different secret
		m2 := NewMiddleware(Config{
			ClientID:     "test-id",
			ClientSecret: "different-secret",
			RedirectURL:  "http://localhost/callback",
		})

		_, ok := m2.verifyValue(signed)
		if ok {
			t.Fatal("expected different key to reject signature")
		}
	})
}
