package crypto

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestBoxRoundTrip(t *testing.T) {
	box, err := NewBox("super-secret-passphrase-12345678")
	if err != nil {
		t.Fatalf("NewBox failed: %v", err)
	}

	cases := []struct {
		name string
		in   string
	}{
		{"ascii", "s3cr3t-pg-password"},
		{"utf8_emoji", "p@ssw0rd-🔒-日本語-tiếng-việt"},
		{"quotes_newlines", "line1\n'quoted' \"double\" \\backslash\\"},
		{"large_4kib", strings.Repeat("A", 4096)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := box.Encrypt(tc.in)
			if err != nil {
				t.Fatalf("Encrypt failed: %v", err)
			}
			if enc == tc.in {
				t.Fatalf("ciphertext should not equal plaintext")
			}
			dec, err := box.Decrypt(enc)
			if err != nil {
				t.Fatalf("Decrypt failed: %v", err)
			}
			if dec != tc.in {
				t.Fatalf("round-trip mismatch: got %q, want %q", dec, tc.in)
			}
		})
	}
}

func TestBoxEmptyStringContract(t *testing.T) {
	box, err := NewBox("test-passphrase-12345678")
	if err != nil {
		t.Fatalf("NewBox failed: %v", err)
	}

	enc, err := box.Encrypt("")
	if err != nil || enc != "" {
		t.Fatalf("Encrypt(\"\") = (%q, %v), want (\"\", nil)", enc, err)
	}
	dec, err := box.Decrypt("")
	if err != nil || dec != "" {
		t.Fatalf("Decrypt(\"\") = (%q, %v), want (\"\", nil)", dec, err)
	}
}

func TestBoxSemanticSecurityRandomNonce(t *testing.T) {
	box, err := NewBox("test-passphrase-12345678")
	if err != nil {
		t.Fatalf("NewBox failed: %v", err)
	}

	c1, err := box.Encrypt("same-secret")
	if err != nil {
		t.Fatalf("Encrypt 1 failed: %v", err)
	}
	c2, err := box.Encrypt("same-secret")
	if err != nil {
		t.Fatalf("Encrypt 2 failed: %v", err)
	}
	if c1 == c2 {
		t.Fatalf("expected distinct ciphertexts due to random nonce, got identical %q", c1)
	}
	for i, c := range []string{c1, c2} {
		plain, err := box.Decrypt(c)
		if err != nil || plain != "same-secret" {
			t.Fatalf("Decrypt #%d = (%q, %v), want (\"same-secret\", nil)", i+1, plain, err)
		}
	}
}

func TestBoxKeyIsolationAndTampering(t *testing.T) {
	box1, err := NewBox("passphrase-one-12345678")
	if err != nil {
		t.Fatalf("NewBox 1 failed: %v", err)
	}
	box2, err := NewBox("other-passphrase-1234")
	if err != nil {
		t.Fatalf("NewBox 2 failed: %v", err)
	}

	enc, err := box1.Encrypt("sensitive-db-password")
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if _, err := box2.Decrypt(enc); err == nil {
		t.Fatalf("expected Decrypt with wrong key to fail")
	}

	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}

	// Flip 1 bit in nonce, payload, and GCM auth tag
	positions := []struct {
		name string
		idx  int
	}{
		{"nonce", 0},
		{"payload", 12},
		{"auth_tag", len(raw) - 1},
	}
	for _, pos := range positions {
		t.Run("tamper_"+pos.name, func(t *testing.T) {
			corrupted := append([]byte(nil), raw...)
			corrupted[pos.idx] ^= 0x01
			corruptedB64 := base64.StdEncoding.EncodeToString(corrupted)
			if _, err := box1.Decrypt(corruptedB64); err == nil {
				t.Fatalf("expected Decrypt to fail when %s bit is flipped", pos.name)
			}
		})
	}
}

func TestBoxMalformedCiphertexts(t *testing.T) {
	box, err := NewBox("test-passphrase-12345678")
	if err != nil {
		t.Fatalf("NewBox failed: %v", err)
	}

	cases := []struct {
		name  string
		input string
	}{
		{"invalid_base64", "!!!not-base64!!!"},
		{"too_short", base64.StdEncoding.EncodeToString([]byte{1, 2, 3})},
		{"nonce_only_no_tag", base64.StdEncoding.EncodeToString(make([]byte, 12))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := box.Decrypt(tc.input); err == nil {
				t.Fatalf("expected error for malformed input %s", tc.name)
			}
		})
	}
}
