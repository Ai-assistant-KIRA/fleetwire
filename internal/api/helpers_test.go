package api_test

import (
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
)

func mustCSR(t *testing.T, d apitest.Device) []byte {
	t.Helper()
	csr, err := pki.NewCSR(d.Key, d.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	return csr
}
