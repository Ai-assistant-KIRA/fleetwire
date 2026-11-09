// Package openapi embeds the API description, so tests and tools use the
// exact file that is published.
package openapi

import _ "embed"

// Spec is api/openapi.yaml.
//
//go:embed openapi.yaml
var Spec []byte
