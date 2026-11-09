// Command dev-ca creates the development PKI for the Compose stack:
//
//	go run ./scripts/dev-ca [-out deploy/compose/secrets] [-force] [-hosts a,b]
//
// It refuses to replace an existing set unless -force is given, and does
// nothing when the set is already complete. Private keys are written 0400
// into a git-ignored directory and are never committed.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
)

func main() {
	out := flag.String("out", "deploy/compose/secrets", "Output directory.")
	force := flag.Bool("force", false, "Regenerate every file, replacing the existing set.")
	hosts := flag.String("hosts", "localhost,127.0.0.1,mosquitto,toxiproxy,api", "Comma-separated DNS names and IPs for the server certificates.")
	flag.Parse()

	var list []string
	for _, h := range strings.Split(*hosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			list = append(list, h)
		}
	}
	created, err := devpki.Generate(*out, devpki.Options{Force: *force, Hosts: list})
	if err != nil {
		fmt.Fprintln(os.Stderr, "dev-ca:", err)
		os.Exit(1)
	}
	if created {
		fmt.Printf("dev-ca: wrote %d files to %s\n", len(devpki.All), *out)
	} else {
		fmt.Printf("dev-ca: %s already holds a complete set; use -force to replace it\n", *out)
	}
}
