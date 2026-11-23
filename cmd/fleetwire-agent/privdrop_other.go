//go:build !linux

package main

import "errors"

// dropPrivileges exists only for the Linux snap.
func dropPrivileges([]string) error {
	return errors.New("drop-privileges works only on Linux")
}
