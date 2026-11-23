//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/statedir"
)

// dropPrivileges runs as root at the end of the snap's command chain: it
// becomes the snap_daemon user and replaces itself with args. It does what
// `setpriv --reuid --regid --clear-groups` does, in the agent's own binary,
// because snapd before 2.68 (Ubuntu Core 24 ships 2.63) does not let a
// snap execute /usr/bin/setpriv under its default AppArmor template.
//
// The calls are the ones snapd's seccomp filter allows for a snap with
// system-usernames: setgroups(0, NULL), then setresgid and setresuid with
// every id set to snap_daemon. Changing every uid away from 0 clears the
// permitted, effective and ambient capabilities, and exec starts the
// command fresh, so no code of the daemon ever runs as root.
func dropPrivileges(args []string) error {
	if len(args) == 0 {
		return errors.New("drop-privileges needs a command to run")
	}
	if os.Geteuid() != 0 {
		return errors.New("drop-privileges must start as root: the snap's command chain runs it")
	}
	path, err := exec.LookPath(args[0])
	if err != nil {
		return fmt.Errorf("drop-privileges: %w", err)
	}
	id := statedir.SnapDaemonID
	if err := syscall.Setgroups(nil); err != nil {
		return fmt.Errorf("clear supplementary groups: %w", err)
	}
	if err := syscall.Setresgid(id, id, id); err != nil {
		return fmt.Errorf("set gid %d: %w", id, err)
	}
	if err := syscall.Setresuid(id, id, id); err != nil {
		return fmt.Errorf("set uid %d: %w", id, err)
	}
	// Fail closed: never exec the command with any id still root.
	ruid, euid, suid := unix.Getresuid()
	rgid, egid, sgid := unix.Getresgid()
	groups, err := syscall.Getgroups()
	if err != nil || len(groups) != 0 || ruid != id || euid != id || suid != id || rgid != id || egid != id || sgid != id {
		return fmt.Errorf("privileges not dropped: uids %d/%d/%d, gids %d/%d/%d, groups %v", ruid, euid, suid, rgid, egid, sgid, groups)
	}
	return syscall.Exec(path, args, os.Environ()) //nolint:gosec // G204: the command comes from the snap's own command chain
}
