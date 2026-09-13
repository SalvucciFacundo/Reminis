//go:build windows

package worker

import (
	"os/exec"
)

func setProcessGroup(cmd *exec.Cmd) {
	// Windows process group handling does not use POSIX Setpgid
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
