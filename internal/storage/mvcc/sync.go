package mvcc

import (
	"os"
	"syscall"
)

// OptimizedSync uses standart sync on other platforms
func OptimizedSync(file *os.File) error {
	return syscall.Fsync(int(file.Fd()))
}
