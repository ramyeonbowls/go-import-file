package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func EnsureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

func MoveFile(src, dstDir string) error {
	base := filepath.Base(src)
	dst := filepath.Join(dstDir, base)

	// jika file sudah ada, tambahkan timestamp
	if _, err := os.Stat(dst); err == nil {
		ext := filepath.Ext(base)
		name := base[:len(base)-len(ext)]
		dst = filepath.Join(
			dstDir,
			fmt.Sprintf("%s_%d%s", name, time.Now().Unix(), ext),
		)
	}

	return os.Rename(src, dst)
}
