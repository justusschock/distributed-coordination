//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd || windows)

package file

import (
	"errors"
	"os"
)

func supported() error {
	return errors.ErrUnsupported
}

func tryLock(*os.File) (bool, error) {
	return false, errors.ErrUnsupported
}

func unlock(*os.File) error {
	return errors.ErrUnsupported
}

func syncDir(string) error {
	return errors.ErrUnsupported
}
