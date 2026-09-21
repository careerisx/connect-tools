//go:build windows

package config

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

const (
	moveFileReplaceExisting = 0x1
	moveFileWriteThrough    = 0x8
)

var moveFileExW = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func replaceFile(source, destination string) error {
	sourcePointer, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return fmt.Errorf("encode source path for MoveFileExW: %w", err)
	}
	destinationPointer, err := syscall.UTF16PtrFromString(destination)
	if err != nil {
		return fmt.Errorf("encode destination path for MoveFileExW: %w", err)
	}

	result, _, callErr := moveFileExW.Call(
		uintptr(unsafe.Pointer(sourcePointer)),
		uintptr(unsafe.Pointer(destinationPointer)),
		uintptr(moveFileReplaceExisting|moveFileWriteThrough),
	)
	if result != 0 {
		return nil
	}
	if callErr != syscall.Errno(0) {
		return fmt.Errorf("MoveFileExW: %w", callErr)
	}
	return errors.New("MoveFileExW failed without an operating system error")
}
