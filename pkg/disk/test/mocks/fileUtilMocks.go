package diskMocks

import (
	"os"
	"time"
)

type MockFileInfo struct{}

func (f MockFileInfo) Name() string {
	return "mock name"
}

func (f MockFileInfo) Size() int64 {
	return 100
}

func (f MockFileInfo) Mode() os.FileMode {
	return os.ModeDevice
}

func (f MockFileInfo) ModTime() time.Time {
	return time.Now()
}

func (f MockFileInfo) IsDir() bool {
	return true
}

func (f MockFileInfo) Sys() any {
	return "mock system"
}
