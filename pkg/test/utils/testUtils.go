package testUtils

type MockMutex struct {
	LockFunc   func()
	UnlockFunc func()
}

func CreateMockMutex(lockFunc func(), unlockFunc func()) *MockMutex {
	return &MockMutex{
		LockFunc:   lockFunc,
		UnlockFunc: unlockFunc,
	}
}

func (mm *MockMutex) Lock() {
	mm.LockFunc()
}

func (mm *MockMutex) Unlock() {
	mm.UnlockFunc()
}
