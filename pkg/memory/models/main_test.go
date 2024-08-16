package memoryModels

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/managers"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	diskManagers.CreateMockBlobManager()
	diskManagers.CreateMockIndexManager()
	diskManagers.CreateMockPartitionManager()
	diskManagers.CreateMockFormatManager()
	diskManagers.CreateMockPageManager()
	code := m.Run()
	diskManagers.DestructBlobManager()
	diskManagers.DestructFormatManager()
	diskManagers.DestructPartitionManager()
	diskManagers.DestructIndexManager()
	diskManagers.DestructPageManager()
	os.Exit(code)
}
