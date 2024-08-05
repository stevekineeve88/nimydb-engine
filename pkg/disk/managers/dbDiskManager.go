package diskManagers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"os"
	"sync"
)

type DBDiskManager interface {
	Create(db string) error
	Delete(db string) error
	GetAll() ([]string, error)
	Exists(db string) bool
}

type dbDiskManager struct {
	dataLocation string
}

var dbDiskManagerInstance *dbDiskManager

func CreateDBDiskManager(dataLocation string) DBDiskManager {
	sync.OnceFunc(func() {
		dbDiskManagerInstance = &dbDiskManager{dataLocation: dataLocation}
	})()
	return dbDiskManagerInstance
}

func (ddm *dbDiskManager) Create(db string) error {
	return diskUtils.CreateDir(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
}

func (ddm *dbDiskManager) Delete(db string) error {
	return diskUtils.DeleteDirectory(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
}

func (ddm *dbDiskManager) GetAll() ([]string, error) {
	return diskUtils.GetDirectoryContents(ddm.dataLocation)
}

func (ddm *dbDiskManager) Exists(db string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
	return err == nil
}
