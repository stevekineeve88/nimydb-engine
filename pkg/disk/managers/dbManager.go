package diskManagers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"os"
)

type DBManager interface {
	Create(db string) error
	Delete(db string) error
	GetAll() ([]string, error)
	Exists(db string) bool
}

type dbManager struct {
	dataLocation string
}

var dbManagerInstance *dbManager

func CreateDBManager(dataLocation string) DBManager {
	if dbManagerInstance == nil {
		dbManagerInstance = &dbManager{dataLocation: dataLocation}
	}
	return dbManagerInstance
}

func (ddm *dbManager) Create(db string) error {
	return diskUtils.CreateDir(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
}

func (ddm *dbManager) Delete(db string) error {
	return diskUtils.DeleteDirectory(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
}

func (ddm *dbManager) GetAll() ([]string, error) {
	return diskUtils.GetDirectoryContents(ddm.dataLocation)
}

func (ddm *dbManager) Exists(db string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
	return err == nil
}
