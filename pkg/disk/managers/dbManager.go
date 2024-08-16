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
	dataLocation       string
	createDirFunc      func(directory string) error
	deleteDirFunc      func(directory string) error
	getDirContentsFunc func(directory string) ([]string, error)
	osStatFunc         func(name string) (os.FileInfo, error)
}

var dbManagerInstance DBManager

func CreateDBManager(dataLocation string) DBManager {
	if dbManagerInstance == nil {
		dbManagerInstance = &dbManager{
			dataLocation:       dataLocation,
			createDirFunc:      diskUtils.CreateDir,
			deleteDirFunc:      diskUtils.DeleteDirectory,
			getDirContentsFunc: diskUtils.GetDirectoryContents,
			osStatFunc:         os.Stat,
		}
	}
	return dbManagerInstance
}

func (ddm *dbManager) Create(db string) error {
	return ddm.createDirFunc(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
}

func (ddm *dbManager) Delete(db string) error {
	return ddm.deleteDirFunc(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
}

func (ddm *dbManager) GetAll() ([]string, error) {
	return ddm.getDirContentsFunc(ddm.dataLocation)
}

func (ddm *dbManager) Exists(db string) bool {
	_, err := ddm.osStatFunc(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
	return err == nil
}
