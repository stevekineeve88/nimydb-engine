package diskManagers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
)

type BlobManager interface {
	Create(db string, blob string) error
	Delete(db string, blob string) error
	GetByDB(db string) ([]string, error)
}

type blobManager struct {
	dataLocation       string
	createDirFunc      func(directory string) error
	deleteDirFunc      func(directory string) error
	getDirContentsFunc func(directory string) ([]string, error)
}

var blobManagerInstance BlobManager

func CreateBlobManager(dataLocation string) BlobManager {
	if blobManagerInstance == nil {
		blobManagerInstance = &blobManager{
			dataLocation:       dataLocation,
			createDirFunc:      diskUtils.CreateDir,
			deleteDirFunc:      diskUtils.DeleteDirectory,
			getDirContentsFunc: diskUtils.GetDirectoryContents,
		}
	}
	return blobManagerInstance
}

func DestructBlobManager() {
	blobManagerInstance = nil
}

func (bdm *blobManager) Create(db string, blob string) error {
	return bdm.createDirFunc(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob))
}

func (bdm *blobManager) Delete(db string, blob string) error {
	return bdm.deleteDirFunc(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob))
}

func (bdm *blobManager) GetByDB(db string) ([]string, error) {
	return bdm.getDirContentsFunc(fmt.Sprintf("%s/%s", bdm.dataLocation, db))
}
