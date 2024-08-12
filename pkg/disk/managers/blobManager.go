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
	dataLocation string
}

var blobManagerInstance *blobManager

func CreateBlobManager(dataLocation string) BlobManager {
	if blobManagerInstance == nil {
		blobManagerInstance = &blobManager{dataLocation: dataLocation}
	}
	return blobManagerInstance
}

func (bdm *blobManager) Create(db string, blob string) error {
	return diskUtils.CreateDir(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob))
}

func (bdm *blobManager) Delete(db string, blob string) error {
	return diskUtils.DeleteDirectory(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob))
}

func (bdm *blobManager) GetByDB(db string) ([]string, error) {
	return diskUtils.GetDirectoryContents(fmt.Sprintf("%s/%s", bdm.dataLocation, db))
}
