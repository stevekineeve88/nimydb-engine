package diskManagers

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"sync"
)

const (
	indexesFile       = "indexes.json"
	indexesDirectory  = "indexes"
	indexPrefixLength = 1
)

type IndexManager interface {
	Initialize(db string, blob string) error
	Create(db string, blob string, pageRecordId string) (string, error)
	GetAll(db string, blob string) (diskModels.Indexes, error)
	GetData(db string, blob string, indexFileName string) (diskModels.IndexRecords, error)
	WriteData(db string, blob string, indexFileName string, data diskModels.IndexRecords) error
	Delete(db string, blob string, indexFileName string) (bool, error)
	GetPageRecordIdPrefix(pageRecordId string) string
}

type indexManager struct {
	dataLocation string
}

var indexManagerInstance *indexManager

func CreateIndexManager(dataLocation string) IndexManager {
	sync.OnceFunc(func() {
		indexManagerInstance = &indexManager{dataLocation: dataLocation}
	})()
	return indexManagerInstance
}

func (idm *indexManager) Initialize(db string, blob string) error {
	indexesFilePath := idm.getIndexesFileName(db, blob)
	if err := diskUtils.CreateFile(indexesFilePath); err != nil {
		return err
	}

	indexes := diskModels.Indexes{}
	indexesData, _ := json.Marshal(indexes)
	if err := diskUtils.WriteFile(indexesFilePath, indexesData); err != nil {
		return nil
	}

	return diskUtils.CreateDir(idm.getIndexesDirectoryName(db, blob))
}

func (idm *indexManager) Create(db string, blob string, pageRecordId string) (string, error) {
	newIndexFile := fmt.Sprintf("%s.json", uuid.New().String())
	newIndexFilePath := fmt.Sprintf("%s/%s", idm.getIndexesDirectoryName(db, blob), newIndexFile)
	if err := diskUtils.CreateFile(newIndexFilePath); err != nil {
		return "", err
	}
	indexRecords := diskModels.IndexRecords{}
	pageRecordsData, _ := json.Marshal(indexRecords)
	if err := diskUtils.WriteFile(newIndexFilePath, pageRecordsData); err != nil {
		return newIndexFile, err
	}

	indexes, err := idm.GetAll(db, blob)
	if err != nil {
		return newIndexFile, err
	}

	indexPrefix := idm.GetPageRecordIdPrefix(pageRecordId)
	if indexItem, ok := indexes[indexPrefix]; !ok {
		indexes[indexPrefix] = diskModels.IndexItem{FileNames: []string{newIndexFile}}
	} else {
		indexItem.FileNames = append(indexItem.FileNames, newIndexFile)
		indexes[indexPrefix] = indexItem
	}
	indexesData, _ := json.Marshal(indexes)
	err = diskUtils.WriteFile(idm.getIndexesFileName(db, blob), indexesData)
	return newIndexFile, err
}

func (idm *indexManager) GetAll(db string, blob string) (diskModels.Indexes, error) {
	var indexes diskModels.Indexes
	indexesFilePath := idm.getIndexesFileName(db, blob)
	file, err := diskUtils.GetFile(indexesFilePath)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(file, &indexes)
	return indexes, err
}

func (idm *indexManager) GetData(db string, blob string, indexFileName string) (diskModels.IndexRecords, error) {
	var indexRecords diskModels.IndexRecords
	file, err := diskUtils.GetFile(fmt.Sprintf("%s/%s", idm.getIndexesDirectoryName(db, blob), indexFileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &indexRecords)
	return indexRecords, err
}

func (idm *indexManager) WriteData(db string, blob string, indexFileName string, data diskModels.IndexRecords) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return diskUtils.WriteFile(fmt.Sprintf("%s/%s", idm.getIndexesDirectoryName(db, blob), indexFileName), dataBytes)
}

func (idm *indexManager) Delete(db string, blob string, indexFileName string) (bool, error) {
	indexes, err := idm.GetAll(db, blob)
	if err != nil {
		return false, err
	}
	for prefix, _ := range indexes {
		for i := 0; i < len(indexes[prefix].FileNames); i++ {
			if indexes[prefix].FileNames[i] == indexFileName {
				filesNames := indexes[prefix].FileNames
				copy(filesNames[i:], filesNames[i+1:])
				filesNames[len(filesNames)-1] = ""
				filesNames = filesNames[:len(filesNames)-1]
				indexes[prefix] = diskModels.IndexItem{FileNames: filesNames}
				indexesData, _ := json.Marshal(indexes)
				err = diskUtils.WriteFile(idm.getIndexesFileName(db, blob), indexesData)
				if err != nil {
					return false, err
				}
				break
			}
		}
	}
	err = diskUtils.DeleteFile(fmt.Sprintf("%s/%s", idm.getIndexesDirectoryName(db, blob), indexFileName))
	return err != nil, err
}

func (idm *indexManager) GetPageRecordIdPrefix(pageRecordId string) string {
	return pageRecordId[0:indexPrefixLength]
}

func (idm *indexManager) getIndexesFileName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", idm.dataLocation, db, blob, indexesFile)
}

func (idm *indexManager) getIndexesDirectoryName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", idm.dataLocation, db, blob, indexesDirectory)
}
