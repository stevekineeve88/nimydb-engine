package diskManagers

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"sync"
)

const (
	partitionsFile      = "partitions.json"
	partitionsDirectory = "partitions"
)

type PartitionManager interface {
	Initialize(db string, blob string, partition diskModels.Partition) error
	AddPage(db string, blob string, hashKeyFileName string, pageFileName string) error
	GetPartition(db string, blob string) (diskModels.Partition, error)
	GetByHashKey(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error)
	GetHashKeyItem(partitionKey string, pageRecord diskModels.PageRecord) (string, error)
	GetAll(db string, blob string) ([]string, error)
	Remove(db string, blob string, hashKeyFileName string, pageFileName string) error
	Delete(db string, blob string, hashKeyFileName string) error
	GetHashKey(partition diskModels.Partition, pageRecord diskModels.PageRecord) (string, error)
}

type partitionManager struct {
	dataLocation string
}

var partitionManagerInstance *partitionManager

func CreatePartitionManager(dataLocation string) PartitionManager {
	sync.OnceFunc(func() {
		partitionManagerInstance = &partitionManager{dataLocation: dataLocation}
	})()
	return partitionManagerInstance
}

func (pdm *partitionManager) Initialize(db string, blob string, partition diskModels.Partition) error {
	partitionFilePath := pdm.getPartitionsFileName(db, blob)
	err := diskUtils.CreateFile(partitionFilePath)
	if err != nil {
		return err
	}

	partitionData, err := json.Marshal(partition)
	if err != nil {
		return err
	}
	err = diskUtils.WriteFile(partitionFilePath, partitionData)
	if err != nil {
		return err
	}

	return diskUtils.CreateDir(pdm.getPartitionsDirectoryName(db, blob))
}

func (pdm *partitionManager) AddPage(db string, blob string, hashKeyFileName string, pageFileName string) error {
	partitionPages, err := pdm.GetByHashKey(db, blob, hashKeyFileName)
	if err != nil {
		partitionPages, err = pdm.createHashKey(db, blob, hashKeyFileName)
		if err != nil {
			return err
		}
	}
	for _, partitionPage := range partitionPages {
		if partitionPage.FileName == pageFileName {
			return nil
		}
	}

	partitionPages = append(partitionPages, diskModels.PartitionPageItem{FileName: pageFileName})
	partitionPagesData, err := json.Marshal(partitionPages)
	if err != nil {
		return err
	}

	return diskUtils.WriteFile(fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName), partitionPagesData)
}

func (pdm *partitionManager) GetPartition(db string, blob string) (diskModels.Partition, error) {
	file, err := diskUtils.GetFile(pdm.getPartitionsFileName(db, blob))
	if err != nil {
		return diskModels.Partition{}, err
	}

	var partition diskModels.Partition
	err = json.Unmarshal(file, &partition)
	return partition, err
}

func (pdm *partitionManager) GetByHashKey(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error) {
	file, err := diskUtils.GetFile(fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName))
	if err != nil {
		return nil, err
	}

	var partitionPages diskModels.PartitionPages
	err = json.Unmarshal(file, &partitionPages)
	return partitionPages, err
}

func (pdm *partitionManager) GetAll(db string, blob string) ([]string, error) {
	return diskUtils.GetDirectoryContents(pdm.getPartitionsDirectoryName(db, blob))
}

func (pdm *partitionManager) createHashKey(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error) {
	hashKeyFilePath := fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName)
	err := diskUtils.CreateFile(hashKeyFilePath)
	if err != nil {
		return nil, err
	}

	partitionPages := diskModels.PartitionPages{}
	partitionPagesData, err := json.Marshal(partitionPages)
	if err != nil {
		return nil, err
	}

	return partitionPages, diskUtils.WriteFile(hashKeyFilePath, partitionPagesData)
}

func (pdm *partitionManager) Remove(db string, blob string, hashKeyFileName string, pageFileName string) error {
	partitionPages, err := pdm.GetByHashKey(db, blob, hashKeyFileName)
	if err != nil {
		return err
	}

	for i := 0; i < len(partitionPages); i++ {
		if partitionPages[i].FileName == pageFileName {
			copy(partitionPages[i:], partitionPages[i+1:])
			partitionPages[len(partitionPages)-1] = diskModels.PartitionPageItem{}
			partitionPages = partitionPages[:len(partitionPages)-1]
			partitionPagesData, _ := json.Marshal(partitionPages)
			return diskUtils.WriteFile(fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName), partitionPagesData)
		}
	}
	return nil
}

func (pdm *partitionManager) Delete(db string, blob string, hashKeyFileName string) error {
	return diskUtils.DeleteFile(fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName))
}

func (pdm *partitionManager) GetHashKey(partition diskModels.Partition, pageRecord diskModels.PageRecord) (string, error) {
	hashKey := ""
	for _, key := range partition.Keys {
		hashKeyItem, err := pdm.GetHashKeyItem(key, pageRecord)
		if err != nil {
			return hashKey, err
		}
		hashKey += hashKeyItem
	}
	return fmt.Sprintf("%s.json", hashKey), nil
}

func (pdm *partitionManager) GetHashKeyItem(partitionKey string, pageRecord diskModels.PageRecord) (string, error) {
	pageRecordItem, ok := pageRecord[partitionKey]
	if !ok {
		return "", errors.New(fmt.Sprintf("%s not found in page record", partitionKey))
	}
	hash := sha1.New()
	hash.Write([]byte(fmt.Sprintf("%+v", pageRecordItem)))
	return base64.URLEncoding.EncodeToString(hash.Sum(nil)), nil
}

func (pdm *partitionManager) getPartitionsFileName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, partitionsFile)
}

func (pdm *partitionManager) getPartitionsDirectoryName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, partitionsDirectory)
}
