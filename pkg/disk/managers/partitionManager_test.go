package diskManagers

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func createTestPartitionManager(dataLocation string) partitionManager {
	return partitionManager{dataLocation: dataLocation}
}

func TestUnit_CreatePartitionManager_CreatesPartitionManager(t *testing.T) {
	dataLocation := "dataLocation"
	pm := CreatePartitionManager(dataLocation)

	pmV := reflect.ValueOf(pm)

	assert.Equal(t, dataLocation, reflect.Indirect(pmV).FieldByName("dataLocation").String())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateFile).Pointer(), reflect.Indirect(pmV).FieldByName("createFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateDir).Pointer(), reflect.Indirect(pmV).FieldByName("createDirFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.WriteFile).Pointer(), reflect.Indirect(pmV).FieldByName("writeFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetFile).Pointer(), reflect.Indirect(pmV).FieldByName("getFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.DeleteFile).Pointer(), reflect.Indirect(pmV).FieldByName("deleteFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetDirectoryContents).Pointer(), reflect.Indirect(pmV).FieldByName("getDirContentsFunc").Pointer())
}

func TestUnit_Initialize_InitializesPartition(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	partition := diskModels.Partition{Keys: []string{"par_1", "par_2"}}
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, partitionsFile), filePath)
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, partitionsFile), filePath)
		partitionBytes, _ := json.Marshal(partition)
		assert.Equal(t, partitionBytes, fileData)
		return nil
	}
	pm.createDirFunc = func(directory string) error {
		createDirCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory), directory)
		return nil
	}

	err := pm.Initialize(db, blob, partition)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.True(t, createDirCalled)
	assert.Nil(t, err)
}

func TestUnit_Initialize_FailsOnCreatePartitionsFileError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	partition := diskModels.Partition{Keys: []string{"par_1", "par_2"}}
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return assert.AnError
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}
	pm.createDirFunc = func(directory string) error {
		createDirCalled = true
		return nil
	}

	err := pm.Initialize(db, blob, partition)

	assert.True(t, createFileCalled)
	assert.False(t, writeFileCalled)
	assert.False(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_Initialize_FailsOnWritePartitionsFileError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	partition := diskModels.Partition{Keys: []string{"par_1", "par_2"}}
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return assert.AnError
	}
	pm.createDirFunc = func(directory string) error {
		createDirCalled = true
		return nil
	}

	err := pm.Initialize(db, blob, partition)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.False(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_Initialize_FailsOnCreatePartitionsDirError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	partition := diskModels.Partition{Keys: []string{"par_1", "par_2"}}
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}
	pm.createDirFunc = func(directory string) error {
		createDirCalled = true
		return assert.AnError
	}

	err := pm.Initialize(db, blob, partition)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.True(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_AddPage_AddsPageToPartition(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	hashKeyFileName := "hash.json"
	pageFileName := "page.json"
	createFileCalled := false
	writeFileCalled := 0
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFileName), filePath)
		partitionPageBytes, _ := json.Marshal(diskModels.PartitionPages{})
		return partitionPageBytes, nil
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFileName), filePath)
		partitionPageBytes, _ := json.Marshal(diskModels.PartitionPages{
			diskModels.PartitionPageItem{FileName: pageFileName},
		})
		assert.Equal(t, partitionPageBytes, fileData)
		return nil
	}

	err := pm.AddPage(db, blob, hashKeyFileName, pageFileName)

	assert.False(t, createFileCalled)
	assert.Equal(t, 1, writeFileCalled)
	assert.Nil(t, err)
}

func TestUnit_AddPage_CreatesHashKeyFileIfNotFound(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	hashKeyFileName := "hash.json"
	pageFileName := "page.json"
	createFileCalled := false
	writeFileCalled := 0
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		return nil, assert.AnError
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFileName), filePath)
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		switch writeFileCalled {
		case 1:
			return nil
		case 2:
			assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFileName), filePath)
			partitionPageBytes, _ := json.Marshal(diskModels.PartitionPages{
				diskModels.PartitionPageItem{FileName: pageFileName},
			})
			assert.Equal(t, partitionPageBytes, fileData)
		}
		return nil
	}

	err := pm.AddPage(db, blob, hashKeyFileName, pageFileName)

	assert.True(t, createFileCalled)
	assert.Equal(t, 2, writeFileCalled)
	assert.Nil(t, err)
}

func TestUnit_AddPage_IgnoresPartitionPageDuplicate(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	hashKeyFileName := "hash.json"
	pageFileName := "page.json"
	createFileCalled := false
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		partitionPageBytes, _ := json.Marshal(diskModels.PartitionPages{
			diskModels.PartitionPageItem{FileName: pageFileName},
		})
		return partitionPageBytes, nil
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}

	err := pm.AddPage(db, blob, hashKeyFileName, pageFileName)

	assert.False(t, createFileCalled)
	assert.False(t, writeFileCalled)
	assert.Nil(t, err)
}

func TestUnit_AddPage_FailsOnCreateNewHashError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	hashKeyFileName := "hash.json"
	pageFileName := "page.json"
	createFileCalled := false
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		return nil, assert.AnError
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return assert.AnError
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}

	err := pm.AddPage(db, blob, hashKeyFileName, pageFileName)

	assert.True(t, createFileCalled)
	assert.False(t, writeFileCalled)
	assert.NotNil(t, err)
}

func TestUnit_AddPage_FailsToWritePartitionHashFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	hashKeyFileName := "hash.json"
	pageFileName := "page.json"
	createFileCalled := false
	writeFileCalled := 0
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		partitionPagesBytes, _ := json.Marshal(diskModels.PartitionPages{})
		return partitionPagesBytes, nil
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		return assert.AnError
	}

	err := pm.AddPage(db, blob, hashKeyFileName, pageFileName)

	assert.False(t, createFileCalled)
	assert.Equal(t, 1, writeFileCalled)
	assert.NotNil(t, err)
}

func TestUnit_GetPartition_GetsPartition(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	partition := diskModels.Partition{Keys: []string{"col_one", "col_two"}}
	getFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, partitionsFile), filePath)
		partitionBytes, _ := json.Marshal(partition)
		return partitionBytes, nil
	}

	result, err := pm.GetPartition(db, blob)

	assert.True(t, getFileCalled)
	assert.Nil(t, err)
	assert.Equal(t, partition, result)
}

func TestUnit_GetPartition_FailsOnGetPartitionFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	getFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}

	result, err := pm.GetPartition(db, blob)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, diskModels.Partition{}, result)
}

func TestUnit_GetPartition_FailsOnInvalidPartitionFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	getFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return []byte("invalid data"), nil
	}

	result, err := pm.GetPartition(db, blob)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, diskModels.Partition{}, result)
}

func TestUnit_GetByHashKey_GetsPartitionPages(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFile := "hash.json"
	getFileCalled := false
	partitionPages := diskModels.PartitionPages{
		diskModels.PartitionPageItem{FileName: "page.json"},
	}
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFile), filePath)
		partitionPagesBytes, _ := json.Marshal(partitionPages)
		return partitionPagesBytes, nil
	}

	result, err := pm.GetByHashKey(db, blob, hashKeyFile)

	assert.True(t, getFileCalled)
	assert.Nil(t, err)
	assert.Equal(t, partitionPages, result)
}

func TestUnit_GetByHashKey_FailsOnGetHashKeyFile(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFile := "hash.json"
	getFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}

	result, err := pm.GetByHashKey(db, blob, hashKeyFile)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_GetByHashKey_FailsOnInvalidHashKeyFile(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFile := "hash.json"
	getFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return []byte("invalid data"), nil
	}

	result, err := pm.GetByHashKey(db, blob, hashKeyFile)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_GetAll_GetsAllHashKeyFiles(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFiles := []string{
		"hash.json",
		"hash_2.json",
	}
	getDirContentsCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getDirContentsFunc = func(directory string) ([]string, error) {
		getDirContentsCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory), directory)
		return hashKeyFiles, nil
	}

	result, err := pm.GetAll(db, blob)

	assert.True(t, getDirContentsCalled)
	assert.Nil(t, err)
	assert.Equal(t, result, hashKeyFiles)
}

func TestUnit_GetAll_FailsOnPartitionContentError(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	getDirContentsCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getDirContentsFunc = func(directory string) ([]string, error) {
		getDirContentsCalled = true
		return nil, assert.AnError
	}

	result, err := pm.GetAll(db, blob)

	assert.True(t, getDirContentsCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_Remove_RemovesPageFromHashKey(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	pageFile := "page_2.json"
	partitionPages := diskModels.PartitionPages{
		diskModels.PartitionPageItem{FileName: "page_1.json"},
		diskModels.PartitionPageItem{FileName: pageFile},
	}
	hashKeyFile := "hash.json"
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFile), filePath)
		partitionPagesBytes, _ := json.Marshal(partitionPages)
		return partitionPagesBytes, nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFile), filePath)
		partitionPagesBytes, _ := json.Marshal(diskModels.PartitionPages{
			diskModels.PartitionPageItem{FileName: "page_1.json"},
		})
		assert.Equal(t, partitionPagesBytes, fileData)
		return nil
	}

	err := pm.Remove(db, blob, hashKeyFile, pageFile)

	assert.True(t, writeFileCalled)
	assert.Nil(t, err)
}

func TestUnit_Remove_SkipsFileNotFoundInHashKey(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	pageFile := "page_2.json"
	partitionPages := diskModels.PartitionPages{
		diskModels.PartitionPageItem{FileName: "page_1.json"},
	}
	hashKeyFile := "hash.json"
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		partitionPagesBytes, _ := json.Marshal(partitionPages)
		return partitionPagesBytes, nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}

	err := pm.Remove(db, blob, hashKeyFile, pageFile)

	assert.False(t, writeFileCalled)
	assert.Nil(t, err)
}

func TestUnit_Remove_FailsToGetHashKeyFile(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	pageFile := "page_2.json"
	hashKeyFile := "hash.json"
	getFileCalled := false
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}

	err := pm.Remove(db, blob, hashKeyFile, pageFile)

	assert.True(t, getFileCalled)
	assert.False(t, writeFileCalled)
	assert.NotNil(t, err)
}

func TestUnit_Remove_FailsOnWriteToHashKeyFile(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	pageFile := "page_2.json"
	partitionPages := diskModels.PartitionPages{
		diskModels.PartitionPageItem{FileName: "page_1.json"},
		diskModels.PartitionPageItem{FileName: pageFile},
	}
	hashKeyFile := "hash.json"
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		partitionPagesBytes, _ := json.Marshal(partitionPages)
		return partitionPagesBytes, nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return assert.AnError
	}

	err := pm.Remove(db, blob, hashKeyFile, pageFile)

	assert.True(t, writeFileCalled)
	assert.NotNil(t, err)
}

func TestUnit_Delete_DeletesHashKeyFile(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFile := "hash.json"
	deleteFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.deleteFileFunc = func(filePath string) error {
		deleteFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFile), filePath)
		return nil
	}

	err := pm.Delete(db, blob, hashKeyFile)

	assert.True(t, deleteFileCalled)
	assert.Nil(t, err)
}

func TestUnit_Delete_FailsToDeleteHashKeyFile(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFile := "hash.json"
	deleteFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.deleteFileFunc = func(filePath string) error {
		deleteFileCalled = true
		return assert.AnError
	}

	err := pm.Delete(db, blob, hashKeyFile)

	assert.True(t, deleteFileCalled)
	assert.NotNil(t, err)
}

func TestUnit_GetHashKey_GetsHashKey(t *testing.T) {
	dataLocation := "location"
	partition := diskModels.Partition{Keys: []string{
		"col_1",
		"col_2",
	}}
	pageRecord := diskModels.PageRecord{
		"col_1": "value_1",
		"col_2": "value_2",
	}
	pm := createTestPartitionManager(dataLocation)
	keyItem1, _ := pm.GetHashKeyItem("col_1", pageRecord)
	keyItem2, _ := pm.GetHashKeyItem("col_2", pageRecord)

	result, err := pm.GetHashKey(partition, pageRecord)

	assert.Nil(t, err)
	assert.Equal(t, result, fmt.Sprintf("%s%s.json", keyItem1, keyItem2))

}

func TestUnit_GetHashKey_FailsOnHashKeyItem(t *testing.T) {
	dataLocation := "location"
	partition := diskModels.Partition{Keys: []string{
		"col_1",
		"col_2",
	}}
	pageRecord := diskModels.PageRecord{
		"col_1": "value_1",
	}
	pm := createTestPartitionManager(dataLocation)
	keyItem1, _ := pm.GetHashKeyItem("col_1", pageRecord)

	result, err := pm.GetHashKey(partition, pageRecord)

	assert.NotNil(t, err)
	assert.Equal(t, result, keyItem1)
}

func TestUnit_GetHashKeyItem_GetsHashKeyItem(t *testing.T) {
	dataLocation := "location"
	key := "col_1"
	pageRecord := diskModels.PageRecord{
		key: "value_1",
	}
	pm := createTestPartitionManager(dataLocation)
	result, err := pm.GetHashKeyItem(key, pageRecord)

	assert.Nil(t, err)
	hash := sha1.New()
	hash.Write([]byte(fmt.Sprintf("%+v", pageRecord[key])))
	hashString := base64.URLEncoding.EncodeToString(hash.Sum(nil))
	assert.Equal(t, hashString, result)
	assert.Equal(t, 28, len(result))
}

func TestUnit_GetHashKeyItem_FailsOnMissingKey(t *testing.T) {
	dataLocation := "location"
	key := "col_1"
	pageRecord := diskModels.PageRecord{
		"wrong_key": "value_1",
	}
	pm := createTestPartitionManager(dataLocation)
	result, err := pm.GetHashKeyItem(key, pageRecord)

	assert.NotNil(t, err)
	assert.Equal(t, "", result)
}

func TestUnit_CreateHashKey_CreatesHashKey(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFile := "hash.json"
	createFileCalled := false
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFile), filePath)
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, partitionsDirectory, hashKeyFile), filePath)
		hashKeyBytes, _ := json.Marshal(diskModels.PartitionPages{})
		assert.Equal(t, hashKeyBytes, fileData)
		return nil
	}

	result, err := pm.CreateHashKey(db, blob, hashKeyFile)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.Nil(t, err)
	assert.Equal(t, diskModels.PartitionPages{}, result)
}

func TestUnit_CreateHashKey_FailsOnHashKeyCreate(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFile := "hash.json"
	createFileCalled := false
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return assert.AnError
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}

	result, err := pm.CreateHashKey(db, blob, hashKeyFile)

	assert.True(t, createFileCalled)
	assert.False(t, writeFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_CreateHashKey_FailsOnHashKeyWrite(t *testing.T) {
	dataLocation := "location"
	db := "db"
	blob := "blob"
	hashKeyFile := "hash.json"
	createFileCalled := false
	writeFileCalled := false
	pm := createTestPartitionManager(dataLocation)
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return assert.AnError
	}

	result, err := pm.CreateHashKey(db, blob, hashKeyFile)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, diskModels.PartitionPages{}, result)
}
