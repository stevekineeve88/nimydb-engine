package diskManagers

import (
	"encoding/json"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func createTestIndexManager(dataLocation string) indexManager {
	return indexManager{dataLocation: dataLocation}
}

func TestUnit_CreateIndexManager_CreatesIndexManager(t *testing.T) {
	dataLocation := "dataLocation"
	im := CreateIndexManager(dataLocation)

	imV := reflect.ValueOf(im)

	assert.Equal(t, dataLocation, reflect.Indirect(imV).FieldByName("dataLocation").String())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateFile).Pointer(), reflect.Indirect(imV).FieldByName("createFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateDir).Pointer(), reflect.Indirect(imV).FieldByName("createDirFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.WriteFile).Pointer(), reflect.Indirect(imV).FieldByName("writeFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetFile).Pointer(), reflect.Indirect(imV).FieldByName("getFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.DeleteFile).Pointer(), reflect.Indirect(imV).FieldByName("deleteFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetUUID).Pointer(), reflect.Indirect(imV).FieldByName("uuidFunc").Pointer())
}

func TestUnit_Initialize_InitializesIndexes(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	im := createTestIndexManager(dataLocation)
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, indexesFile), filePath)
		return nil
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, indexesFile), filePath)
		blankBytes, _ := json.Marshal(diskModels.Indexes{})
		assert.Equal(t, blankBytes, fileData)
		return nil
	}
	im.createDirFunc = func(directory string) error {
		createDirCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, indexesDirectory), directory)
		return nil
	}

	err := im.Initialize(db, blob)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.True(t, createDirCalled)
	assert.Nil(t, err)
}

func TestUnit_Initialize_FailsOnIndexesFileCreateError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	im := createTestIndexManager(dataLocation)
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return assert.AnError
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}
	im.createDirFunc = func(directory string) error {
		createDirCalled = true
		return nil
	}

	err := im.Initialize(db, blob)

	assert.True(t, createFileCalled)
	assert.False(t, writeFileCalled)
	assert.False(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_Initialize_FailsOnIndexesFileWriteError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	im := createTestIndexManager(dataLocation)
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return assert.AnError
	}
	im.createDirFunc = func(directory string) error {
		createDirCalled = true
		return nil
	}

	err := im.Initialize(db, blob)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.False(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_Initialize_FailsOnIndexesDirectoryCreateError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	im := createTestIndexManager(dataLocation)
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}
	im.createDirFunc = func(directory string) error {
		createDirCalled = true
		return assert.AnError
	}

	err := im.Initialize(db, blob)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.True(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_Create_CreatesIndexFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	pageRecordId := "12345"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	im := createTestIndexManager(dataLocation)
	im.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s.json", dataLocation, db, blob, indexesDirectory, testUuid), filePath)
		return nil
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		switch writeFileCalled {
		case 0:
			assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s.json", dataLocation, db, blob, indexesDirectory, testUuid), filePath)
			indexRecordsBytes, _ := json.Marshal(diskModels.IndexRecords{})
			assert.Equal(t, indexRecordsBytes, fileData)
		case 1:
			assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, indexesFile), filePath)
			indexesBytes, _ := json.Marshal(diskModels.Indexes{
				im.GetPageRecordIdPrefix(pageRecordId): diskModels.IndexItem{FileNames: []string{testUuid + ".json"}},
			})
			assert.Equal(t, indexesBytes, fileData)
		default:
			panic("write not handled")
		}
		writeFileCalled++
		return nil
	}
	im.getFileFunc = func(filePath string) ([]byte, error) {
		indexesData, _ := json.Marshal(diskModels.Indexes{})
		return indexesData, nil
	}

	result, err := im.Create(db, blob, pageRecordId)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 2, writeFileCalled)
	assert.Nil(t, err)
	assert.Equal(t, testUuid+".json", result)
}

func TestUnit_Create_CreatesIndexFileOnExistingPrefix(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	pageRecordId := "12345"
	existingIndexFile := "existing"
	writeFileCalled := 0
	im := createTestIndexManager(dataLocation)
	indexes := diskModels.Indexes{
		im.GetPageRecordIdPrefix(pageRecordId): diskModels.IndexItem{FileNames: []string{existingIndexFile}},
	}
	im.uuidFunc = func() string {
		return testUuid
	}
	im.createFileFunc = func(filePath string) error {
		return nil
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		switch writeFileCalled {
		case 1:
			return nil
		case 2:
			expected := diskModels.Indexes{
				im.GetPageRecordIdPrefix(pageRecordId): diskModels.IndexItem{FileNames: []string{existingIndexFile, testUuid + ".json"}},
			}
			expectedBytes, _ := json.Marshal(expected)
			assert.Equal(t, expectedBytes, fileData)
			return nil
		default:
			return nil
		}
	}
	im.getFileFunc = func(filePath string) ([]byte, error) {
		indexesData, _ := json.Marshal(indexes)
		return indexesData, nil
	}

	_, _ = im.Create(db, blob, pageRecordId)

	assert.Equal(t, 2, writeFileCalled)
}

func TestUnit_Create_FailsOnCreatingIndexFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	pageRecordId := "12345"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	im := createTestIndexManager(dataLocation)
	im.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return assert.AnError
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		return nil
	}
	im.getFileFunc = func(filePath string) ([]byte, error) {
		indexesData, _ := json.Marshal(diskModels.Indexes{})
		return indexesData, nil
	}

	result, err := im.Create(db, blob, pageRecordId)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 0, writeFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, "", result)
}

func TestUnit_Create_FailsOnWritingIndexRecords(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	pageRecordId := "12345"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	im := createTestIndexManager(dataLocation)
	im.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		return assert.AnError
	}
	im.getFileFunc = func(filePath string) ([]byte, error) {
		indexesData, _ := json.Marshal(diskModels.Indexes{})
		return indexesData, nil
	}

	result, err := im.Create(db, blob, pageRecordId)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 1, writeFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, testUuid+".json", result)
}

func TestUnit_Create_FailsOnGettingIndexes(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	pageRecordId := "12345"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	getFileCalled := false
	im := createTestIndexManager(dataLocation)
	im.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		return nil
	}
	im.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}

	result, err := im.Create(db, blob, pageRecordId)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 1, writeFileCalled)
	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, testUuid+".json", result)
}

func TestUnit_Create_FailsOnWritingIndexes(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	pageRecordId := "12345"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	im := createTestIndexManager(dataLocation)
	im.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	im.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	im.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		if writeFileCalled == 1 {
			return nil
		}
		return assert.AnError
	}
	im.getFileFunc = func(filePath string) ([]byte, error) {
		indexesData, _ := json.Marshal(diskModels.Indexes{})
		return indexesData, nil
	}

	result, err := im.Create(db, blob, pageRecordId)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 2, writeFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, testUuid+".json", result)
}

func TestUnit_GetAll_GetsIndexes(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	indexes := diskModels.Indexes{
		"1": diskModels.IndexItem{FileNames: []string{"file_1", "file_2"}},
		"2": diskModels.IndexItem{FileNames: []string{"file_3"}},
	}
	getFileCalled := false
	im := createTestIndexManager(dataLocation)
	im.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		indexesBytes, _ := json.Marshal(indexes)
		return indexesBytes, nil
	}

	result, err := im.GetAll(db, blob)

	assert.True(t, getFileCalled)
	assert.Nil(t, err)
	assert.Equal(t, indexes, result)
}

func TestUnit_GetAll_FailsOnGetIndexesFileError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	getFileCalled := false
	im := createTestIndexManager(dataLocation)
	im.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}

	result, err := im.GetAll(db, blob)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_GetAll_FailsOnInvalidIndexesData(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	getFileCalled := false
	im := createTestIndexManager(dataLocation)
	im.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return []byte("invalid data"), nil
	}

	result, err := im.GetAll(db, blob)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}
