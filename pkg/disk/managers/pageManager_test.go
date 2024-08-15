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

func createTestPageManager(dataLocation string) pageManager {
	return pageManager{dataLocation: dataLocation}
}

func TestUnit_CreatePageManager_CreatesPageManager(t *testing.T) {
	dataLocation := "dataLocation"
	pm := CreatePageManager(dataLocation)

	pmV := reflect.ValueOf(pm)

	assert.Equal(t, dataLocation, reflect.Indirect(pmV).FieldByName("dataLocation").String())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateFile).Pointer(), reflect.Indirect(pmV).FieldByName("createFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateDir).Pointer(), reflect.Indirect(pmV).FieldByName("createDirFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.WriteFile).Pointer(), reflect.Indirect(pmV).FieldByName("writeFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetFile).Pointer(), reflect.Indirect(pmV).FieldByName("getFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.DeleteFile).Pointer(), reflect.Indirect(pmV).FieldByName("deleteFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetUUID).Pointer(), reflect.Indirect(pmV).FieldByName("uuidFunc").Pointer())
}

func TestUnit_Initialize_InitializesPages(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	pm := createTestPageManager(dataLocation)
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, pagesFile), filePath)
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, pagesFile), filePath)
		blankBytes, _ := json.Marshal(diskModels.Pages{})
		assert.Equal(t, blankBytes, fileData)
		return nil
	}
	pm.createDirFunc = func(directory string) error {
		createDirCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, pagesDirectory), directory)
		return nil
	}

	err := pm.Initialize(db, blob)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.True(t, createDirCalled)
	assert.Nil(t, err)
}

func TestUnit_Initialize_FailsOnPagesFileCreateError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	pm := createTestPageManager(dataLocation)
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

	err := pm.Initialize(db, blob)

	assert.True(t, createFileCalled)
	assert.False(t, writeFileCalled)
	assert.False(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_Initialize_FailsOnPagesFileWriteError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	pm := createTestPageManager(dataLocation)
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

	err := pm.Initialize(db, blob)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.False(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_Initialize_FailsOnPagesDirectoryCreateError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	createFileCalled := false
	writeFileCalled := false
	createDirCalled := false
	pm := createTestPageManager(dataLocation)
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

	err := pm.Initialize(db, blob)

	assert.True(t, createFileCalled)
	assert.True(t, writeFileCalled)
	assert.True(t, createDirCalled)
	assert.NotNil(t, err)
}

func TestUnit_Create_CreatesPageFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	pm := createTestPageManager(dataLocation)
	pm.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s.json", dataLocation, db, blob, pagesDirectory, testUuid), filePath)
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		switch writeFileCalled {
		case 0:
			assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s.json", dataLocation, db, blob, pagesDirectory, testUuid), filePath)
			pageRecordsBytes, _ := json.Marshal(diskModels.PageRecords{})
			assert.Equal(t, pageRecordsBytes, fileData)
		case 1:
			assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, pagesFile), filePath)
			pagesBytes, _ := json.Marshal(diskModels.Pages{
				diskModels.PageItem{FileName: testUuid + ".json"},
			})
			assert.Equal(t, pagesBytes, fileData)
		default:
			panic("write not handled")
		}
		writeFileCalled++
		return nil
	}
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		pagesData, _ := json.Marshal(diskModels.Pages{})
		return pagesData, nil
	}

	result, err := pm.Create(db, blob)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 2, writeFileCalled)
	assert.Nil(t, err)
	assert.Equal(t, testUuid+".json", result)
}

func TestUnit_Create_FailsOnCreatingPageFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	pm := createTestPageManager(dataLocation)
	pm.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return assert.AnError
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		return nil
	}
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		pagesData, _ := json.Marshal(diskModels.Pages{})
		return pagesData, nil
	}

	result, err := pm.Create(db, blob)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 0, writeFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, "", result)
}

func TestUnit_Create_FailsOnWritingPageRecords(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	pm := createTestPageManager(dataLocation)
	pm.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		return assert.AnError
	}
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		pagesData, _ := json.Marshal(diskModels.Pages{})
		return pagesData, nil
	}

	result, err := pm.Create(db, blob)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 1, writeFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, testUuid+".json", result)
}

func TestUnit_Create_FailsOnGettingPages(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	getFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		return nil
	}
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}

	result, err := pm.Create(db, blob)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 1, writeFileCalled)
	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, testUuid+".json", result)
}

func TestUnit_Create_FailsOnWritingPages(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	testUuid := "uuid"
	uuidCalled := false
	createFileCalled := false
	writeFileCalled := 0
	pm := createTestPageManager(dataLocation)
	pm.uuidFunc = func() string {
		uuidCalled = true
		return testUuid
	}
	pm.createFileFunc = func(filePath string) error {
		createFileCalled = true
		return nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled++
		if writeFileCalled == 1 {
			return nil
		}
		return assert.AnError
	}
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		pagesData, _ := json.Marshal(diskModels.Pages{})
		return pagesData, nil
	}

	result, err := pm.Create(db, blob)

	assert.True(t, uuidCalled)
	assert.True(t, createFileCalled)
	assert.Equal(t, 2, writeFileCalled)
	assert.NotNil(t, err)
	assert.Equal(t, testUuid+".json", result)
}

func TestUnit_GetAll_GetsPages(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pages := diskModels.Pages{
		diskModels.PageItem{FileName: "file_one"},
		diskModels.PageItem{FileName: "file_two"},
	}
	getFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		pagesBytes, _ := json.Marshal(pages)
		return pagesBytes, nil
	}

	result, err := pm.GetAll(db, blob)

	assert.True(t, getFileCalled)
	assert.Nil(t, err)
	assert.Equal(t, pages, result)
}

func TestUnit_GetAll_FailsOnGetPagesFileError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	getFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}

	result, err := pm.GetAll(db, blob)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_GetAll_FailsOnInvalidPagesData(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	getFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return []byte("invalid data"), nil
	}

	result, err := pm.GetAll(db, blob)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_GetData_GetsPageRecords(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageRecords := diskModels.PageRecords{
		"id_1": {
			"col_one": "1",
			"col_two": "2",
		},
		"id_2": {
			"col_one": "2",
			"col_two": "6",
		},
	}
	pageFile := "page.json"
	getFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, pagesDirectory, pageFile), filePath)
		pageRecordsBytes, _ := json.Marshal(pageRecords)
		return pageRecordsBytes, nil
	}

	result, err := pm.GetData(db, blob, pageFile)

	assert.True(t, getFileCalled)
	assert.Nil(t, err)
	assert.Equal(t, pageRecords, result)
}

func TestUnit_GetData_FailsOnGetPageFileError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageFile := "page.json"
	getFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}

	result, err := pm.GetData(db, blob, pageFile)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_GetData_FailsOnInvalidPageRecords(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageFile := "page.json"
	getFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return []byte("invalid data"), nil
	}

	result, err := pm.GetData(db, blob, pageFile)

	assert.True(t, getFileCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_WriteData_WritesPageRecords(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageRecords := diskModels.PageRecords{
		"id_1": {
			"col_one": "1",
			"col_two": "2",
		},
		"id_2": {
			"col_one": "2",
			"col_two": "6",
		},
	}
	pageFile := "page.json"
	writeFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, pagesDirectory, pageFile), filePath)
		pageRecordsBytes, _ := json.Marshal(pageRecords)
		assert.Equal(t, pageRecordsBytes, fileData)
		return nil
	}

	err := pm.WriteData(db, blob, pageFile, pageRecords)

	assert.True(t, writeFileCalled)
	assert.Nil(t, err)
}

func TestUnit_WriteData_FailsOnWritePageRecordsError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageRecords := diskModels.PageRecords{
		"id_1": {
			"col_one": "1",
			"col_two": "2",
		},
		"id_2": {
			"col_one": "2",
			"col_two": "6",
		},
	}
	pageFile := "page.json"
	writeFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return assert.AnError
	}

	err := pm.WriteData(db, blob, pageFile, pageRecords)

	assert.True(t, writeFileCalled)
	assert.NotNil(t, err)
}

func TestUnit_Delete_DeletesPageFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageFile := "page.json"
	pages := diskModels.Pages{
		diskModels.PageItem{FileName: "file_one"},
		diskModels.PageItem{FileName: pageFile},
	}
	writeFileCalled := false
	deleteFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		pagesBytes, _ := json.Marshal(pages)
		return pagesBytes, nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, pagesFile), filePath)
		pagesBytes, _ := json.Marshal(diskModels.Pages{
			diskModels.PageItem{FileName: "file_one"},
		})
		assert.Equal(t, pagesBytes, fileData)
		return nil
	}
	pm.deleteFileFunc = func(filePath string) error {
		deleteFileCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s/%s", dataLocation, db, blob, pagesDirectory, pageFile), filePath)
		return nil
	}

	phantomFile, err := pm.Delete(db, blob, pageFile)

	assert.True(t, writeFileCalled)
	assert.True(t, deleteFileCalled)
	assert.Nil(t, err)
	assert.False(t, phantomFile)
}

func TestUnit_Delete_FailsOnGetPagesFileError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageFile := "page.json"
	writeFileCalled := false
	deleteFileCalled := false
	getFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		getFileCalled = true
		return nil, assert.AnError
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}
	pm.deleteFileFunc = func(filePath string) error {
		deleteFileCalled = true
		return nil
	}

	phantomFile, err := pm.Delete(db, blob, pageFile)

	assert.True(t, getFileCalled)
	assert.False(t, writeFileCalled)
	assert.False(t, deleteFileCalled)
	assert.NotNil(t, err)
	assert.False(t, phantomFile)
}

func TestUnit_Delete_FailsOnWritePagesFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageFile := "page.json"
	pages := diskModels.Pages{
		diskModels.PageItem{FileName: "file_one"},
		diskModels.PageItem{FileName: pageFile},
	}
	writeFileCalled := false
	deleteFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		pagesBytes, _ := json.Marshal(pages)
		return pagesBytes, nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return assert.AnError
	}
	pm.deleteFileFunc = func(filePath string) error {
		deleteFileCalled = true
		return nil
	}

	phantomFile, err := pm.Delete(db, blob, pageFile)

	assert.True(t, writeFileCalled)
	assert.False(t, deleteFileCalled)
	assert.NotNil(t, err)
	assert.False(t, phantomFile)
}

func TestUnit_Delete_FailsOnDeletePageFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	pageFile := "page.json"
	pages := diskModels.Pages{
		diskModels.PageItem{FileName: "file_one"},
		diskModels.PageItem{FileName: pageFile},
	}
	writeFileCalled := false
	deleteFileCalled := false
	pm := createTestPageManager(dataLocation)
	pm.getFileFunc = func(filePath string) ([]byte, error) {
		pagesBytes, _ := json.Marshal(pages)
		return pagesBytes, nil
	}
	pm.writeFileFunc = func(filePath string, fileData []byte) error {
		writeFileCalled = true
		return nil
	}
	pm.deleteFileFunc = func(filePath string) error {
		deleteFileCalled = true
		return assert.AnError
	}

	phantomFile, err := pm.Delete(db, blob, pageFile)

	assert.True(t, writeFileCalled)
	assert.True(t, deleteFileCalled)
	assert.NotNil(t, err)
	assert.True(t, phantomFile)
}
