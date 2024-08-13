package diskManagers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func createTestBlobManager(dataLocation string) blobManager {
	return blobManager{dataLocation: dataLocation}
}

func TestUnit_CreateBlobManager_CreatesBlobManager(t *testing.T) {
	dataLocation := "dataLocation"
	bm := CreateBlobManager(dataLocation)

	bmV := reflect.ValueOf(bm)

	assert.Equal(t, dataLocation, reflect.Indirect(bmV).FieldByName("dataLocation").String())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateDir).Pointer(), reflect.Indirect(bmV).FieldByName("createDirFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.DeleteDirectory).Pointer(), reflect.Indirect(bmV).FieldByName("deleteDirFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetDirectoryContents).Pointer(), reflect.Indirect(bmV).FieldByName("getDirContentsFunc").Pointer())
}

func TestUnit_Create_CreatesBlob(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	called := false
	bm := createTestBlobManager(dataLocation)
	bm.createDirFunc = func(directory string) error {
		called = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s", dataLocation, db, blob), directory)
		return nil
	}

	err := bm.Create(db, blob)

	assert.True(t, called)
	assert.Nil(t, err)
}

func TestUnit_Create_FailsOnCreateBlobError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	called := false
	bm := createTestBlobManager(dataLocation)
	bm.createDirFunc = func(directory string) error {
		called = true
		return assert.AnError
	}

	err := bm.Create(db, blob)

	assert.True(t, called)
	assert.NotNil(t, err)
}

func TestUnit_Delete_DeletesBlob(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	called := false
	bm := createTestBlobManager(dataLocation)
	bm.deleteDirFunc = func(directory string) error {
		called = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s", dataLocation, db, blob), directory)
		return nil
	}

	err := bm.Delete(db, blob)

	assert.True(t, called)
	assert.Nil(t, err)
}

func TestUnit_Delete_FailsOnDeleteBlobError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	called := false
	bm := createTestBlobManager(dataLocation)
	bm.deleteDirFunc = func(directory string) error {
		called = true
		return assert.AnError
	}

	err := bm.Delete(db, blob)

	assert.True(t, called)
	assert.NotNil(t, err)
}

func TestUnit_GetByDB_GetsBlobsByDB(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blobList := []string{
		"blob_1",
		"blob_2",
	}
	called := false
	bm := createTestBlobManager(dataLocation)
	bm.getDirContentsFunc = func(directory string) ([]string, error) {
		called = true
		assert.Equal(t, fmt.Sprintf("%s/%s", dataLocation, db), directory)
		return blobList, nil
	}

	result, err := bm.GetByDB(db)

	assert.True(t, called)
	assert.Nil(t, err)
	assert.Equal(t, blobList, result)
}

func TestUnit_GetByDB_FailsOnGetByDBError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	called := false
	bm := createTestBlobManager(dataLocation)
	bm.getDirContentsFunc = func(directory string) ([]string, error) {
		called = true
		return nil, assert.AnError
	}

	result, err := bm.GetByDB(db)

	assert.True(t, called)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}
