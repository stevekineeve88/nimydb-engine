package diskManagers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/test/mocks"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
)

func createTestDBManager(dataLocation string) dbManager {
	return dbManager{
		dataLocation: dataLocation,
	}
}

func TestUnit_CreateDBManager_CreatesDBManager(t *testing.T) {
	dataLocation := "dataLocation"
	dbm := CreateDBManager(dataLocation)

	dbmV := reflect.ValueOf(dbm)

	assert.Equal(t, dataLocation, reflect.Indirect(dbmV).FieldByName("dataLocation").String())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateDir).Pointer(), reflect.Indirect(dbmV).FieldByName("createDirFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.DeleteDirectory).Pointer(), reflect.Indirect(dbmV).FieldByName("deleteDirFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetDirectoryContents).Pointer(), reflect.Indirect(dbmV).FieldByName("getDirContentsFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(os.Stat).Pointer(), reflect.Indirect(dbmV).FieldByName("osStatFunc").Pointer())
}

func TestUnit_Create_CreatesDB(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	called := false
	dbm := createTestDBManager(dataLocation)
	dbm.createDirFunc = func(directory string) error {
		called = true
		assert.Equal(t, fmt.Sprintf("%s/%s", dataLocation, db), directory)
		return nil
	}

	err := dbm.Create(db)

	assert.True(t, called)
	assert.Nil(t, err)
}

func TestUnit_Create_FailsOnCreateDBError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	called := false
	dbm := createTestDBManager(dataLocation)
	dbm.createDirFunc = func(directory string) error {
		called = true
		return assert.AnError
	}

	err := dbm.Create(db)

	assert.True(t, called)
	assert.NotNil(t, err)
}

func TestUnit_Delete_DeletesDB(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	called := false
	dbm := createTestDBManager(dataLocation)
	dbm.deleteDirFunc = func(directory string) error {
		called = true
		assert.Equal(t, fmt.Sprintf("%s/%s", dataLocation, db), directory)
		return nil
	}

	err := dbm.Delete(db)

	assert.True(t, called)
	assert.Nil(t, err)
}

func TestUnit_Delete_FailsOnDeleteDBError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	called := false
	dbm := createTestDBManager(dataLocation)
	dbm.deleteDirFunc = func(directory string) error {
		called = true
		return assert.AnError
	}

	err := dbm.Delete(db)

	assert.True(t, called)
	assert.NotNil(t, err)
}

func TestUnit_GetAll_GetsDBContents(t *testing.T) {
	dataLocation := "dataLocation"
	contents := []string{
		"content_1",
		"content_2",
	}
	called := false
	dbm := createTestDBManager(dataLocation)
	dbm.getDirContentsFunc = func(directory string) ([]string, error) {
		called = true
		assert.Equal(t, dataLocation, directory)
		return contents, nil
	}

	result, err := dbm.GetAll()

	assert.True(t, called)
	assert.Nil(t, err)
	assert.Equal(t, contents, result)
}

func TestUnit_GetAll_FailsOnDBContentsError(t *testing.T) {
	dataLocation := "dataLocation"
	called := false
	dbm := createTestDBManager(dataLocation)
	dbm.getDirContentsFunc = func(directory string) ([]string, error) {
		called = true
		return nil, assert.AnError
	}

	result, err := dbm.GetAll()

	assert.True(t, called)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_Exists_ExistsOnStatDB(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	called := false
	dbm := createTestDBManager(dataLocation)
	dbm.osStatFunc = func(name string) (os.FileInfo, error) {
		called = true
		assert.Equal(t, fmt.Sprintf("%s/%s", dataLocation, db), name)
		return diskMocks.MockFileInfo{}, nil
	}

	exists := dbm.Exists(db)

	assert.True(t, called)
	assert.True(t, exists)
}

func TestUnit_Exists_NotExistsOnStatDB(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	called := false
	dbm := createTestDBManager(dataLocation)
	dbm.osStatFunc = func(name string) (os.FileInfo, error) {
		called = true
		return nil, assert.AnError
	}

	exists := dbm.Exists(db)

	assert.True(t, called)
	assert.False(t, exists)
}
