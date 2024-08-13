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

func createTestFormatManager(dataLocation string) formatManager {
	return formatManager{dataLocation: dataLocation}
}

func TestUnit_CreateFormatManager_CreatesFormatManager(t *testing.T) {
	dataLocation := "dataLocation"
	fm := CreateFormatManager(dataLocation)

	fmV := reflect.ValueOf(fm)

	assert.Equal(t, dataLocation, reflect.Indirect(fmV).FieldByName("dataLocation").String())
	assert.Equal(t, reflect.ValueOf(diskUtils.CreateFile).Pointer(), reflect.Indirect(fmV).FieldByName("createFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.WriteFile).Pointer(), reflect.Indirect(fmV).FieldByName("writeFileFunc").Pointer())
	assert.Equal(t, reflect.ValueOf(diskUtils.GetFile).Pointer(), reflect.Indirect(fmV).FieldByName("getFileFunc").Pointer())
}

func TestUnit_Create_CreatesFormatFile(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	format := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: "some_key_type"},
		"col_two": diskModels.FormatItem{KeyType: "another_key_type"},
	}
	formatByes, _ := json.Marshal(format)
	createCalled := false
	writeCalled := false
	fm := createTestFormatManager(dataLocation)
	fm.createFileFunc = func(filePath string) error {
		createCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, formatFile), filePath)
		return nil
	}
	fm.writeFileFunc = func(filePath string, fileBytes []byte) error {
		writeCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, formatFile), filePath)
		assert.Equal(t, formatByes, fileBytes)
		return nil
	}

	err := fm.Create(db, blob, format)

	assert.True(t, createCalled)
	assert.True(t, writeCalled)
	assert.Nil(t, err)
}

func TestUnit_Create_FailsOnFormatFileWriteError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	format := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: "some_key_type"},
		"col_two": diskModels.FormatItem{KeyType: "another_key_type"},
	}
	createCalled := false
	writeCalled := false
	fm := createTestFormatManager(dataLocation)
	fm.createFileFunc = func(filePath string) error {
		createCalled = true
		return nil
	}
	fm.writeFileFunc = func(filePath string, fileBytes []byte) error {
		writeCalled = true
		return assert.AnError
	}

	err := fm.Create(db, blob, format)

	assert.True(t, createCalled)
	assert.True(t, writeCalled)
	assert.NotNil(t, err)
}

func TestUnit_Get_GetsFormat(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	format := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: "some_key_type"},
		"col_two": diskModels.FormatItem{KeyType: "another_key_type"},
	}
	getCalled := false
	fm := createTestFormatManager(dataLocation)
	fm.getFileFunc = func(filePath string) ([]byte, error) {
		getCalled = true
		assert.Equal(t, fmt.Sprintf("%s/%s/%s/%s", dataLocation, db, blob, formatFile), filePath)
		formatBytes, _ := json.Marshal(format)
		return formatBytes, nil
	}

	result, err := fm.Get(db, blob)

	assert.True(t, getCalled)
	assert.Nil(t, err)
	assert.Equal(t, format, result)
}

func TestUnit_Get_FailsOnGetFormatError(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	getCalled := false
	fm := createTestFormatManager(dataLocation)
	fm.getFileFunc = func(filePath string) ([]byte, error) {
		getCalled = true
		return nil, assert.AnError
	}

	result, err := fm.Get(db, blob)

	assert.True(t, getCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_Get_FailsOnInvalidFormatBytes(t *testing.T) {
	dataLocation := "dataLocation"
	db := "db"
	blob := "blob"
	getCalled := false
	fm := createTestFormatManager(dataLocation)
	fm.getFileFunc = func(filePath string) ([]byte, error) {
		getCalled = true
		return []byte("invalid format"), nil
	}

	result, err := fm.Get(db, blob)

	assert.True(t, getCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}
