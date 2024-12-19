package memoryModels

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/managers"
	diskModels "github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	memoryConstants "github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	testUtils "github.com/stevekineeve88/nimydb-engine/pkg/test/utils"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
)

func createTestBlobMap(db string, dataLocation string, dataCaching bool, m sync.Locker) BlobMap {
	return BlobMap{
		m:               m,
		itemMap:         make(map[string]*Blob),
		db:              db,
		dataLocation:    dataLocation,
		dataCaching:     dataCaching,
		blobDiskManager: diskManagers.MockBlobManagerInstance,
	}
}

func createTestBlob(db string, blob string, m sync.Locker) Blob {
	return Blob{
		m:                    m,
		blob:                 blob,
		db:                   db,
		indexDiskManager:     diskManagers.MockIndexManagerInstance,
		partitionDiskManager: diskManagers.MockPartitionManagerInstance,
	}
}

func TestUnit_NewBlobMap_CreatesBlobMap(t *testing.T) {
	db := "db"
	dataLocation := "dataLocation"
	blobMap := NewBlobMap(db, dataLocation, true)

	assert.Equal(t, db, blobMap.db)
	assert.Equal(t, dataLocation, blobMap.dataLocation)
	assert.True(t, blobMap.dataCaching)
	assert.NotNil(t, blobMap.m)
	assert.Equal(t, make(map[string]*Blob), blobMap.itemMap)
	assert.Equal(t, reflect.ValueOf(InitializeBlob).Pointer(), reflect.ValueOf(blobMap.initializeBlobFunc).Pointer())
	assert.Equal(t, reflect.ValueOf(CreateBlob).Pointer(), reflect.ValueOf(blobMap.createBlobFunc).Pointer())
}

func TestUnit_Add_AddsBlobToMap(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_1": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_2": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	expectedPartition := diskModels.Partition{Keys: []string{"col_1"}}
	initializeBlobCalled := false
	lockedCalled := false
	unlockedCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.initializeBlobFunc = func(db string, blob string, dataLocation string, format diskModels.Format, partition *diskModels.Partition, dataCaching bool) (Blob, error) {
		initializeBlobCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		assert.Equal(t, expectedFormat, format)
		assert.Equal(t, expectedFormat, format)
		assert.Equal(t, expectedPartition, *partition)
		assert.Equal(t, blobMap.dataCaching, dataCaching)
		return Blob{}, nil
	}

	_, err := blobMap.Add(expectedBlob, expectedFormat, &expectedPartition)

	assert.True(t, lockedCalled)
	assert.True(t, unlockedCalled)
	assert.True(t, initializeBlobCalled)
	assert.Nil(t, err)
	_, ok := blobMap.itemMap[expectedBlob]
	assert.True(t, ok)
}

func TestUnit_Add_FailsOnInitializeBlobError(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_1": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_2": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	expectedPartition := diskModels.Partition{Keys: []string{"col_1"}}
	initializeBlobCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.initializeBlobFunc = func(db string, blob string, dataLocation string, format diskModels.Format, partition *diskModels.Partition, dataCaching bool) (Blob, error) {
		initializeBlobCalled = true
		return Blob{}, assert.AnError
	}

	_, err := blobMap.Add(expectedBlob, expectedFormat, &expectedPartition)

	assert.True(t, initializeBlobCalled)
	assert.NotNil(t, err)
	_, ok := blobMap.itemMap[expectedBlob]
	assert.False(t, ok)
}

func TestUnit_Get_GetsBlob(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlob := "blob"
	createBlobCalled := false
	lockedCalled := false
	unlockedCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.itemMap[expectedBlob] = &Blob{}
	blobMap.createBlobFunc = func(db string, blob string, dataLocation string, dataCaching bool) (Blob, error) {
		createBlobCalled = true
		return Blob{}, nil
	}

	result, err := blobMap.Get(expectedBlob)

	assert.True(t, lockedCalled)
	assert.True(t, unlockedCalled)
	assert.False(t, createBlobCalled)
	assert.Nil(t, err)
	expectedBlobPointer := blobMap.itemMap[expectedBlob]
	assert.Equal(t, &expectedBlobPointer, &result)
}

func TestUnit_Get_GetsFromDiskIfNotFound(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlob := "blob"
	createBlobCalled := false
	lockedCalled := false
	unlockedCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.createBlobFunc = func(db string, blob string, dataLocation string, dataCaching bool) (Blob, error) {
		createBlobCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		assert.Equal(t, expectedDataLocation, dataLocation)
		assert.Equal(t, blobMap.dataCaching, dataCaching)
		return Blob{}, nil
	}

	result, err := blobMap.Get(expectedBlob)

	assert.True(t, lockedCalled)
	assert.True(t, unlockedCalled)
	assert.True(t, createBlobCalled)
	assert.Nil(t, err)
	expectedBlobPointer := blobMap.itemMap[expectedBlob]
	assert.Equal(t, &expectedBlobPointer, &result)
}

func TestUnit_Get_FailsOnGetBlobError(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlob := "blob"
	createBlobCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.createBlobFunc = func(db string, blob string, dataLocation string, dataCaching bool) (Blob, error) {
		createBlobCalled = true
		return Blob{}, assert.AnError
	}

	result, err := blobMap.Get(expectedBlob)

	assert.True(t, createBlobCalled)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestUnit_Delete_DeletesBlob(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlob := "blob"
	deleteCalled := false
	lockedCalled := false
	unlockedCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.itemMap[expectedBlob] = &Blob{}
	diskManagers.MockBlobManagerInstance.DeleteFunc = func(db string, blob string) error {
		deleteCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return nil
	}

	fmt.Println(diskManagers.MockBlobManagerInstance.DeleteFunc(expectedDB, expectedBlob))
	err := blobMap.Delete(expectedBlob)

	assert.True(t, lockedCalled)
	assert.True(t, unlockedCalled)
	assert.True(t, deleteCalled)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(blobMap.itemMap))
}

func TestUnit_Delete_FailsOnDeleteBlobError(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlob := "blob"
	deleteCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.itemMap[expectedBlob] = &Blob{}
	diskManagers.MockBlobManagerInstance.DeleteFunc = func(db string, blob string) error {
		deleteCalled = true
		return assert.AnError
	}

	err := blobMap.Delete(expectedBlob)

	assert.True(t, deleteCalled)
	assert.NotNil(t, err)
	assert.Equal(t, 1, len(blobMap.itemMap))
}

func TestUnit_Remove_RemovesBlobFromMap(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlob := "blob"
	lockedCalled := false
	unlockedCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.itemMap[expectedBlob] = &Blob{}

	blobMap.Remove(expectedBlob)

	assert.True(t, lockedCalled)
	assert.True(t, unlockedCalled)
	assert.Equal(t, 0, len(blobMap.itemMap))
}

func TestUnit_ConvertToPageRecords_ConvertsMapToPageRecords(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlobs := []string{
		"blob_one",
		"blob_two",
	}
	getByDBCalled := false
	lockedCalled := 0
	unlockedCalled := 0
	m := testUtils.CreateMockMutex(func() {
		lockedCalled++
	}, func() {
		unlockedCalled++
	})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.itemMap[expectedBlobs[0]] = &Blob{
		format: diskModels.Format{
			"col_1": diskModels.FormatItem{KeyType: memoryConstants.String},
		},
		partition: diskModels.Partition{Keys: []string{"col_1"}},
	}
	blobMap.itemMap[expectedBlobs[1]] = &Blob{
		format: diskModels.Format{
			"col_2": diskModels.FormatItem{KeyType: memoryConstants.Int},
		},
	}
	diskManagers.MockBlobManagerInstance.GetByDBFunc = func(db string) ([]string, error) {
		getByDBCalled = true
		assert.Equal(t, expectedDB, db)
		return expectedBlobs, nil
	}

	pageRecords := blobMap.ConvertToPageRecords()

	assert.True(t, getByDBCalled)
	assert.Equal(t, 2, lockedCalled)
	assert.Equal(t, 2, unlockedCalled)
	assert.Equal(t, len(expectedBlobs), len(pageRecords))
	for i := 0; i < len(expectedBlobs); i++ {
		assert.Equal(t, expectedBlobs[i], pageRecords[i]["name"].(string))
		assert.Equal(t, blobMap.itemMap[expectedBlobs[i]].format.ConvertToPageRecords(), pageRecords[i]["format"])
		assert.Equal(t, blobMap.itemMap[expectedBlobs[i]].partition.ConvertToPageRecords(), pageRecords[i]["partition"])
	}
}

func TestUnit_ConvertToPageRecords_SkipsUnknownBlob(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	expectedBlobs := []string{
		"blob_one",
		"blob_two",
	}
	getByDBCalled := false
	lockedCalled := 0
	unlockedCalled := 0
	createBlobCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled++
	}, func() {
		unlockedCalled++
	})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	blobMap.itemMap[expectedBlobs[1]] = &Blob{
		format: diskModels.Format{
			"col_2": diskModels.FormatItem{KeyType: memoryConstants.Int},
		},
	}
	blobMap.createBlobFunc = func(db string, blob string, dataLocation string, dataCaching bool) (Blob, error) {
		createBlobCalled = true
		assert.Equal(t, expectedBlobs[0], blob)
		return Blob{}, assert.AnError
	}
	diskManagers.MockBlobManagerInstance.GetByDBFunc = func(db string) ([]string, error) {
		getByDBCalled = true
		return expectedBlobs, nil
	}

	pageRecords := blobMap.ConvertToPageRecords()

	assert.True(t, getByDBCalled)
	assert.True(t, createBlobCalled)
	assert.Equal(t, 2, lockedCalled)
	assert.Equal(t, 2, unlockedCalled)
	assert.Equal(t, 1, len(pageRecords))
	assert.Equal(t, expectedBlobs[1], pageRecords[0]["name"].(string))
}

func TestUnit_ConvertToPageRecords_FailsOnGetDBError(t *testing.T) {
	expectedDB := "db"
	expectedDataLocation := "dataLocation"
	getByDBCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	blobMap := createTestBlobMap(expectedDB, expectedDataLocation, true, m)
	diskManagers.MockBlobManagerInstance.GetByDBFunc = func(db string) ([]string, error) {
		getByDBCalled = true
		return nil, assert.AnError
	}

	pageRecords := blobMap.ConvertToPageRecords()

	assert.True(t, getByDBCalled)
	assert.Equal(t, 0, len(pageRecords))
}

func TestUnit_CreateBlob_CreatesBlob(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
	}
	expectedPartition := diskModels.Partition{Keys: []string{"col_one"}}
	expectedHashKeys := []string{
		"hashKeyFile.json",
	}
	getFormatCalled := false
	getAllPagesCalled := false
	getAllIndexesCalled := false
	getPartitionCalled := false
	getAllHashKeysCalled := false
	getByHashKeyCalled := false
	diskManagers.MockFormatManagerInstance.GetFunc = func(db string, blob string) (diskModels.Format, error) {
		getFormatCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return expectedFormat, nil
	}
	diskManagers.MockPageManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Pages, error) {
		getAllPagesCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return diskModels.Pages{}, nil
	}
	diskManagers.MockIndexManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Indexes, error) {
		getAllIndexesCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return diskModels.Indexes{}, nil
	}
	diskManagers.MockPartitionManagerInstance.GetPartitionFunc = func(db string, blob string) (diskModels.Partition, error) {
		getPartitionCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return expectedPartition, nil
	}
	diskManagers.MockPartitionManagerInstance.GetAllFunc = func(db string, blob string) ([]string, error) {
		getAllHashKeysCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return expectedHashKeys, nil
	}
	diskManagers.MockPartitionManagerInstance.GetByHashKeyFunc = func(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error) {
		getByHashKeyCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		assert.Equal(t, expectedHashKeys[0], hashKeyFileName)
		return diskModels.PartitionPages{}, nil
	}

	result, err := CreateBlob(expectedDB, expectedBlob, dataLocation, true)

	assert.True(t, getFormatCalled)
	assert.True(t, getAllPagesCalled)
	assert.True(t, getAllIndexesCalled)
	assert.True(t, getPartitionCalled)
	assert.True(t, getAllHashKeysCalled)
	assert.True(t, getByHashKeyCalled)
	assert.Nil(t, err)
	assert.Equal(t, expectedBlob, result.blob)
	assert.Equal(t, expectedDB, result.db)
	assert.Equal(t, expectedPartition, result.partition)
	assert.Equal(t, expectedFormat, result.format)
}

func TestUnit_CreateBlob_CreatesBlobWithoutPartition(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
	}
	expectedHashKeys := []string{
		"hashKeyFile.json",
	}
	getFormatCalled := false
	getAllPagesCalled := false
	getAllIndexesCalled := false
	getPartitionCalled := false
	getAllHashKeysCalled := false
	getByHashKeyCalled := false
	diskManagers.MockFormatManagerInstance.GetFunc = func(db string, blob string) (diskModels.Format, error) {
		getFormatCalled = true
		return expectedFormat, nil
	}
	diskManagers.MockPageManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Pages, error) {
		getAllPagesCalled = true
		return diskModels.Pages{}, nil
	}
	diskManagers.MockIndexManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Indexes, error) {
		getAllIndexesCalled = true
		return diskModels.Indexes{}, nil
	}
	diskManagers.MockPartitionManagerInstance.GetPartitionFunc = func(db string, blob string) (diskModels.Partition, error) {
		getPartitionCalled = true
		return diskModels.Partition{}, assert.AnError
	}
	diskManagers.MockPartitionManagerInstance.GetAllFunc = func(db string, blob string) ([]string, error) {
		getAllHashKeysCalled = true
		return expectedHashKeys, nil
	}
	diskManagers.MockPartitionManagerInstance.GetByHashKeyFunc = func(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error) {
		getByHashKeyCalled = true
		return diskModels.PartitionPages{}, nil
	}

	result, err := CreateBlob(expectedDB, expectedBlob, dataLocation, true)

	assert.True(t, getFormatCalled)
	assert.True(t, getAllPagesCalled)
	assert.True(t, getAllIndexesCalled)
	assert.True(t, getPartitionCalled)
	assert.False(t, getAllHashKeysCalled)
	assert.False(t, getByHashKeyCalled)
	assert.Nil(t, err)
	assert.Equal(t, expectedBlob, result.blob)
	assert.Equal(t, expectedDB, result.db)
	assert.Equal(t, diskModels.Partition{}, result.partition)
	assert.Equal(t, expectedFormat, result.format)
}

func TestUnit_CreateBlob_FailsOnGetFormat(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	getFormatCalled := false
	getAllPagesCalled := false
	getAllIndexesCalled := false
	getPartitionCalled := false
	diskManagers.MockFormatManagerInstance.GetFunc = func(db string, blob string) (diskModels.Format, error) {
		getFormatCalled = true
		return diskModels.Format{}, assert.AnError
	}
	diskManagers.MockPageManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Pages, error) {
		getAllPagesCalled = true
		return diskModels.Pages{}, nil
	}
	diskManagers.MockIndexManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Indexes, error) {
		getAllIndexesCalled = true
		return diskModels.Indexes{}, nil
	}
	diskManagers.MockPartitionManagerInstance.GetPartitionFunc = func(db string, blob string) (diskModels.Partition, error) {
		getPartitionCalled = true
		return diskModels.Partition{}, nil
	}

	_, err := CreateBlob(expectedDB, expectedBlob, dataLocation, true)

	assert.True(t, getFormatCalled)
	assert.False(t, getAllPagesCalled)
	assert.False(t, getAllIndexesCalled)
	assert.False(t, getPartitionCalled)
	assert.NotNil(t, err)
}

func TestUnit_CreateBlob_FailsOnInitializePagesError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	getFormatCalled := false
	getAllPagesCalled := false
	getAllIndexesCalled := false
	getPartitionCalled := false
	diskManagers.MockFormatManagerInstance.GetFunc = func(db string, blob string) (diskModels.Format, error) {
		getFormatCalled = true
		return diskModels.Format{}, nil
	}
	diskManagers.MockPageManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Pages, error) {
		getAllPagesCalled = true
		return diskModels.Pages{}, assert.AnError
	}
	diskManagers.MockIndexManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Indexes, error) {
		getAllIndexesCalled = true
		return diskModels.Indexes{}, nil
	}
	diskManagers.MockPartitionManagerInstance.GetPartitionFunc = func(db string, blob string) (diskModels.Partition, error) {
		getPartitionCalled = true
		return diskModels.Partition{}, nil
	}

	_, err := CreateBlob(expectedDB, expectedBlob, dataLocation, true)

	assert.True(t, getFormatCalled)
	assert.True(t, getAllPagesCalled)
	assert.False(t, getAllIndexesCalled)
	assert.False(t, getPartitionCalled)
	assert.NotNil(t, err)
}

func TestUnit_CreateBlob_FailsOnInitializeIndexesError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	getFormatCalled := false
	getAllPagesCalled := false
	getAllIndexesCalled := false
	getPartitionCalled := false
	diskManagers.MockFormatManagerInstance.GetFunc = func(db string, blob string) (diskModels.Format, error) {
		getFormatCalled = true
		return diskModels.Format{}, nil
	}
	diskManagers.MockPageManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Pages, error) {
		getAllPagesCalled = true
		return diskModels.Pages{}, nil
	}
	diskManagers.MockIndexManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Indexes, error) {
		getAllIndexesCalled = true
		return diskModels.Indexes{}, assert.AnError
	}
	diskManagers.MockPartitionManagerInstance.GetPartitionFunc = func(db string, blob string) (diskModels.Partition, error) {
		getPartitionCalled = true
		return diskModels.Partition{}, nil
	}

	_, err := CreateBlob(expectedDB, expectedBlob, dataLocation, true)

	assert.True(t, getFormatCalled)
	assert.True(t, getAllPagesCalled)
	assert.True(t, getAllIndexesCalled)
	assert.False(t, getPartitionCalled)
	assert.NotNil(t, err)
}

func TestUnit_CreateBlob_FailsOnInitializePartitionsError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	getFormatCalled := false
	getAllPagesCalled := false
	getAllIndexesCalled := false
	getPartitionCalled := false
	diskManagers.MockFormatManagerInstance.GetFunc = func(db string, blob string) (diskModels.Format, error) {
		getFormatCalled = true
		return diskModels.Format{}, nil
	}
	diskManagers.MockPageManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Pages, error) {
		getAllPagesCalled = true
		return diskModels.Pages{}, nil
	}
	diskManagers.MockIndexManagerInstance.GetAllFunc = func(db string, blob string) (diskModels.Indexes, error) {
		getAllIndexesCalled = true
		return diskModels.Indexes{}, nil
	}
	diskManagers.MockPartitionManagerInstance.GetPartitionFunc = func(db string, blob string) (diskModels.Partition, error) {
		getPartitionCalled = true
		return diskModels.Partition{}, nil
	}
	diskManagers.MockPartitionManagerInstance.GetAllFunc = func(db string, blob string) ([]string, error) {
		return []string{}, assert.AnError
	}

	_, err := CreateBlob(expectedDB, expectedBlob, dataLocation, true)

	assert.True(t, getFormatCalled)
	assert.True(t, getAllPagesCalled)
	assert.True(t, getAllIndexesCalled)
	assert.True(t, getPartitionCalled)
	assert.NotNil(t, err)
}

func TestUnit_InitializeBlob_InitializesBlob(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	expectedPartition := diskModels.Partition{Keys: []string{"col_one"}}
	createBlobCalled := false
	createFormatCalled := false
	initializePagesCalled := false
	initializeIndexesCalled := false
	initializePartitionsCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return nil
	}
	diskManagers.MockFormatManagerInstance.CreateFunc = func(db string, blob string, format diskModels.Format) error {
		createFormatCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		assert.Equal(t, expectedFormat, format)
		return nil
	}
	diskManagers.MockPageManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializePagesCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return nil
	}
	diskManagers.MockIndexManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializeIndexesCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return nil
	}
	diskManagers.MockPartitionManagerInstance.InitializeFunc = func(db string, blob string, partition diskModels.Partition) error {
		initializePartitionsCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		assert.Equal(t, expectedPartition, partition)
		return nil
	}

	result, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, &expectedPartition, true)

	assert.True(t, createBlobCalled)
	assert.True(t, createFormatCalled)
	assert.True(t, initializePagesCalled)
	assert.True(t, initializeIndexesCalled)
	assert.True(t, initializePartitionsCalled)
	assert.Nil(t, err)
	assert.Equal(t, expectedDB, result.db)
	assert.Equal(t, expectedBlob, result.blob)
	assert.Equal(t, expectedFormat, result.format)
	assert.Equal(t, expectedPartition, result.partition)
}

func TestUnit_InitializeBlob_InitializesBlobWithoutPartition(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	createBlobCalled := false
	createFormatCalled := false
	initializePagesCalled := false
	initializeIndexesCalled := false
	initializePartitionsCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return nil
	}
	diskManagers.MockFormatManagerInstance.CreateFunc = func(db string, blob string, format diskModels.Format) error {
		createFormatCalled = true
		return nil
	}
	diskManagers.MockPageManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializePagesCalled = true
		return nil
	}
	diskManagers.MockIndexManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializeIndexesCalled = true
		return nil
	}
	diskManagers.MockPartitionManagerInstance.InitializeFunc = func(db string, blob string, partition diskModels.Partition) error {
		initializePartitionsCalled = true
		return nil
	}

	result, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, nil, true)

	assert.True(t, createBlobCalled)
	assert.True(t, createFormatCalled)
	assert.True(t, initializePagesCalled)
	assert.True(t, initializeIndexesCalled)
	assert.False(t, initializePartitionsCalled)
	assert.Nil(t, err)
	assert.Equal(t, expectedDB, result.db)
	assert.Equal(t, expectedBlob, result.blob)
	assert.Equal(t, expectedFormat, result.format)
	assert.Equal(t, diskModels.Partition{}, result.partition)
}

func TestUnit_InitializeBlob_FailsOnPartitionStructureError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	expectedPartition := diskModels.Partition{Keys: []string{"wrong_column"}}
	createBlobCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return nil
	}

	_, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, &expectedPartition, true)

	assert.False(t, createBlobCalled)
	assert.NotNil(t, err)
}

func TestUnit_InitializeBlob_FailsOnBlobNamingConventionError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "NotAllowed"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	createBlobCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return nil
	}

	_, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, nil, true)

	assert.False(t, createBlobCalled)
	assert.NotNil(t, err)
}

func TestUnit_InitializeBlob_FailsOnFormatStructureError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one":    diskModels.FormatItem{KeyType: memoryConstants.String},
		"NotAllowed": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	createBlobCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return nil
	}

	_, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, nil, true)

	assert.False(t, createBlobCalled)
	assert.NotNil(t, err)
}

func TestUnit_InitializeBlob_FailsOnBlobCreationError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	createBlobCalled := false
	createFormatCalled := false
	initializePagesCalled := false
	initializeIndexesCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return assert.AnError
	}
	diskManagers.MockFormatManagerInstance.CreateFunc = func(db string, blob string, format diskModels.Format) error {
		createFormatCalled = true
		return nil
	}
	diskManagers.MockPageManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializePagesCalled = true
		return nil
	}
	diskManagers.MockIndexManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializeIndexesCalled = true
		return nil
	}

	_, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, nil, true)

	assert.True(t, createBlobCalled)
	assert.False(t, createFormatCalled)
	assert.False(t, initializePagesCalled)
	assert.False(t, initializeIndexesCalled)
	assert.NotNil(t, err)
}

func TestUnit_InitializeBlob_FailsOnFormatCreationError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	createBlobCalled := false
	createFormatCalled := false
	initializePagesCalled := false
	initializeIndexesCalled := false
	deleteBlobCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return nil
	}
	diskManagers.MockFormatManagerInstance.CreateFunc = func(db string, blob string, format diskModels.Format) error {
		createFormatCalled = true
		return assert.AnError
	}
	diskManagers.MockPageManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializePagesCalled = true
		return nil
	}
	diskManagers.MockIndexManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializeIndexesCalled = true
		return nil
	}
	diskManagers.MockBlobManagerInstance.DeleteFunc = func(db string, blob string) error {
		deleteBlobCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return nil
	}

	_, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, nil, true)

	assert.True(t, createBlobCalled)
	assert.True(t, createFormatCalled)
	assert.True(t, deleteBlobCalled)
	assert.False(t, initializePagesCalled)
	assert.False(t, initializeIndexesCalled)
	assert.NotNil(t, err)
}

func TestUnit_InitializeBlob_FailsOnPagesInitError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	createBlobCalled := false
	createFormatCalled := false
	initializePagesCalled := false
	initializeIndexesCalled := false
	deleteBlobCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return nil
	}
	diskManagers.MockFormatManagerInstance.CreateFunc = func(db string, blob string, format diskModels.Format) error {
		createFormatCalled = true
		return nil
	}
	diskManagers.MockPageManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializePagesCalled = true
		return assert.AnError
	}
	diskManagers.MockIndexManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializeIndexesCalled = true
		return nil
	}
	diskManagers.MockBlobManagerInstance.DeleteFunc = func(db string, blob string) error {
		deleteBlobCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return nil
	}

	_, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, nil, true)

	assert.True(t, createBlobCalled)
	assert.True(t, createFormatCalled)
	assert.True(t, deleteBlobCalled)
	assert.True(t, initializePagesCalled)
	assert.True(t, deleteBlobCalled)
	assert.False(t, initializeIndexesCalled)
	assert.NotNil(t, err)
}

func TestUnit_InitializeBlob_FailsOnIndexesInitError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	createBlobCalled := false
	createFormatCalled := false
	initializePagesCalled := false
	initializeIndexesCalled := false
	deleteBlobCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return nil
	}
	diskManagers.MockFormatManagerInstance.CreateFunc = func(db string, blob string, format diskModels.Format) error {
		createFormatCalled = true
		return nil
	}
	diskManagers.MockPageManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializePagesCalled = true
		return nil
	}
	diskManagers.MockIndexManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializeIndexesCalled = true
		return assert.AnError
	}
	diskManagers.MockBlobManagerInstance.DeleteFunc = func(db string, blob string) error {
		deleteBlobCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return nil
	}

	_, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, nil, true)

	assert.True(t, createBlobCalled)
	assert.True(t, createFormatCalled)
	assert.True(t, deleteBlobCalled)
	assert.True(t, initializePagesCalled)
	assert.True(t, initializeIndexesCalled)
	assert.True(t, deleteBlobCalled)
	assert.NotNil(t, err)
}

func TestUnit_InitializeBlob_FailsOnPartitionsInitError(t *testing.T) {
	dataLocation := "dataLocation"
	expectedDB := "db"
	expectedBlob := "blob"
	expectedFormat := diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	expectedPartition := diskModels.Partition{Keys: []string{"col_one"}}
	createBlobCalled := false
	createFormatCalled := false
	initializePagesCalled := false
	initializeIndexesCalled := false
	initializePartitionsCalled := false
	deleteBlobCalled := false
	diskManagers.MockBlobManagerInstance.CreateFunc = func(db string, blob string) error {
		createBlobCalled = true
		return nil
	}
	diskManagers.MockFormatManagerInstance.CreateFunc = func(db string, blob string, format diskModels.Format) error {
		createFormatCalled = true
		return nil
	}
	diskManagers.MockPageManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializePagesCalled = true
		return nil
	}
	diskManagers.MockIndexManagerInstance.InitializeFunc = func(db string, blob string) error {
		initializeIndexesCalled = true
		return nil
	}
	diskManagers.MockPartitionManagerInstance.InitializeFunc = func(db string, blob string, partition diskModels.Partition) error {
		initializePartitionsCalled = true
		return assert.AnError
	}
	diskManagers.MockBlobManagerInstance.DeleteFunc = func(db string, blob string) error {
		deleteBlobCalled = true
		assert.Equal(t, expectedDB, db)
		assert.Equal(t, expectedBlob, blob)
		return nil
	}

	_, err := InitializeBlob(expectedDB, expectedBlob, dataLocation, expectedFormat, &expectedPartition, true)

	assert.True(t, createBlobCalled)
	assert.True(t, createFormatCalled)
	assert.True(t, deleteBlobCalled)
	assert.True(t, initializePagesCalled)
	assert.True(t, initializeIndexesCalled)
	assert.True(t, initializePartitionsCalled)
	assert.True(t, deleteBlobCalled)
	assert.NotNil(t, err)
}

func TestUnit_GetByRecordId_GetsRecord(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
	}
	indexPrefix := pageRecordId[0:1]
	lockedCalled := false
	unlockedCalled := false
	getByPrefixCalled := false
	indexReadCalled := false
	getPageCalled := false
	pageReadCalled := false
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return indexPrefix
	}
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	indexes := []IndexI{
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled = true
				return diskModels.IndexRecords{
					pageRecordId: pageFileName,
				}, nil
			},
		},
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
	}
	mockIndexMap := &MockIndexMap{
		GetByPrefixFunc: func(prefix string) ([]IndexI, error) {
			getByPrefixCalled = true
			assert.Equal(t, indexPrefix, prefix)
			return indexes, nil
		},
	}
	mockPageMap := &MockPageMap{
		GetFunc: func(fileName string) (PageI, error) {
			getPageCalled = true
			assert.Equal(t, pageFileName, fileName)
			return page, nil
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.indexMap = mockIndexMap
	b.pageMap = mockPageMap

	pageRecordsMap, err := b.GetByRecordId(pageRecordId)

	assert.True(t, getByPrefixCalled)
	assert.True(t, indexReadCalled)
	assert.True(t, getPageCalled)
	assert.True(t, pageReadCalled)
	assert.False(t, lockedCalled)
	assert.False(t, unlockedCalled)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(pageRecordsMap))
	assert.Equal(t, 1, len(pageRecordsMap[pageFileName]))
	assert.Equal(t, pageRecord, pageRecordsMap[pageFileName])
}

func TestUnit_GetByRecordId_ReturnsEmptyIfNotFound(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
	}
	indexPrefix := pageRecordId[0:1]
	getByPrefixCalled := false
	indexReadCalled := false
	getPageCalled := false
	pageReadCalled := false
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return indexPrefix
	}
	m := testUtils.CreateMockMutex(func() {}, func() {})
	indexes := []IndexI{
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled = true
				return diskModels.IndexRecords{
					pageRecordId: pageFileName,
				}, nil
			},
		},
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
	}
	mockIndexMap := &MockIndexMap{
		GetByPrefixFunc: func(prefix string) ([]IndexI, error) {
			getByPrefixCalled = true
			return indexes, nil
		},
	}
	mockPageMap := &MockPageMap{
		GetFunc: func(fileName string) (PageI, error) {
			getPageCalled = true
			return page, nil
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.indexMap = mockIndexMap
	b.pageMap = mockPageMap

	pageRecordsMap, err := b.GetByRecordId("wrong_record_id")

	assert.True(t, getByPrefixCalled)
	assert.True(t, indexReadCalled)
	assert.False(t, getPageCalled)
	assert.False(t, pageReadCalled)
	assert.Nil(t, err)

	assert.Equal(t, 0, len(pageRecordsMap))
}

func TestUnit_GetByRecordId_SkipsNullIndexPointer(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
	}
	indexPrefix := pageRecordId[0:1]
	getByPrefixCalled := false
	indexReadCalled := 0
	getPageCalled := false
	pageReadCalled := false
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return indexPrefix
	}
	m := testUtils.CreateMockMutex(func() {}, func() {})
	indexes := []IndexI{
		nil,
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled++
				return diskModels.IndexRecords{
					pageRecordId: pageFileName,
				}, nil
			},
		},
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
	}
	mockIndexMap := &MockIndexMap{
		GetByPrefixFunc: func(prefix string) ([]IndexI, error) {
			getByPrefixCalled = true
			return indexes, nil
		},
	}
	mockPageMap := &MockPageMap{
		GetFunc: func(fileName string) (PageI, error) {
			getPageCalled = true
			return page, nil
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.indexMap = mockIndexMap
	b.pageMap = mockPageMap

	_, err := b.GetByRecordId(pageRecordId)

	assert.True(t, getByPrefixCalled)
	assert.Equal(t, 1, indexReadCalled)
	assert.True(t, getPageCalled)
	assert.True(t, pageReadCalled)
	assert.Nil(t, err)
}

func TestUnit_GetByRecordId_SkipsOnFailedIndexRead(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
	}
	indexPrefix := pageRecordId[0:1]
	getByPrefixCalled := false
	indexReadCalled := 0
	getPageCalled := false
	pageReadCalled := false
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return indexPrefix
	}
	m := testUtils.CreateMockMutex(func() {}, func() {})
	indexes := []IndexI{
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled++
				return nil, assert.AnError
			},
		},
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled++
				return diskModels.IndexRecords{
					pageRecordId: pageFileName,
				}, nil
			},
		},
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
	}
	mockIndexMap := &MockIndexMap{
		GetByPrefixFunc: func(prefix string) ([]IndexI, error) {
			getByPrefixCalled = true
			return indexes, nil
		},
	}
	mockPageMap := &MockPageMap{
		GetFunc: func(fileName string) (PageI, error) {
			getPageCalled = true
			return page, nil
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.indexMap = mockIndexMap
	b.pageMap = mockPageMap

	result, err := b.GetByRecordId(pageRecordId)

	assert.True(t, getByPrefixCalled)
	assert.Equal(t, 2, indexReadCalled)
	assert.True(t, getPageCalled)
	assert.True(t, pageReadCalled)
	assert.Nil(t, err)
	assert.Equal(t, pageRecord, result[pageFileName])
}

func TestUnit_GetByRecordId_FailsOnPageGetError(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
	}
	indexPrefix := pageRecordId[0:1]
	getByPrefixCalled := false
	indexReadCalled := false
	getPageCalled := false
	pageReadCalled := false
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return indexPrefix
	}
	m := testUtils.CreateMockMutex(func() {}, func() {})
	indexes := []IndexI{
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled = true
				return diskModels.IndexRecords{
					pageRecordId: pageFileName,
				}, nil
			},
		},
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
	}
	mockIndexMap := &MockIndexMap{
		GetByPrefixFunc: func(prefix string) ([]IndexI, error) {
			getByPrefixCalled = true
			return indexes, nil
		},
	}
	mockPageMap := &MockPageMap{
		GetFunc: func(fileName string) (PageI, error) {
			getPageCalled = true
			return page, assert.AnError
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.indexMap = mockIndexMap
	b.pageMap = mockPageMap

	result, err := b.GetByRecordId(pageRecordId)

	assert.True(t, getByPrefixCalled)
	assert.True(t, indexReadCalled)
	assert.True(t, getPageCalled)
	assert.False(t, pageReadCalled)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestUnit_GetByRecordId_FailsOnPageReadError(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	indexPrefix := pageRecordId[0:1]
	getByPrefixCalled := false
	indexReadCalled := false
	getPageCalled := false
	pageReadCalled := false
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return indexPrefix
	}
	m := testUtils.CreateMockMutex(func() {}, func() {})
	indexes := []IndexI{
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled = true
				return diskModels.IndexRecords{
					pageRecordId: pageFileName,
				}, nil
			},
		},
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return nil, assert.AnError
		},
	}
	mockIndexMap := &MockIndexMap{
		GetByPrefixFunc: func(prefix string) ([]IndexI, error) {
			getByPrefixCalled = true
			return indexes, nil
		},
	}
	mockPageMap := &MockPageMap{
		GetFunc: func(fileName string) (PageI, error) {
			getPageCalled = true
			return page, nil
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.indexMap = mockIndexMap
	b.pageMap = mockPageMap

	result, err := b.GetByRecordId(pageRecordId)

	assert.True(t, getByPrefixCalled)
	assert.True(t, indexReadCalled)
	assert.True(t, getPageCalled)
	assert.True(t, pageReadCalled)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestUnit_GetByRecordId_FailsOnMissingRecordInPage(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	indexPrefix := pageRecordId[0:1]
	getByPrefixCalled := false
	indexReadCalled := false
	getPageCalled := false
	pageReadCalled := false
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return indexPrefix
	}
	m := testUtils.CreateMockMutex(func() {}, func() {})
	indexes := []IndexI{
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled = true
				return diskModels.IndexRecords{
					pageRecordId: pageFileName,
				}, nil
			},
		},
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return diskModels.PageRecords{
				"wrong_id": {
					"col_one": "sdf",
					"col_two": 123,
				},
			}, nil
		},
	}
	mockIndexMap := &MockIndexMap{
		GetByPrefixFunc: func(prefix string) ([]IndexI, error) {
			getByPrefixCalled = true
			return indexes, nil
		},
	}
	mockPageMap := &MockPageMap{
		GetFunc: func(fileName string) (PageI, error) {
			getPageCalled = true
			return page, nil
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.indexMap = mockIndexMap
	b.pageMap = mockPageMap

	result, err := b.GetByRecordId(pageRecordId)

	assert.True(t, getByPrefixCalled)
	assert.True(t, indexReadCalled)
	assert.True(t, getPageCalled)
	assert.True(t, pageReadCalled)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestUnit_GetByRecordId_FailsOnFormatError(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"invalid_column": "some data here",
			"col_two":        1,
		},
	}
	indexPrefix := pageRecordId[0:1]
	getByPrefixCalled := false
	indexReadCalled := false
	getPageCalled := false
	pageReadCalled := false
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return indexPrefix
	}
	m := testUtils.CreateMockMutex(func() {}, func() {})
	indexes := []IndexI{
		&MockIndex{
			ReadFunc: func() (diskModels.IndexRecords, error) {
				indexReadCalled = true
				return diskModels.IndexRecords{
					pageRecordId: pageFileName,
				}, nil
			},
		},
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
	}
	mockIndexMap := &MockIndexMap{
		GetByPrefixFunc: func(prefix string) ([]IndexI, error) {
			getByPrefixCalled = true
			return indexes, nil
		},
	}
	mockPageMap := &MockPageMap{
		GetFunc: func(fileName string) (PageI, error) {
			getPageCalled = true
			return page, nil
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.indexMap = mockIndexMap
	b.pageMap = mockPageMap

	result, err := b.GetByRecordId(pageRecordId)

	assert.True(t, getByPrefixCalled)
	assert.True(t, indexReadCalled)
	assert.True(t, getPageCalled)
	assert.True(t, pageReadCalled)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestUnit_GetFullScan_GetsRecords(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
	}
	lockedCalled := false
	unlockedCalled := false
	getAllPagesCalled := false
	pageReadCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
		GetFileNameFunc: func() string {
			return pageFileName
		},
	}
	mockPageMap := &MockPageMap{
		GetAllFunc: func() []PageI {
			getAllPagesCalled = true
			return []PageI{page}
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.pageMap = mockPageMap

	pageRecordsMap, err := b.GetFullScan([]FilterItem{
		{
			Key:   "col_two",
			Op:    "=",
			Value: 1,
		},
	})

	assert.True(t, getAllPagesCalled)
	assert.True(t, pageReadCalled)
	assert.False(t, lockedCalled)
	assert.False(t, unlockedCalled)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(pageRecordsMap))
	assert.Equal(t, 1, len(pageRecordsMap[pageFileName]))
	assert.Equal(t, pageRecord, pageRecordsMap[pageFileName])
}

func TestUnit_GetFullScan_SkipsEmptyPage(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
	}
	getAllPagesCalled := false
	pageReadCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
		GetFileNameFunc: func() string {
			return pageFileName
		},
	}
	mockPageMap := &MockPageMap{
		GetAllFunc: func() []PageI {
			getAllPagesCalled = true
			return []PageI{page}
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.pageMap = mockPageMap

	pageRecordsMap, err := b.GetFullScan([]FilterItem{
		{
			Key:   "col_two",
			Op:    "=",
			Value: 3,
		},
	})

	assert.True(t, getAllPagesCalled)
	assert.True(t, pageReadCalled)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(pageRecordsMap))
}

func TestUnit_GetFullScan_FailsOnFilterError(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageRecordId := "12345"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		pageRecordId: diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
	}
	getAllPagesCalled := false
	pageReadCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
		GetFileNameFunc: func() string {
			return pageFileName
		},
	}
	mockPageMap := &MockPageMap{
		GetAllFunc: func() []PageI {
			getAllPagesCalled = true
			return []PageI{page}
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.pageMap = mockPageMap

	pageRecordsMap, err := b.GetFullScan([]FilterItem{
		{
			Key:   "col_two",
			Op:    "=",
			Value: "WRONG_DATA_TYPE",
		},
	})

	assert.False(t, getAllPagesCalled)
	assert.False(t, pageReadCalled)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(pageRecordsMap))
}

func TestUnit_GetByPartition_GetsRecords(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		"12345": diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
		"11223": diskModels.PageRecord{
			"col_one": "some other data",
			"col_two": 1,
		},
	}
	expectedHashKeyFile := "hashKeyFile.json"
	expectedHashKeyItem := "hashKeyItem"
	lockedCalled := false
	unlockedCalled := false
	pageReadCalled := false
	getAllHashKeysCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
		GetFileNameFunc: func() string {
			return pageFileName
		},
	}
	mockPartitionMap := &MockPartitionMap{
		GetAllHashKeysFunc: func() []string {
			getAllHashKeysCalled = true
			return []string{
				expectedHashKeyFile,
			}
		},
		GetByHashFunc: func(hashKeyFile string) ([]PageI, error) {
			assert.Equal(t, expectedHashKeyFile, hashKeyFile)
			return []PageI{page}, nil
		},
	}
	diskManagers.MockPartitionManagerInstance.GetHashKeyItemFunc = func(partitionKey string, pageRecord diskModels.PageRecord) (string, error) {
		return expectedHashKeyItem, nil
	}
	diskManagers.MockPartitionManagerInstance.CompareHashKeyItemFunc = func(compare string, hashKeyFile string) bool {
		assert.Equal(t, expectedHashKeyFile, hashKeyFile)
		assert.Equal(t, expectedHashKeyItem, compare)
		return true
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{Keys: []string{"col_two"}}
	b.partitionMap = mockPartitionMap

	pageRecordsMap, err := b.GetByPartition(SearchPartition{"col_two": 1}, []FilterItem{
		{
			Key:   "col_one",
			Op:    "CONTAINS",
			Value: "data",
		},
	})

	assert.True(t, pageReadCalled)
	assert.True(t, getAllHashKeysCalled)
	assert.False(t, lockedCalled)
	assert.False(t, unlockedCalled)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(pageRecordsMap))
	assert.Equal(t, 2, len(pageRecordsMap[pageFileName]))
}

func TestUnit_GetByPartition_SkipsEmptyPages(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		"12345": diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
		"11223": diskModels.PageRecord{
			"col_one": "some other data",
			"col_two": 1,
		},
	}
	expectedHashKeyFile := "hashKeyFile.json"
	expectedHashKeyItem := "hashKeyItem"
	pageReadCalled := false
	getAllHashKeysCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
		GetFileNameFunc: func() string {
			return pageFileName
		},
	}
	mockPartitionMap := &MockPartitionMap{
		GetAllHashKeysFunc: func() []string {
			getAllHashKeysCalled = true
			return []string{
				expectedHashKeyFile,
			}
		},
		GetByHashFunc: func(hashKeyFile string) ([]PageI, error) {
			return []PageI{page}, nil
		},
	}
	diskManagers.MockPartitionManagerInstance.GetHashKeyItemFunc = func(partitionKey string, pageRecord diskModels.PageRecord) (string, error) {
		return expectedHashKeyItem, nil
	}
	diskManagers.MockPartitionManagerInstance.CompareHashKeyItemFunc = func(compare string, hashKeyFile string) bool {
		return true
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{Keys: []string{"col_two"}}
	b.partitionMap = mockPartitionMap

	pageRecordsMap, err := b.GetByPartition(SearchPartition{"col_two": 1}, []FilterItem{
		{
			Key:   "col_one",
			Op:    "CONTAINS",
			Value: "some other random item",
		},
	})

	assert.True(t, pageReadCalled)
	assert.True(t, getAllHashKeysCalled)
	assert.Nil(t, err)

	assert.Equal(t, 0, len(pageRecordsMap))
}

func TestUnit_GetByPartition_SkipsOnNoPartitions(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		"12345": diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
		"11223": diskModels.PageRecord{
			"col_one": "some other data",
			"col_two": 1,
		},
	}
	expectedHashKeyFile := "hashKeyFile.json"
	expectedHashKeyItem := "hashKeyItem"
	pageReadCalled := false
	getAllHashKeysCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
		GetFileNameFunc: func() string {
			return pageFileName
		},
	}
	mockPartitionMap := &MockPartitionMap{
		GetAllHashKeysFunc: func() []string {
			getAllHashKeysCalled = true
			return []string{
				expectedHashKeyFile,
			}
		},
		GetByHashFunc: func(hashKeyFile string) ([]PageI, error) {
			return []PageI{page}, nil
		},
	}
	diskManagers.MockPartitionManagerInstance.GetHashKeyItemFunc = func(partitionKey string, pageRecord diskModels.PageRecord) (string, error) {
		return expectedHashKeyItem, nil
	}
	diskManagers.MockPartitionManagerInstance.CompareHashKeyItemFunc = func(compare string, hashKeyFile string) bool {
		return true
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{}
	b.partitionMap = mockPartitionMap

	pageRecordsMap, err := b.GetByPartition(SearchPartition{"col_two": 1}, []FilterItem{})

	assert.False(t, pageReadCalled)
	assert.False(t, getAllHashKeysCalled)
	assert.Nil(t, err)

	assert.Equal(t, 0, len(pageRecordsMap))
}

func TestUnit_GetByPartition_FailsOnConvertFilterItem(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		"12345": diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
		"11223": diskModels.PageRecord{
			"col_one": "some other data",
			"col_two": 1,
		},
	}
	expectedHashKeyFile := "hashKeyFile.json"
	expectedHashKeyItem := "hashKeyItem"
	pageReadCalled := false
	getAllHashKeysCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
		GetFileNameFunc: func() string {
			return pageFileName
		},
	}
	mockPartitionMap := &MockPartitionMap{
		GetAllHashKeysFunc: func() []string {
			getAllHashKeysCalled = true
			return []string{
				expectedHashKeyFile,
			}
		},
		GetByHashFunc: func(hashKeyFile string) ([]PageI, error) {
			return []PageI{page}, nil
		},
	}
	diskManagers.MockPartitionManagerInstance.GetHashKeyItemFunc = func(partitionKey string, pageRecord diskModels.PageRecord) (string, error) {
		return expectedHashKeyItem, nil
	}
	diskManagers.MockPartitionManagerInstance.CompareHashKeyItemFunc = func(compare string, hashKeyFile string) bool {
		return true
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{Keys: []string{"col_two"}}
	b.partitionMap = mockPartitionMap

	pageRecordsMap, err := b.GetByPartition(SearchPartition{"col_two": 1}, []FilterItem{
		{
			Key:   "col_two",
			Op:    "=",
			Value: "WRONG_DATA_TYPE",
		},
	})

	assert.False(t, pageReadCalled)
	assert.False(t, getAllHashKeysCalled)
	assert.NotNil(t, err)

	assert.Equal(t, 0, len(pageRecordsMap))
}

func TestUnit_GetByPartition_FailsOnHashKeyFilter(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	pageFileName := "page.json"
	pageRecord := diskModels.PageRecords{
		"12345": diskModels.PageRecord{
			"col_one": "some data here",
			"col_two": 1,
		},
		"11223": diskModels.PageRecord{
			"col_one": "some other data",
			"col_two": 1,
		},
	}
	expectedHashKeyFile := "hashKeyFile.json"
	pageReadCalled := false
	getAllHashKeysCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			pageReadCalled = true
			return pageRecord, nil
		},
		GetFileNameFunc: func() string {
			return pageFileName
		},
	}
	mockPartitionMap := &MockPartitionMap{
		GetAllHashKeysFunc: func() []string {
			getAllHashKeysCalled = true
			return []string{
				expectedHashKeyFile,
			}
		},
		GetByHashFunc: func(hashKeyFile string) ([]PageI, error) {
			return []PageI{page}, nil
		},
	}
	diskManagers.MockPartitionManagerInstance.GetHashKeyItemFunc = func(partitionKey string, pageRecord diskModels.PageRecord) (string, error) {
		return "", assert.AnError
	}
	diskManagers.MockPartitionManagerInstance.CompareHashKeyItemFunc = func(compare string, hashKeyFile string) bool {
		return true
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{Keys: []string{"col_two"}}
	b.partitionMap = mockPartitionMap

	pageRecordsMap, err := b.GetByPartition(SearchPartition{"col_two": 1}, []FilterItem{})

	assert.False(t, pageReadCalled)
	assert.True(t, getAllHashKeysCalled)
	assert.NotNil(t, err)

	assert.Equal(t, 0, len(pageRecordsMap))
}

func TestUnit_GetByPartition_FailsOnGetByHashKey(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	expectedHashKeyFile := "hashKeyFile.json"
	expectedHashKeyItem := "hashKeyItem"
	getAllHashKeysCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	mockPartitionMap := &MockPartitionMap{
		GetAllHashKeysFunc: func() []string {
			getAllHashKeysCalled = true
			return []string{
				expectedHashKeyFile,
			}
		},
		GetByHashFunc: func(hashKeyFile string) ([]PageI, error) {
			return nil, assert.AnError
		},
	}
	diskManagers.MockPartitionManagerInstance.GetHashKeyItemFunc = func(partitionKey string, pageRecord diskModels.PageRecord) (string, error) {
		return expectedHashKeyItem, nil
	}
	diskManagers.MockPartitionManagerInstance.CompareHashKeyItemFunc = func(compare string, hashKeyFile string) bool {
		return true
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = diskModels.Partition{Keys: []string{"col_two"}}
	b.partitionMap = mockPartitionMap

	pageRecordsMap, err := b.GetByPartition(SearchPartition{"col_two": 1}, []FilterItem{})

	assert.True(t, getAllHashKeysCalled)
	assert.NotNil(t, err)

	assert.Equal(t, 0, len(pageRecordsMap))
}

func TestUnit_AddWithPartition_AddsWithPartition(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	expectedPartition := diskModels.Partition{Keys: []string{"col_two"}}
	expectedPageRecord := diskModels.PageRecord{
		"col_one": "some string",
		"col_two": 2,
	}
	lockedCalled := false
	unlockedCalled := false
	m := testUtils.CreateMockMutex(func() {
		lockedCalled = true
	}, func() {
		unlockedCalled = true
	})
	diskManagers.MockPartitionManagerInstance.GetHashKeyFunc = func(partition diskModels.Partition, pageRecord diskModels.PageRecord) (string, error) {
		assert.Equal(t, expectedPartition, partition)
		assert.Equal(t, expectedPageRecord, pageRecord)
		return "testHashKey", nil
	}
	diskManagers.MockIndexManagerInstance.GetPageRecordIdPrefixFunc = func(pageRecordId string) string {
		return "prefix"
	}
	page := &MockPage{
		ReadFunc: func() (diskModels.PageRecords, error) {
			return diskModels.PageRecords{}, nil
		},
		GetFileNameFunc: func() string {
			return "pageFile.json"
		},
		WriteFunc: func(data diskModels.PageRecords) error {
			return nil
		},
	}
	index := &MockIndex{
		GetFileNameFunc: func() string {
			return "myIndex.json"
		},
		ReadFunc: func() (diskModels.IndexRecords, error) {
			return diskModels.IndexRecords{}, nil
		},
		WriteFunc: func(data diskModels.IndexRecords) error {
			return nil
		},
	}
	b := createTestBlob(expectedDB, expectedBlob, m)
	b.format = diskModels.Format{
		"col_one": diskModels.FormatItem{KeyType: memoryConstants.String},
		"col_two": diskModels.FormatItem{KeyType: memoryConstants.Int},
	}
	b.partition = expectedPartition
	b.partitionMap = &MockPartitionMap{
		GetByHashFunc: func(hashKeyFile string) ([]PageI, error) {
			return []PageI{page}, nil
		},
		GetCurrentPageFunc: func(hashKeyFile string) (PageI, error) {
			return page, nil
		},
	}
	b.indexMap = &MockIndexMap{
		GetCurrentIndexFunc: func(prefix string) (IndexI, error) {
			return index, nil
		},
		GetFunc: func(prefix string, fileName string) (IndexI, error) {
			return index, nil
		},
	}

	pageRecords, err := b.AddWithPartition([]diskModels.PageRecord{expectedPageRecord})

	assert.True(t, lockedCalled)
	assert.True(t, unlockedCalled)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(pageRecords))
	assert.Equal(t, 1, len(pageRecords[page.GetFileName()]))
	for _, pageRecord := range pageRecords[page.GetFileName()] {
		assert.Equal(t, expectedPageRecord, pageRecord)
	}
}

func TestUnit_AddWithPartition_NoAddOnMissingPartition(t *testing.T) {
	expectedDB := "db"
	expectedBlob := "blob"
	expectedPageRecord := diskModels.PageRecord{
		"col_one": "some string",
		"col_two": 2,
	}
	getHashKeyCalled := false
	m := testUtils.CreateMockMutex(func() {}, func() {})
	diskManagers.MockPartitionManagerInstance.GetHashKeyFunc = func(partition diskModels.Partition, pageRecord diskModels.PageRecord) (string, error) {
		getHashKeyCalled = true
		return "testHashKey", nil
	}
	b := createTestBlob(expectedDB, expectedBlob, m)

	pageRecords, err := b.AddWithPartition([]diskModels.PageRecord{expectedPageRecord})
	assert.Nil(t, err)
	assert.False(t, getHashKeyCalled)
	assert.Equal(t, 0, len(pageRecords))
}
