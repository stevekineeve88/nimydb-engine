package memoryModels

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/test/utils"
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

func createTestBlob(
	db string,
	blob string,
	dataLocation string,
	dataCaching bool,
	m sync.Locker,
	partition diskModels.Partition,
	format diskModels.Format,
) Blob {
	pageMap := NewPageMap(db, blob, dataLocation, dataCaching)
	return Blob{
		m:                    m,
		blob:                 blob,
		db:                   db,
		pageMap:              pageMap,
		indexMap:             NewIndexMap(db, blob, dataLocation, dataCaching),
		partitionMap:         NewPartitionMap(db, blob, dataLocation, pageMap),
		partition:            partition,
		format:               format,
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
