package diskManagers

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
)

type MockBlobManager struct {
	CreateFunc  func(db string, blob string) error
	DeleteFunc  func(db string, blob string) error
	GetByDBFunc func(db string) ([]string, error)
}

var MockBlobManagerInstance *MockBlobManager

func CreateMockBlobManager() {
	MockBlobManagerInstance = &MockBlobManager{}
	blobManagerInstance = MockBlobManagerInstance
}

func (bm *MockBlobManager) Create(db string, blob string) error {
	return bm.CreateFunc(db, blob)
}

func (bm *MockBlobManager) Delete(db string, blob string) error {
	return bm.DeleteFunc(db, blob)
}

func (bm *MockBlobManager) GetByDB(db string) ([]string, error) {
	return bm.GetByDBFunc(db)
}

type MockIndexManager struct {
	InitializeFunc            func(db string, blob string) error
	CreateFunc                func(db string, blob string, pageRecordId string) (string, error)
	GetAllFunc                func(db string, blob string) (diskModels.Indexes, error)
	GetDataFunc               func(db string, blob string, indexFileName string) (diskModels.IndexRecords, error)
	WriteDataFunc             func(db string, blob string, indexFileName string, data diskModels.IndexRecords) error
	DeleteFunc                func(db string, blob string, indexFileName string) (bool, error)
	GetPageRecordIdPrefixFunc func(pageRecordId string) string
}

var MockIndexManagerInstance *MockIndexManager

func CreateMockIndexManager() {
	MockIndexManagerInstance = &MockIndexManager{}
	indexManagerInstance = MockIndexManagerInstance
}

func (im *MockIndexManager) Initialize(db string, blob string) error {
	return im.InitializeFunc(db, blob)
}

func (im *MockIndexManager) Create(db string, blob string, pageRecordId string) (string, error) {
	return im.CreateFunc(db, blob, pageRecordId)
}

func (im *MockIndexManager) GetAll(db string, blob string) (diskModels.Indexes, error) {
	return im.GetAllFunc(db, blob)
}

func (im *MockIndexManager) GetData(db string, blob string, indexFileName string) (diskModels.IndexRecords, error) {
	return im.GetDataFunc(db, blob, indexFileName)
}

func (im *MockIndexManager) WriteData(db string, blob string, indexFileName string, data diskModels.IndexRecords) error {
	return im.WriteDataFunc(db, blob, indexFileName, data)
}

func (im *MockIndexManager) Delete(db string, blob string, indexFileName string) (bool, error) {
	return im.DeleteFunc(db, blob, indexFileName)
}

func (im *MockIndexManager) GetPageRecordIdPrefix(pageRecordId string) string {
	return im.GetPageRecordIdPrefixFunc(pageRecordId)
}

type MockPartitionManager struct {
	InitializeFunc     func(db string, blob string, partition diskModels.Partition) error
	AddPageFunc        func(db string, blob string, hashKeyFileName string, pageFileName string) error
	GetPartitionFunc   func(db string, blob string) (diskModels.Partition, error)
	GetByHashKeyFunc   func(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error)
	GetHashKeyItemFunc func(partitionKey string, pageRecord diskModels.PageRecord) (string, error)
	GetAllFunc         func(db string, blob string) ([]string, error)
	RemoveFunc         func(db string, blob string, hashKeyFileName string, pageFileName string) error
	DeleteFunc         func(db string, blob string, hashKeyFileName string) error
	GetHashKeyFunc     func(partition diskModels.Partition, pageRecord diskModels.PageRecord) (string, error)
	CreateHashKeyFunc  func(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error)
}

var MockPartitionManagerInstance *MockPartitionManager

func CreateMockPartitionManager() {
	MockPartitionManagerInstance = &MockPartitionManager{}
	partitionManagerInstance = MockPartitionManagerInstance
}

func (pm *MockPartitionManager) Initialize(db string, blob string, partition diskModels.Partition) error {
	return pm.InitializeFunc(db, blob, partition)
}

func (pm *MockPartitionManager) AddPage(db string, blob string, hashKeyFileName string, pageFileName string) error {
	return pm.AddPageFunc(db, blob, hashKeyFileName, pageFileName)
}

func (pm *MockPartitionManager) GetPartition(db string, blob string) (diskModels.Partition, error) {
	return pm.GetPartitionFunc(db, blob)
}

func (pm *MockPartitionManager) GetByHashKey(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error) {
	return pm.GetByHashKeyFunc(db, blob, hashKeyFileName)
}

func (pm *MockPartitionManager) GetHashKeyItem(partitionKey string, pageRecord diskModels.PageRecord) (string, error) {
	return pm.GetHashKeyItemFunc(partitionKey, pageRecord)
}

func (pm *MockPartitionManager) GetAll(db string, blob string) ([]string, error) {
	return pm.GetAllFunc(db, blob)
}

func (pm *MockPartitionManager) Remove(db string, blob string, hashKeyFileName string, pageFileName string) error {
	return pm.RemoveFunc(db, blob, hashKeyFileName, pageFileName)
}

func (pm *MockPartitionManager) Delete(db string, blob string, hashKeyFileName string) error {
	return pm.DeleteFunc(db, blob, hashKeyFileName)
}

func (pm *MockPartitionManager) GetHashKey(partition diskModels.Partition, pageRecord diskModels.PageRecord) (string, error) {
	return pm.GetHashKeyFunc(partition, pageRecord)
}

func (pm *MockPartitionManager) CreateHashKey(db string, blob string, hashKeyFileName string) (diskModels.PartitionPages, error) {
	return pm.CreateHashKeyFunc(db, blob, hashKeyFileName)
}

type MockFormatManager struct {
	CreateFunc func(db string, blob string, format diskModels.Format) error
	GetFunc    func(db string, blob string) (diskModels.Format, error)
}

var MockFormatManagerInstance *MockFormatManager

func CreateMockFormatManager() {
	MockFormatManagerInstance = &MockFormatManager{}
	formatManagerInstance = MockFormatManagerInstance
}

func (fm *MockFormatManager) Create(db string, blob string, format diskModels.Format) error {
	return fm.CreateFunc(db, blob, format)
}
func (fm *MockFormatManager) Get(db string, blob string) (diskModels.Format, error) {
	return fm.GetFunc(db, blob)
}

type MockPageManager struct {
	InitializeFunc func(db string, blob string) error
	CreateFunc     func(db string, blob string) (string, error)
	GetAllFunc     func(db string, blob string) (diskModels.Pages, error)
	GetDataFunc    func(db string, blob string, pageFileName string) (diskModels.PageRecords, error)
	WriteDataFunc  func(db string, blob string, pageFileName string, data diskModels.PageRecords) error
	DeleteFunc     func(db string, blob string, pageFileName string) (bool, error)
}

var MockPageManagerInstance *MockPageManager

func CreateMockPageManager() {
	MockPageManagerInstance = &MockPageManager{}
	pageManagerInstance = MockPageManagerInstance
}

func (pm *MockPageManager) Initialize(db string, blob string) error {
	return pm.InitializeFunc(db, blob)
}

func (pm *MockPageManager) Create(db string, blob string) (string, error) {
	return pm.CreateFunc(db, blob)
}

func (pm *MockPageManager) GetAll(db string, blob string) (diskModels.Pages, error) {
	return pm.GetAllFunc(db, blob)
}

func (pm *MockPageManager) GetData(db string, blob string, pageFileName string) (diskModels.PageRecords, error) {
	return pm.GetDataFunc(db, blob, pageFileName)
}

func (pm *MockPageManager) WriteData(db string, blob string, pageFileName string, data diskModels.PageRecords) error {
	return pm.WriteDataFunc(db, blob, pageFileName, data)
}

func (pm *MockPageManager) Delete(db string, blob string, pageFileName string) (bool, error) {
	return pm.DeleteFunc(db, blob, pageFileName)
}
