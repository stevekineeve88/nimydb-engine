package memoryModels

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
)

type MockPageMap struct {
	InitializeFunc     func() error
	GetFunc            func(fileName string) (PageI, error)
	GetAllFunc         func() []PageI
	AddFunc            func() (PageI, error)
	DeleteFunc         func(fileName string) (bool, error)
	GetCurrentPageFunc func() (PageI, error)
}

func (pm *MockPageMap) Initialize() error {
	return pm.InitializeFunc()
}

func (pm *MockPageMap) Get(fileName string) (PageI, error) {
	return pm.GetFunc(fileName)
}

func (pm *MockPageMap) GetAll() []PageI {
	return pm.GetAllFunc()
}

func (pm *MockPageMap) Add() (PageI, error) {
	return pm.AddFunc()
}

func (pm *MockPageMap) Delete(fileName string) (bool, error) {
	return pm.DeleteFunc(fileName)
}

func (pm *MockPageMap) GetCurrentPage() (PageI, error) {
	return pm.GetCurrentPageFunc()
}

type MockPage struct {
	ReadFunc        func() (diskModels.PageRecords, error)
	WriteFunc       func(data diskModels.PageRecords) error
	GetFileNameFunc func() string
}

func (p *MockPage) Read() (diskModels.PageRecords, error) {
	return p.ReadFunc()
}

func (p *MockPage) Write(data diskModels.PageRecords) error {
	return p.WriteFunc(data)
}

func (p *MockPage) GetFileName() string {
	return p.GetFileNameFunc()
}

type MockIndexMap struct {
	InitializeFunc      func() error
	GetFunc             func(prefix string, fileName string) (IndexI, error)
	GetByPrefixFunc     func(prefix string) ([]IndexI, error)
	AddFunc             func(pageRecordId string) (IndexI, error)
	DeleteFunc          func(prefix string, fileName string) error
	GetCurrentIndexFunc func(prefix string) (IndexI, error)
}

func (im *MockIndexMap) Initialize() error {
	return im.InitializeFunc()
}

func (im *MockIndexMap) Get(prefix string, fileName string) (IndexI, error) {
	return im.GetFunc(prefix, fileName)
}

func (im *MockIndexMap) GetByPrefix(prefix string) ([]IndexI, error) {
	return im.GetByPrefixFunc(prefix)
}

func (im *MockIndexMap) Add(pageRecordId string) (IndexI, error) {
	return im.AddFunc(pageRecordId)
}

func (im *MockIndexMap) Delete(prefix string, fileName string) error {
	return im.DeleteFunc(prefix, fileName)
}

func (im *MockIndexMap) GetCurrentIndex(prefix string) (IndexI, error) {
	return im.GetCurrentIndexFunc(prefix)
}

type MockIndex struct {
	ReadFunc        func() (diskModels.IndexRecords, error)
	WriteFunc       func(data diskModels.IndexRecords) error
	DeleteFunc      func(pageRecordIds []string) (int, error)
	GetFileNameFunc func() string
}

func (i *MockIndex) Read() (diskModels.IndexRecords, error) {
	return i.ReadFunc()
}

func (i *MockIndex) Write(data diskModels.IndexRecords) error {
	return i.WriteFunc(data)
}

func (i *MockIndex) Delete(pageRecordIds []string) (int, error) {
	return i.DeleteFunc(pageRecordIds)
}

func (i *MockIndex) GetFileName() string {
	return i.GetFileNameFunc()
}

type MockPartitionMap struct {
	InitializeFunc     func() error
	GetByHashFunc      func(hashKeyFile string) ([]PageI, error)
	GetAllHashKeysFunc func() []string
	AddFunc            func(hashKeyFile string, pageFileName string) error
	DeleteFunc         func(hashKeyFile string, pageFileName string) error
	GetCurrentPageFunc func(hashKeyFile string) (PageI, error)
}

func (pm *MockPartitionMap) Initialize() error {
	return pm.InitializeFunc()
}

func (pm *MockPartitionMap) GetByHash(hashKeyFile string) ([]PageI, error) {
	return pm.GetByHashFunc(hashKeyFile)
}

func (pm *MockPartitionMap) GetAllHashKeys() []string {
	return pm.GetAllHashKeysFunc()
}

func (pm *MockPartitionMap) Add(hashKeyFile string, pageFileName string) error {
	return pm.AddFunc(hashKeyFile, pageFileName)
}

func (pm *MockPartitionMap) Delete(hashKeyFile string, pageFileName string) error {
	return pm.DeleteFunc(hashKeyFile, pageFileName)
}

func (pm *MockPartitionMap) GetCurrentPage(hashKeyFile string) (PageI, error) {
	return pm.GetCurrentPageFunc(hashKeyFile)
}
