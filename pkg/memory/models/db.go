package memoryModels

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/managers"
	"sync"
)

type DBMap struct {
	m             *sync.Mutex
	itemMap       map[string]*BlobMap
	dataLocation  string
	dataCaching   bool
	dbDiskManager diskManagers.DBDiskManager
}

func NewDBMap(dataLocation string, dataCaching bool) DBMap {
	return DBMap{
		m:             &sync.Mutex{},
		itemMap:       make(map[string]*BlobMap),
		dataLocation:  dataLocation,
		dataCaching:   dataCaching,
		dbDiskManager: diskManagers.CreateDBDiskManager(dataLocation),
	}
}

func (dbm *DBMap) Add(db string) (*BlobMap, error) {
	dbm.m.Lock()
	defer dbm.m.Unlock()
	dbFormatter := DBFormatter{Name: db}
	if err := dbFormatter.HasDBNameConvention(); err != nil {
		return nil, err
	}
	if err := dbm.dbDiskManager.Create(db); err != nil {
		return nil, err
	}
	blobMap := NewBlobMap(db, dbm.dataLocation, dbm.dataCaching)
	dbm.itemMap[db] = &blobMap
	return &blobMap, nil
}

func (dbm *DBMap) Delete(db string) error {
	dbm.m.Lock()
	defer dbm.m.Unlock()
	if err := dbm.dbDiskManager.Delete(db); err != nil {
		return err
	}
	delete(dbm.itemMap, db)
	return nil
}

func (dbm *DBMap) GetBlobMap(db string) (*BlobMap, error) {
	dbm.m.Lock()
	defer dbm.m.Unlock()
	if blobMap, ok := dbm.itemMap[db]; ok {
		return blobMap, nil
	}
	if !dbm.dbDiskManager.Exists(db) {
		return nil, fmt.Errorf("db %s does not exist", db)
	}
	blobMap := NewBlobMap(db, dbm.dataLocation, dbm.dataCaching)
	dbm.itemMap[db] = &blobMap
	return &blobMap, nil
}

func (dbm *DBMap) Remove(db string) {
	dbm.m.Lock()
	defer dbm.m.Unlock()
	delete(dbm.itemMap, db)
}
