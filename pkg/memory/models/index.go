package memoryModels

import (
	"errors"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"sync"
)

type IndexMap struct {
	m                *sync.Mutex
	itemMap          IndexPrefixMap
	currentPages     IndexPrefixCurrentPageMap
	db               string
	blob             string
	indexDiskManager diskManagers.IndexDiskManager
	dataLocation     string
	dataCaching      bool
}

type IndexPrefixMap map[string]map[string]*Index
type IndexPrefixCurrentPageMap map[string]*Index

func NewIndexMap(db string, blob string, dataLocation string, dataCaching bool) IndexMap {
	return IndexMap{
		m:                &sync.Mutex{},
		itemMap:          IndexPrefixMap{},
		currentPages:     IndexPrefixCurrentPageMap{},
		db:               db,
		blob:             blob,
		indexDiskManager: diskManagers.CreateIndexDiskManager(dataLocation),
		dataLocation:     dataLocation,
		dataCaching:      dataCaching,
	}
}

func (im *IndexMap) Initialize() error {
	indexes, err := im.indexDiskManager.GetAll(im.db, im.blob)
	if err != nil {
		return err
	}
	for prefix, index := range indexes {
		im.itemMap[prefix] = make(map[string]*Index)
		for _, fileName := range index.FileNames {
			indexObj := NewIndex(im.db, im.blob, fileName, im.dataLocation, im.dataCaching)
			im.itemMap[prefix][fileName] = indexObj
			im.currentPages[prefix] = indexObj
		}
	}
	return nil
}

func (im *IndexMap) Get(prefix string, fileName string) (*Index, error) {
	im.m.Lock()
	defer im.m.Unlock()
	if indexMap, ok := im.itemMap[prefix]; ok {
		if index, indexFound := indexMap[fileName]; indexFound {
			return index, nil
		}
	}
	return nil, fmt.Errorf("index file %s not found", fileName)
}

func (im *IndexMap) GetByPrefix(prefix string) ([]*Index, error) {
	im.m.Lock()
	defer im.m.Unlock()
	indexes := []*Index{}
	if indexMap, ok := im.itemMap[prefix]; ok {
		for _, index := range indexMap {
			indexes = append(indexes, index)
		}
	}
	return indexes, nil
}

func (im *IndexMap) Add(pageRecordId string) (*Index, error) {
	im.m.Lock()
	defer im.m.Unlock()
	fileName, err := im.indexDiskManager.Create(im.db, im.blob, pageRecordId)
	if err != nil {
		if fileName != "" {
			_, _ = im.indexDiskManager.Delete(im.db, im.blob, fileName)
		}
		return nil, err
	}
	prefix := im.indexDiskManager.GetPageRecordIdPrefix(pageRecordId)
	if _, ok := im.itemMap[prefix]; !ok {
		im.itemMap[prefix] = make(map[string]*Index)
	}
	index := NewIndex(im.db, im.blob, fileName, im.dataLocation, im.dataCaching)
	im.itemMap[prefix][fileName] = index
	im.currentPages[prefix] = index
	return index, nil
}

func (im *IndexMap) Delete(prefix string, fileName string) error {
	im.m.Lock()
	defer im.m.Unlock()
	isPhantomFile, err := im.indexDiskManager.Delete(im.db, im.blob, fileName)
	if err != nil && !isPhantomFile {
		return err
	}
	delete(im.itemMap[prefix], fileName)
	if len(im.itemMap[prefix]) == 0 {
		delete(im.itemMap, prefix)
		if im.currentPages[prefix] != nil && fileName == im.currentPages[prefix].fileName {
			delete(im.currentPages, prefix)
		}
	}
	return err
}

func (im *IndexMap) GetCurrentIndex(prefix string) (*Index, error) {
	im.m.Lock()
	defer im.m.Unlock()
	if _, ok := im.currentPages[prefix]; ok {
		if index := im.currentPages[prefix]; index != nil {
			return index, nil
		}
	}
	return nil, errors.New("current index not found")
}

type Index struct {
	m                *sync.Mutex
	fileName         string
	indexDiskManager diskManagers.IndexDiskManager
	db               string
	blob             string
	cache            diskModels.IndexRecords
	dataCaching      bool
}

func NewIndex(db string, blob string, fileName string, dataLocation string, dataCaching bool) *Index {
	return &Index{
		m:                &sync.Mutex{},
		fileName:         fileName,
		indexDiskManager: diskManagers.CreateIndexDiskManager(dataLocation),
		db:               db,
		blob:             blob,
		cache:            nil,
		dataCaching:      dataCaching,
	}
}

func (i *Index) Read() (diskModels.IndexRecords, error) {
	i.m.Lock()
	defer i.m.Unlock()
	return i._read()
}

func (i *Index) Write(data diskModels.IndexRecords) error {
	i.m.Lock()
	defer i.m.Unlock()
	return i._write(data)
}

func (i *Index) Delete(pageRecordIds []string) (int, error) {
	i.m.Lock()
	defer i.m.Unlock()
	indexRecords, err := i._read()
	if err != nil {
		return -1, err
	}
	for _, pageRecordId := range pageRecordIds {
		delete(indexRecords, pageRecordId)
	}
	return len(indexRecords), i._write(indexRecords)
}

func (i *Index) _read() (diskModels.IndexRecords, error) {
	if !i.dataCaching {
		return i.indexDiskManager.GetData(i.db, i.blob, i.fileName)
	}
	if len(i.cache) == 0 {
		data, err := i.indexDiskManager.GetData(i.db, i.blob, i.fileName)
		if err != nil {
			return diskModels.IndexRecords{}, err
		}
		i.cache = data
	}
	indexRecords := diskModels.IndexRecords{}
	for pageRecordId, pageFileName := range i.cache {
		indexRecords[pageRecordId] = pageFileName
	}
	return indexRecords, nil
}

func (i *Index) _write(data diskModels.IndexRecords) error {
	err := i.indexDiskManager.WriteData(i.db, i.blob, i.fileName, data)
	if err != nil {
		return err
	}
	if i.dataCaching {
		i.cache = data
	}
	return nil
}
