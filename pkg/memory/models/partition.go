package memoryModels

import (
	"errors"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/managers"
	"sync"
)

type PartitionMap struct {
	m                    *sync.Mutex
	itemMap              PartitionHashMap
	pageMap              *PageMap
	currentPages         PartitionHashCurrentPageMap
	db                   string
	blob                 string
	partitionDiskManager diskManagers.PartitionManager
	dataLocation         string
}

type PartitionHashMap map[string][]string
type PartitionHashCurrentPageMap map[string]string

func NewPartitionMap(db string, blob string, dataLocation string, pageMap *PageMap) PartitionMap {
	return PartitionMap{
		m:                    &sync.Mutex{},
		itemMap:              PartitionHashMap{},
		pageMap:              pageMap,
		currentPages:         PartitionHashCurrentPageMap{},
		db:                   db,
		blob:                 blob,
		partitionDiskManager: diskManagers.CreatePartitionManager(dataLocation),
		dataLocation:         dataLocation,
	}
}

func (pm *PartitionMap) Initialize() error {
	partitionHashes, err := pm.partitionDiskManager.GetAll(pm.db, pm.blob)
	if err != nil {
		return err
	}
	for _, partitionHash := range partitionHashes {
		partitionPages, err := pm.partitionDiskManager.GetByHashKey(pm.db, pm.blob, partitionHash)
		if err != nil {
			return err
		}
		for _, partitionPage := range partitionPages {
			_, err := pm.pageMap.Get(partitionPage.FileName)
			if err != nil {
				continue
			}
			pm.itemMap[partitionHash] = append(pm.itemMap[partitionHash], partitionPage.FileName)
			pm.currentPages[partitionHash] = partitionPage.FileName
		}
	}
	return nil
}

func (pm *PartitionMap) GetByHash(hashKeyFile string) ([]*Page, error) {
	pm.m.Lock()
	defer pm.m.Unlock()
	pages := []*Page{}
	if pageFiles, ok := pm.itemMap[hashKeyFile]; ok {
		for _, pageFile := range pageFiles {
			if page, err := pm.pageMap.Get(pageFile); err != nil {
				continue
			} else {
				pages = append(pages, page)
			}
		}
	}
	return pages, nil
}

func (pm *PartitionMap) GetAllHashKeys() []string {
	pm.m.Lock()
	pm.m.Unlock()
	hashKeys := []string{}
	for hashKey, _ := range pm.itemMap {
		hashKeys = append(hashKeys, hashKey)
	}
	return hashKeys
}

func (pm *PartitionMap) Add(hashKeyFile string, pageFileName string) error {
	pm.m.Lock()
	defer pm.m.Unlock()
	_, err := pm.pageMap.Get(pageFileName)
	if err != nil {
		return err
	}
	err = pm.partitionDiskManager.AddPage(pm.db, pm.blob, hashKeyFile, pageFileName)
	if err != nil {
		return err
	}
	pm.itemMap[hashKeyFile] = append(pm.itemMap[hashKeyFile], pageFileName)
	pm.currentPages[hashKeyFile] = pageFileName
	return nil
}

func (pm *PartitionMap) Delete(hashKeyFile string, pageFileName string) error {
	pm.m.Lock()
	defer pm.m.Unlock()
	if err := pm.partitionDiskManager.Remove(pm.db, pm.blob, hashKeyFile, pageFileName); err != nil {
		return err
	}
	if _, ok := pm.itemMap[hashKeyFile]; ok {
		for i := 0; i < len(pm.itemMap[hashKeyFile]); i++ {
			if pm.itemMap[hashKeyFile][i] == pageFileName {
				filesNames := pm.itemMap[hashKeyFile]
				copy(filesNames[i:], filesNames[i+1:])
				filesNames[len(filesNames)-1] = ""
				filesNames = filesNames[:len(filesNames)-1]
				pm.itemMap[hashKeyFile] = filesNames
				if pm.currentPages[hashKeyFile] == pageFileName {
					delete(pm.currentPages, hashKeyFile)
				}
				if len(pm.itemMap[hashKeyFile]) == 0 {
					delete(pm.itemMap, hashKeyFile)
				}
				return nil
			}
		}
	}
	return nil
}

func (pm *PartitionMap) GetCurrentPage(hashKeyFile string) (*Page, error) {
	pm.m.Lock()
	defer pm.m.Unlock()
	if pageFile, ok := pm.currentPages[hashKeyFile]; ok {
		if page, err := pm.pageMap.Get(pageFile); err == nil {
			return page, nil
		}
	}
	return nil, errors.New("current partition page not found")
}
