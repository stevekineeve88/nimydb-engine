package memoryModels

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"sync"
)

type PageMapI interface {
	Initialize() error
	Get(fileName string) (PageI, error)
	GetAll() []PageI
	Add() (PageI, error)
	Delete(fileName string) (bool, error)
	GetCurrentPage() (PageI, error)
}

type PageMap struct {
	m               *sync.Mutex
	itemMap         map[string]PageI
	currentPage     PageI
	db              string
	blob            string
	pageDiskManager diskManagers.PageManager
	dataLocation    string
	dataCaching     bool
}

func NewPageMap(db string, blob string, dataLocation string, dataCaching bool) PageMapI {
	return &PageMap{
		m:               &sync.Mutex{},
		itemMap:         make(map[string]PageI),
		currentPage:     nil,
		db:              db,
		blob:            blob,
		pageDiskManager: diskManagers.CreatePageManager(dataLocation),
		dataLocation:    dataLocation,
		dataCaching:     dataCaching,
	}
}

func (pm *PageMap) Initialize() error {
	pages, err := pm.pageDiskManager.GetAll(pm.db, pm.blob)
	if err != nil {
		return err
	}
	for _, page := range pages {
		pageObj := NewPage(pm.db, pm.blob, page.FileName, pm.dataLocation, pm.dataCaching)
		pm.itemMap[page.FileName] = pageObj
		pm.currentPage = pageObj
	}
	return nil
}

func (pm *PageMap) Get(fileName string) (PageI, error) {
	pm.m.Lock()
	defer pm.m.Unlock()
	if page, ok := pm.itemMap[fileName]; ok && page != nil {
		return page, nil
	}
	return nil, fmt.Errorf("%s not found in page map", fileName)
}

func (pm *PageMap) GetAll() []PageI {
	pm.m.Lock()
	defer pm.m.Unlock()
	pages := []PageI{}
	for _, page := range pm.itemMap {
		if page != nil {
			pages = append(pages, page)
		}
	}
	return pages
}

func (pm *PageMap) Add() (PageI, error) {
	pm.m.Lock()
	defer pm.m.Unlock()
	fileName, err := pm.pageDiskManager.Create(pm.db, pm.blob)
	if err != nil {
		if fileName != "" {
			_, _ = pm.pageDiskManager.Delete(pm.db, pm.blob, fileName)
		}
		return nil, err
	}
	page := NewPage(pm.db, pm.blob, fileName, pm.dataLocation, pm.dataCaching)
	pm.itemMap[fileName] = page
	pm.currentPage = page
	return page, nil
}

func (pm *PageMap) Delete(fileName string) (bool, error) {
	pm.m.Lock()
	defer pm.m.Unlock()
	isPhantomFile, err := pm.pageDiskManager.Delete(pm.db, pm.blob, fileName)
	if err != nil && !isPhantomFile {
		return isPhantomFile, err
	}
	delete(pm.itemMap, fileName)
	if pm.currentPage != nil && fileName == pm.currentPage.GetFileName() {
		pm.currentPage = nil
	}
	return isPhantomFile, err
}

func (pm *PageMap) GetCurrentPage() (PageI, error) {
	pm.m.Lock()
	defer pm.m.Unlock()
	if pm.currentPage == nil {
		return nil, fmt.Errorf("current page not set")
	}
	return pm.currentPage, nil
}

type PageI interface {
	Read() (diskModels.PageRecords, error)
	Write(data diskModels.PageRecords) error
	GetFileName() string
}

type Page struct {
	m               *sync.Mutex
	fileName        string
	pageDiskManager diskManagers.PageManager
	db              string
	blob            string
	dataCaching     bool
	cache           diskModels.PageRecords
}

func NewPage(db string, blob string, fileName string, dataLocation string, dataCaching bool) PageI {
	return &Page{
		m:               &sync.Mutex{},
		fileName:        fileName,
		pageDiskManager: diskManagers.CreatePageManager(dataLocation),
		db:              db,
		blob:            blob,
		dataCaching:     dataCaching,
		cache:           nil,
	}
}

func (p *Page) Read() (diskModels.PageRecords, error) {
	p.m.Lock()
	defer p.m.Unlock()
	if !p.dataCaching {
		return p.pageDiskManager.GetData(p.db, p.blob, p.fileName)
	}
	if len(p.cache) == 0 {
		data, err := p.pageDiskManager.GetData(p.db, p.blob, p.fileName)
		if err != nil {
			return diskModels.PageRecords{}, err
		}
		p.cache = data
	}
	pageRecords := diskModels.PageRecords{}
	for pageRecordId, pageRecord := range p.cache {
		pageRecords[pageRecordId] = pageRecord
	}
	return pageRecords, nil
}

func (p *Page) Write(data diskModels.PageRecords) error {
	p.m.Lock()
	defer p.m.Unlock()
	err := p.pageDiskManager.WriteData(p.db, p.blob, p.fileName, data)
	if err != nil {
		return err
	}
	if p.dataCaching {
		p.cache = data
	}
	return nil
}

func (p *Page) GetFileName() string {
	return p.fileName
}
