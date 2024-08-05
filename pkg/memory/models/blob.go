package memoryModels

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	"sync"
)

type BlobMap struct {
	m            *sync.Mutex
	itemMap      map[string]*Blob
	db           string
	dataLocation string
	dataCaching  bool
}

func NewBlobMap(db string, dataLocation string, dataCaching bool) BlobMap {
	return BlobMap{
		m:            &sync.Mutex{},
		itemMap:      make(map[string]*Blob),
		db:           db,
		dataLocation: dataLocation,
		dataCaching:  dataCaching,
	}
}

func (bm *BlobMap) Add(blob string, format diskModels.Format, partition *diskModels.Partition) (*Blob, error) {
	bm.m.Lock()
	defer bm.m.Unlock()
	blobObj, err := InitializeBlob(bm.db, blob, bm.dataLocation, format, partition, bm.dataCaching)
	if err != nil {
		return nil, err
	}
	bm.itemMap[blob] = &blobObj
	return &blobObj, nil
}

func (bm *BlobMap) Get(blob string) (*Blob, error) {
	bm.m.Lock()
	defer bm.m.Unlock()
	if blobObj, ok := bm.itemMap[blob]; ok {
		return blobObj, nil
	}
	blobObj, err := CreateBlob(bm.db, blob, bm.dataLocation, bm.dataCaching)
	if err != nil {
		return nil, err
	}
	bm.itemMap[blob] = &blobObj
	return &blobObj, nil
}

func (bm *BlobMap) Delete(blob string) error {
	bm.m.Lock()
	defer bm.m.Unlock()
	if blobObj, ok := bm.itemMap[blob]; ok {
		err := blobObj.DeleteBlob()
		if err != nil {
			return err
		}
		delete(bm.itemMap, blob)
	}
	return nil
}

func (bm *BlobMap) Remove(blob string) {
	bm.m.Lock()
	defer bm.m.Unlock()
	delete(bm.itemMap, blob)
}

type PageRecordsMap map[string]diskModels.PageRecords

type Blob struct {
	m                    *sync.Mutex
	blob                 string
	db                   string
	pageMap              *PageMap
	indexMap             *IndexMap
	partitionMap         *PartitionMap
	partition            diskModels.Partition
	format               diskModels.Format
	indexDiskManager     diskManagers.IndexDiskManager
	partitionDiskManager diskManagers.PartitionDiskManager
	blobDiskManager      diskManagers.BlobDiskManager
}

func CreateBlob(db string, blob string, dataLocation string, dataCaching bool) (Blob, error) {
	indexDiskManager := diskManagers.CreateIndexDiskManager(dataLocation)
	partitionDiskManager := diskManagers.CreatePartitionDiskManager(dataLocation)
	blobDiskManager := diskManagers.CreateBlobDiskManager(dataLocation)
	formatDiskManager := diskManagers.CreateFormatDiskManager(dataLocation)

	pageMap := NewPageMap(db, blob, dataLocation, dataCaching)
	indexMap := NewIndexMap(db, blob, dataLocation, dataCaching)
	partitionMap := NewPartitionMap(db, blob, dataLocation, &pageMap)

	blobStruct := Blob{
		m:                    &sync.Mutex{},
		blob:                 blob,
		db:                   db,
		pageMap:              &pageMap,
		indexMap:             &indexMap,
		partitionMap:         &partitionMap,
		partition:            diskModels.Partition{},
		indexDiskManager:     indexDiskManager,
		partitionDiskManager: partitionDiskManager,
		blobDiskManager:      blobDiskManager,
	}

	format, err := formatDiskManager.Get(db, blob)
	if err != nil {
		return blobStruct, err
	}
	blobStruct.format = format

	if err := blobStruct.pageMap.Initialize(); err != nil {
		return blobStruct, err
	}
	if err := blobStruct.indexMap.Initialize(); err != nil {
		return blobStruct, err
	}

	if partition, err := partitionDiskManager.GetPartition(db, blob); err == nil {
		if err := blobStruct.partitionMap.Initialize(); err != nil {
			return blobStruct, err
		}
		blobStruct.partition = partition
	}

	return blobStruct, nil
}

func InitializeBlob(db string, blob string, dataLocation string, format diskModels.Format, partition *diskModels.Partition, dataCaching bool) (Blob, error) {
	indexDiskManager := diskManagers.CreateIndexDiskManager(dataLocation)
	pageDiskManager := diskManagers.CreatePageDiskManager(dataLocation)
	partitionDiskManager := diskManagers.CreatePartitionDiskManager(dataLocation)
	blobDiskManager := diskManagers.CreateBlobDiskManager(dataLocation)
	formatDiskManager := diskManagers.CreateFormatDiskManager(dataLocation)

	var formatter BlobFormatter
	if partition != nil {
		formatter = CreateFormatterWithPartition(blob, format, *partition)
		if err := formatter.HasPartitionStructure(); err != nil {
			return Blob{}, err
		}
	} else {
		formatter = CreateFormatter(blob, format)
	}

	if err := formatter.HasBlobNameConvention(); err != nil {
		return Blob{}, err
	}
	if err := formatter.HasFormatStructure(); err != nil {
		return Blob{}, err
	}

	if err := blobDiskManager.Create(db, blob); err != nil {
		return Blob{}, err
	}
	if err := formatDiskManager.Create(db, blob, format); err != nil {
		_ = blobDiskManager.Delete(db, blob)
		return Blob{}, err
	}
	if err := pageDiskManager.Initialize(db, blob); err != nil {
		_ = blobDiskManager.Delete(db, blob)
		return Blob{}, err
	}
	if err := indexDiskManager.Initialize(db, blob); err != nil {
		_ = blobDiskManager.Delete(db, blob)
		return Blob{}, err
	}
	if partition != nil {
		if err := partitionDiskManager.Initialize(db, blob, *partition); err != nil {
			_ = blobDiskManager.Delete(db, blob)
			return Blob{}, err
		}
	}

	pageMap := NewPageMap(db, blob, dataLocation, dataCaching)
	indexMap := NewIndexMap(db, blob, dataLocation, dataCaching)
	partitionMap := NewPartitionMap(db, blob, dataLocation, &pageMap)

	return Blob{
		m:                    &sync.Mutex{},
		blob:                 blob,
		db:                   db,
		pageMap:              &pageMap,
		indexMap:             &indexMap,
		partitionMap:         &partitionMap,
		partition:            diskModels.Partition{},
		format:               format,
		indexDiskManager:     indexDiskManager,
		partitionDiskManager: partitionDiskManager,
		blobDiskManager:      blobDiskManager,
	}, nil
}

func (b *Blob) DeleteBlob() error {
	b.m.Lock()
	defer b.m.Unlock()
	return b.blobDiskManager.Delete(b.db, b.blob)
}

func (b *Blob) GetByRecordId(pageRecordId string) (PageRecordsMap, error) {
	indexFiles, err := b.indexMap.GetByPrefix(b.indexDiskManager.GetPageRecordIdPrefix(pageRecordId))
	if err != nil {
		return PageRecordsMap{}, nil
	}
	for _, indexFile := range indexFiles {
		if indexFile == nil {
			continue
		}
		indexRecords, err := indexFile.Read()
		if err != nil {
			return PageRecordsMap{}, nil
		}
		if pageFile, ok := indexRecords[pageRecordId]; ok {
			page, err := b.pageMap.Get(pageFile)
			if !ok {
				return PageRecordsMap{}, err
			}
			data, err := page.Read()
			if err != nil {
				return PageRecordsMap{}, nil
			}
			record, ok := data[pageRecordId]
			if !ok {
				return PageRecordsMap{}, fmt.Errorf("record with id %s not found in page %s", pageRecordId, pageFile)
			}
			var formatter BlobFormatter
			if b.IsPartition() {
				formatter = CreateFormatterWithPartition(b.blob, b.format, b.partition)
			} else {
				formatter = CreateFormatter(b.blob, b.format)
			}
			formattedRecord, err := formatter.FormatRecord(record)
			if err != nil {
				return PageRecordsMap{}, err
			}
			return PageRecordsMap{
				pageFile: {
					pageRecordId: formattedRecord,
				},
			}, nil
		}
	}
	return PageRecordsMap{}, nil
}

func (b *Blob) GetFullScan(filterItems []FilterItem) (PageRecordsMap, error) {
	filter := Filter{FilterItems: filterItems, Format: b.format}
	err := filter.ConvertFilterItems()
	if err != nil {
		return PageRecordsMap{}, err
	}
	var wg sync.WaitGroup
	total := PageRecordsMap{}
	pages := b.pageMap.GetAll()
	for i := 0; i < len(pages); i += memoryConstants.SearchThreadCount {
		var groups [memoryConstants.SearchThreadCount]diskModels.PageRecords
		threadItem := i
		threadIndex := 0
		for threadItem < len(pages) && threadIndex < memoryConstants.SearchThreadCount {
			wg.Add(1)
			go b.SearchPage(pages[threadItem], filter, &groups, &wg, threadIndex)
			threadIndex++
			threadItem++
		}
		wg.Wait()
		currentFileIndex := i

		for _, groupItem := range groups {
			if len(groupItem) == 0 {
				currentFileIndex++
				continue
			}
			total[pages[currentFileIndex].fileName] = groupItem
			currentFileIndex++
		}
	}
	return total, nil
}

func (b *Blob) GetByPartition(searchPartition SearchPartition, filterItems []FilterItem) (PageRecordsMap, error) {
	if b.partition.Keys == nil {
		return PageRecordsMap{}, nil
	}
	filter := Filter{FilterItems: filterItems, Format: b.format}
	err := filter.ConvertFilterItems()
	if err != nil {
		return PageRecordsMap{}, err
	}
	hashKeyFiles, err := b.FilterHashKeyFiles(b.partitionMap.GetAllHashKeys(), searchPartition)
	if err != nil {
		return PageRecordsMap{}, err
	}
	total := PageRecordsMap{}
	for _, hashKeyFile := range hashKeyFiles {
		pages, err := b.partitionMap.GetByHash(hashKeyFile)
		if err != nil {
			return total, err
		}
		var wg sync.WaitGroup
		for i := 0; i < len(pages); i += memoryConstants.SearchThreadCount {
			var groups [memoryConstants.SearchThreadCount]diskModels.PageRecords
			threadItem := i
			threadIndex := 0
			for threadItem < len(pages) && threadIndex < memoryConstants.SearchThreadCount {
				wg.Add(1)
				go b.SearchPage(pages[threadItem], filter, &groups, &wg, threadIndex)
				threadIndex++
				threadItem++
			}
			wg.Wait()
			currentFileIndex := i

			for _, groupItem := range groups {
				if len(groupItem) == 0 {
					currentFileIndex++
					continue
				}
				total[pages[currentFileIndex].fileName] = groupItem
				currentFileIndex++
			}
		}
	}
	return total, nil
}

func (b *Blob) AddWithPartition(insertPageRecords []diskModels.PageRecord) (PageRecordsMap, error) {
	if b.partition.Keys == nil {
		return PageRecordsMap{}, nil
	}
	b.m.Lock()
	defer b.m.Unlock()
	formatter := CreateFormatterWithPartition(b.blob, b.format, b.partition)
	hashKeyMap := make(map[string][]diskModels.PageRecord)
	for _, insertPageRecord := range insertPageRecords {
		newInsertRecord, err := formatter.FormatRecord(insertPageRecord)
		if err != nil {
			return PageRecordsMap{}, err
		}
		hashKey, err := b.partitionDiskManager.GetHashKey(b.partition, newInsertRecord)
		if err != nil {
			return PageRecordsMap{}, err
		}
		_, ok := hashKeyMap[hashKey]
		if !ok {
			hashKeyMap[hashKey] = []diskModels.PageRecord{}
		}
		hashKeyMap[hashKey] = append(hashKeyMap[hashKey], newInsertRecord)
	}
	total := PageRecordsMap{}
	for hashKey, pageRecords := range hashKeyMap {
		partitionTotal, err := b.addRecordsByPartition(hashKey, pageRecords)
		if err != nil {
			return total, err
		}
		for pageFile, data := range partitionTotal {
			if len(data) > 0 {
				total[pageFile] = data
			}
		}
	}
	return total, nil
}

func (b *Blob) Add(insertPageRecords []diskModels.PageRecord) (PageRecordsMap, error) {
	if b.partition.Keys != nil {
		return PageRecordsMap{}, nil
	}
	b.m.Lock()
	defer b.m.Unlock()
	currentPage, err := b.pageMap.GetCurrentPage()
	if err != nil {
		currentPage, err = b.pageMap.Add()
		if err != nil {
			return PageRecordsMap{}, err
		}
	}
	formatter := CreateFormatter(b.blob, b.format)
	pageRecords, err := currentPage.Read()
	if err != nil {
		return nil, err
	}
	total := PageRecordsMap{}
	total[currentPage.fileName] = diskModels.PageRecords{}
	indexes := diskModels.IndexRecords{}
	for _, insertPageRecord := range insertPageRecords {
		formattedInsertRecord, err := formatter.FormatRecord(insertPageRecord)
		if err != nil {
			return total, err
		}
		lastRecordId := uuid.New().String()
		pageRecords[lastRecordId] = formattedInsertRecord
		total[currentPage.fileName][lastRecordId] = formattedInsertRecord
		indexes[lastRecordId] = currentPage.fileName
		if len(pageRecords) > memoryConstants.MaxPageSize {
			err = currentPage.Write(pageRecords)
			if err != nil {
				delete(total, currentPage.fileName)
				return total, err
			}
			currentPage, err = b.pageMap.Add()
			if err != nil {
				return total, err
			}
			total[currentPage.fileName] = diskModels.PageRecords{}
			pageRecords = diskModels.PageRecords{}
		}
	}
	err = currentPage.Write(pageRecords)
	if err != nil {
		delete(total, currentPage.fileName)
		return total, err
	}
	err = b.addIndexes(indexes)
	return total, err
}

func (b *Blob) UpdateByIndex(pageRecordId string, updateRecord diskModels.PageRecord) (PageRecordsMap, error) {
	b.m.Lock()
	defer b.m.Unlock()
	var formatter BlobFormatter
	if b.partition.Keys == nil {
		formatter = CreateFormatter(b.blob, b.format)
	} else {
		formatter = CreateFormatterWithPartition(b.blob, b.format, b.partition)
	}
	updateRecordFormatted, err := formatter.FormatUpdateRecord(updateRecord)
	if err != nil {
		return PageRecordsMap{}, err
	}
	indexFiles, err := b.indexMap.GetByPrefix(b.indexDiskManager.GetPageRecordIdPrefix(pageRecordId))
	if err != nil {
		return PageRecordsMap{}, err
	}
	for _, indexFile := range indexFiles {
		if indexFile == nil {
			continue
		}
		indexRecords, err := indexFile.Read()
		if err != nil {
			return PageRecordsMap{}, nil
		}
		if pageFile, ok := indexRecords[pageRecordId]; ok {
			page, err := b.pageMap.Get(pageFile)
			if err != nil {
				return PageRecordsMap{}, err
			}
			data, err := page.Read()
			if err != nil {
				return PageRecordsMap{}, nil
			}
			_, ok := data[pageRecordId]
			if !ok {
				return PageRecordsMap{}, fmt.Errorf("record with id %s not found in page %s", pageRecordId, pageFile)
			}
			for key, value := range updateRecordFormatted {
				data[pageRecordId][key] = value
			}
			err = page.Write(data)
			if err != nil {
				return PageRecordsMap{}, err
			}
			return PageRecordsMap{
				pageFile: {
					pageRecordId: updateRecordFormatted,
				},
			}, nil
		}
	}
	return PageRecordsMap{}, nil
}

func (b *Blob) UpdateByPartition(updateRecord diskModels.PageRecord, searchPartition SearchPartition, filterItems []FilterItem) (PageRecordsMap, error) {
	if b.partition.Keys == nil {
		return PageRecordsMap{}, nil
	}
	b.m.Lock()
	defer b.m.Unlock()
	formatter := CreateFormatterWithPartition(b.blob, b.format, b.partition)
	updateRecordFormatted, err := formatter.FormatUpdateRecord(updateRecord)
	if err != nil {
		return PageRecordsMap{}, nil
	}
	filter := Filter{FilterItems: filterItems, Format: b.format}
	err = filter.ConvertFilterItems()
	if err != nil {
		return PageRecordsMap{}, err
	}
	hashKeyFiles, err := b.FilterHashKeyFiles(b.partitionMap.GetAllHashKeys(), searchPartition)
	if err != nil {
		return PageRecordsMap{}, err
	}
	total := PageRecordsMap{}
	for _, hashKeyFile := range hashKeyFiles {
		pages, err := b.partitionMap.GetByHash(hashKeyFile)
		if err != nil {
			return total, err
		}
		var wg sync.WaitGroup
		for i := 0; i < len(pages); i += memoryConstants.SearchThreadCount {
			var groups [memoryConstants.SearchThreadCount]diskModels.PageRecords
			threadItem := i
			threadIndex := 0
			for threadItem < len(pages) && threadIndex < memoryConstants.SearchThreadCount {
				wg.Add(1)
				go b.SearchPageUpdate(pages[threadItem], filter, &groups, &wg, threadIndex, updateRecordFormatted)
				threadIndex++
				threadItem++
			}
			wg.Wait()
			currentFileIndex := i

			for _, groupItem := range groups {
				if len(groupItem) == 0 {
					currentFileIndex++
					continue
				}
				total[pages[currentFileIndex].fileName] = groupItem
				currentFileIndex++
			}
		}
	}
	return total, nil
}

func (b *Blob) Update(updateRecord diskModels.PageRecord, filterItems []FilterItem) (PageRecordsMap, error) {
	b.m.Lock()
	defer b.m.Unlock()
	filter := Filter{FilterItems: filterItems, Format: b.format}
	err := filter.ConvertFilterItems()
	if err != nil {
		return nil, err
	}
	pages := b.pageMap.GetAll()
	var formatter BlobFormatter
	if b.partition.Keys == nil {
		formatter = CreateFormatter(b.blob, b.format)
	} else {
		formatter = CreateFormatterWithPartition(b.blob, b.format, b.partition)
	}
	updateRecordFormatted, err := formatter.FormatUpdateRecord(updateRecord)
	if err != nil {
		return nil, err
	}
	total := PageRecordsMap{}
	var wg sync.WaitGroup
	for i := 0; i < len(pages); i += memoryConstants.SearchThreadCount {
		var groups [memoryConstants.SearchThreadCount]diskModels.PageRecords
		threadItem := i
		threadIndex := 0
		for threadItem < len(pages) && threadIndex < memoryConstants.SearchThreadCount {
			wg.Add(1)
			go b.SearchPageUpdate(pages[threadItem], filter, &groups, &wg, threadIndex, updateRecordFormatted)
			threadIndex++
			threadItem++
		}
		wg.Wait()
		currentFileIndex := i

		for _, groupItem := range groups {
			if len(groupItem) == 0 {
				currentFileIndex++
				continue
			}
			total[pages[currentFileIndex].fileName] = groupItem
			currentFileIndex++
		}
	}
	return total, nil
}

func (b *Blob) DeleteByIndex(pageRecordId string) (PageRecordsMap, error) {
	b.m.Lock()
	defer b.m.Unlock()
	indexFiles, err := b.indexMap.GetByPrefix(b.indexDiskManager.GetPageRecordIdPrefix(pageRecordId))
	if err != nil {
		return PageRecordsMap{}, nil
	}
	for _, indexFile := range indexFiles {
		if indexFile == nil {
			continue
		}
		indexRecords, err := indexFile.Read()
		if err != nil {
			return PageRecordsMap{}, nil
		}
		if pageFile, ok := indexRecords[pageRecordId]; ok {
			page, err := b.pageMap.Get(pageFile)
			if err != nil {
				return PageRecordsMap{}, err
			}
			data, err := page.Read()
			if err != nil {
				return PageRecordsMap{}, nil
			}
			deletedRecord, ok := data[pageRecordId]
			if !ok {
				return PageRecordsMap{}, fmt.Errorf("record with id %s not found in page %s", pageRecordId, pageFile)
			}

			delete(data, pageRecordId)
			err = page.Write(data)
			if err != nil {
				return PageRecordsMap{}, err
			}

			delete(indexRecords, pageRecordId)
			err = indexFile.Write(indexRecords)
			if err != nil {
				return PageRecordsMap{}, err
			}
			return PageRecordsMap{
				pageFile: {
					pageRecordId: deletedRecord,
				},
			}, nil
		}
	}
	return PageRecordsMap{}, nil
}

func (b *Blob) DeleteByPartition(searchPartition SearchPartition, filterItems []FilterItem) (PageRecordsMap, error) {
	if b.partition.Keys == nil {
		return PageRecordsMap{}, nil
	}
	b.m.Lock()
	defer b.m.Unlock()
	filter := Filter{FilterItems: filterItems, Format: b.format}
	err := filter.ConvertFilterItems()
	if err != nil {
		return PageRecordsMap{}, err
	}
	hashKeyFiles, err := b.FilterHashKeyFiles(b.partitionMap.GetAllHashKeys(), searchPartition)
	if err != nil {
		return PageRecordsMap{}, err
	}
	total := PageRecordsMap{}
	for _, hashKeyFile := range hashKeyFiles {
		pages, err := b.partitionMap.GetByHash(hashKeyFile)
		if err != nil {
			return total, err
		}
		var wg sync.WaitGroup
		for i := 0; i < len(pages); i += memoryConstants.SearchThreadCount {
			var groups [memoryConstants.SearchThreadCount]diskModels.PageRecords
			threadItem := i
			threadIndex := 0
			for threadItem < len(pages) && threadIndex < memoryConstants.SearchThreadCount {
				wg.Add(1)
				go b.SearchPageDelete(pages[threadItem], filter, &groups, &wg, threadIndex)
				threadIndex++
				threadItem++
			}
			wg.Wait()
			currentFileIndex := i

			for _, groupItem := range groups {
				if len(groupItem) == 0 {
					currentFileIndex++
					continue
				}
				pageRecordIds := []string{}
				for pageRecordId, _ := range groupItem {
					pageRecordIds = append(pageRecordIds, pageRecordId)
				}
				total[pages[currentFileIndex].fileName] = groupItem
				currentFileIndex++
			}
		}
	}
	return total, nil
}

func (b *Blob) Delete(filterItems []FilterItem) (PageRecordsMap, error) {
	b.m.Lock()
	defer b.m.Unlock()
	filter := Filter{FilterItems: filterItems, Format: b.format}
	err := filter.ConvertFilterItems()
	if err != nil {
		return PageRecordsMap{}, err
	}
	pages := b.pageMap.GetAll()
	total := PageRecordsMap{}
	var wg sync.WaitGroup
	for i := 0; i < len(pages); i += memoryConstants.SearchThreadCount {
		var groups [memoryConstants.SearchThreadCount]diskModels.PageRecords
		threadItem := i
		threadIndex := 0
		for threadItem < len(pages) && threadIndex < memoryConstants.SearchThreadCount {
			wg.Add(1)
			go b.SearchPageDelete(pages[threadItem], filter, &groups, &wg, threadIndex)
			threadIndex++
			threadItem++
		}
		wg.Wait()
		currentFileIndex := i

		for _, groupItem := range groups {
			if len(groupItem) == 0 {
				currentFileIndex++
				continue
			}
			pageRecordIds := []string{}
			for pageRecordId, _ := range groupItem {
				pageRecordIds = append(pageRecordIds, pageRecordId)
			}
			total[pages[currentFileIndex].fileName] = groupItem
			currentFileIndex++
		}
	}
	return total, nil
}

func (b *Blob) SearchPage(page *Page, filter Filter, groups *[memoryConstants.SearchThreadCount]diskModels.PageRecords, wg *sync.WaitGroup, index int) {
	defer wg.Done()
	if page == nil {
		return
	}
	var formatter BlobFormatter
	if b.IsPartition() {
		formatter = CreateFormatterWithPartition(b.blob, b.format, b.partition)
	} else {
		formatter = CreateFormatter(b.blob, b.format)
	}
	groupItem := diskModels.PageRecords{}
	pageData, err := page.Read()
	if err != nil {
		return
	}
	for key, record := range pageData {
		if passes, _ := filter.Passes(record); passes {
			formattedRecord, err := formatter.FormatRecord(record)
			if err == nil {
				groupItem[key] = formattedRecord
			}
		}
	}
	groups[index] = groupItem
}

func (b *Blob) SearchPageUpdate(page *Page, filter Filter, groups *[memoryConstants.SearchThreadCount]diskModels.PageRecords, wg *sync.WaitGroup, index int, updateRecordFormatted diskModels.PageRecord) {
	defer wg.Done()
	if page == nil {
		return
	}
	groupItem := diskModels.PageRecords{}
	pageData, err := page.Read()
	if err != nil {
		return
	}
	affected := false
	for pageRecordId, record := range pageData {
		if passes, _ := filter.Passes(record); passes {
			for key, value := range updateRecordFormatted {
				pageData[pageRecordId][key] = value
			}
			groupItem[pageRecordId] = pageData[pageRecordId]
			affected = true
		}
	}
	if affected {
		err = page.Write(pageData)
		if err != nil {
			return
		}
	}
	groups[index] = groupItem
}

func (b *Blob) SearchPageDelete(page *Page, filter Filter, groups *[memoryConstants.SearchThreadCount]diskModels.PageRecords, wg *sync.WaitGroup, index int) {
	defer wg.Done()
	groupItem := diskModels.PageRecords{}
	if page == nil {
		return
	}
	pageData, err := page.Read()
	if err != nil {
		return
	}
	affected := false
	pageRecordIds := []string{}
	for pageRecordId, record := range pageData {
		if passes, _ := filter.Passes(record); passes {
			groupItem[pageRecordId] = pageData[pageRecordId]
			delete(pageData, pageRecordId)
			pageRecordIds = append(pageRecordIds, pageRecordId)
			affected = true
		}
	}
	if affected {
		if len(pageData) == 0 {
			isPhantomFile, err := b.pageMap.Delete(page.fileName)
			if (err == nil || isPhantomFile) && b.partition.Keys != nil {
				hashKey, err := b.partitionDiskManager.GetHashKey(b.partition, groupItem[pageRecordIds[0]])
				if err == nil {
					_ = b.partitionMap.Delete(hashKey, page.fileName)
				}
			}
		} else {
			err = page.Write(pageData)
			if err != nil {
				return
			}
		}
		b.deleteIndexes(pageRecordIds)
	}
	groups[index] = groupItem
}

func (b *Blob) FilterHashKeyFiles(hashKeys []string, searchPartition SearchPartition) ([]string, error) {
	var foundFiles []string
	for _, partitionHashKeyFileName := range hashKeys {
		currentChar := 0
		found := true
		for _, partitionKey := range b.partition.Keys {
			_, ok := searchPartition[partitionKey]
			if !ok {
				currentChar += 28
				continue
			}
			valueHash, err := b.partitionDiskManager.GetHashKeyItem(partitionKey, diskModels.PageRecord(searchPartition))
			if err != nil {
				return nil, err
			}
			if partitionHashKeyFileName[currentChar:currentChar+len(valueHash)] != valueHash {
				found = false
				break
			}
			currentChar += 28
		}
		if found {
			foundFiles = append(foundFiles, partitionHashKeyFileName)
		}
	}
	return foundFiles, nil
}

func (b *Blob) IsPartition() bool {
	return b.partition.Keys != nil
}

func (b *Blob) addRecordsByPartition(hashKeyFile string, insertPageRecords []diskModels.PageRecord) (PageRecordsMap, error) {
	pages, err := b.partitionMap.GetByHash(hashKeyFile)
	if err != nil {
		return PageRecordsMap{}, err
	}
	if len(pages) == 0 {
		page, err := b.pageMap.Add()
		if err != nil {
			return PageRecordsMap{}, err
		}
		err = b.partitionMap.Add(hashKeyFile, page.fileName)
		if err != nil {
			return PageRecordsMap{}, err
		}
	}
	currentPage, err := b.partitionMap.GetCurrentPage(hashKeyFile)
	if err != nil {
		return PageRecordsMap{}, err
	}
	pageRecords, err := currentPage.Read()
	if err != nil {
		return PageRecordsMap{}, err
	}
	lastPageRecordId := ""
	total := PageRecordsMap{}
	total[currentPage.fileName] = diskModels.PageRecords{}
	indexes := diskModels.IndexRecords{}
	for _, insertPageRecord := range insertPageRecords {
		lastPageRecordId = uuid.New().String()
		pageRecords[lastPageRecordId] = insertPageRecord
		total[currentPage.fileName][lastPageRecordId] = insertPageRecord
		indexes[lastPageRecordId] = currentPage.fileName
		if len(pageRecords) > memoryConstants.MaxPageSize {
			err = currentPage.Write(pageRecords)
			if err != nil {
				delete(total, currentPage.fileName)
				return total, err
			}
			pageRecords = diskModels.PageRecords{}
			currentPage, err = b.pageMap.Add()
			if err != nil {
				return total, err
			}
			err = b.partitionMap.Add(hashKeyFile, currentPage.fileName)
			if err != nil {
				return total, err
			}
			total[currentPage.fileName] = diskModels.PageRecords{}
		}
	}
	err = currentPage.Write(pageRecords)
	if err != nil {
		delete(total, currentPage.fileName)
		return total, err
	}
	return total, b.addIndexes(indexes)
}

func (b *Blob) addIndexes(indexes diskModels.IndexRecords) error {
	indexFileMap := make(map[string]diskModels.IndexRecords)
	indexPrefixMap := make(map[string]string)
	for pageRecordId, pageFile := range indexes {
		prefix := b.indexDiskManager.GetPageRecordIdPrefix(pageRecordId)
		var currentIndex *Index = nil
		index, err := b.indexMap.GetCurrentIndex(prefix)
		if err != nil {
			index, err = b.indexMap.Add(pageRecordId)
			if err != nil {
				return err
			}
		}
		currentIndex = index
		indexPrefixMap[prefix] = currentIndex.fileName
		_, ok := indexFileMap[currentIndex.fileName]
		if !ok {
			indexData, err := currentIndex.Read()
			if err != nil {
				return err
			}
			indexFileMap[currentIndex.fileName] = indexData
		}
		indexFileMap[currentIndex.fileName][pageRecordId] = pageFile
		if len(indexFileMap[currentIndex.fileName]) > memoryConstants.MaxIndexSize {
			err = currentIndex.Write(indexFileMap[currentIndex.fileName])
			if err != nil {
				return err
			}
			delete(indexFileMap, currentIndex.fileName)
			delete(indexPrefixMap, prefix)
			_, err = b.indexMap.Add(pageRecordId)
			if err != nil {
				return err
			}
		}
	}
	for prefix, indexFile := range indexPrefixMap {
		index, err := b.indexMap.Get(prefix, indexFile)
		if err != nil {
			return err
		}
		err = index.Write(indexFileMap[indexFile])
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Blob) deleteIndexes(pageRecordIds []string) {
	pageRecordIdMap := make(map[string][]string)
	indexMap := make(map[string][]*Index)
	for _, pageRecordId := range pageRecordIds {
		prefix := b.indexDiskManager.GetPageRecordIdPrefix(pageRecordId)
		if _, ok := pageRecordIdMap[prefix]; !ok {
			pageRecordIdMap[prefix] = []string{}
			indexes, err := b.indexMap.GetByPrefix(prefix)
			if err != nil {
				continue
			}
			indexMap[prefix] = indexes
		}
		pageRecordIdMap[prefix] = append(pageRecordIdMap[prefix], pageRecordId)
	}

	for prefix, indexes := range indexMap {
		for _, index := range indexes {
			length, err := index.Delete(pageRecordIdMap[prefix])
			if err == nil && length == 0 {
				_ = b.indexMap.Delete(prefix, index.fileName)
			}
		}
	}
}
