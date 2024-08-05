package memoryManagers

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/models"
)

type OperationManager interface {
	CreateDB(db string) error
	DeleteDB(db string) error
	CreateBlob(db string, blob string, format diskModels.Format, partition *diskModels.Partition) error
	DeleteBlob(db string, blob string) error
	GetRecordByIndex(db string, blob string, index string) (diskModels.PageRecord, error)
	GetRecords(db string, blob string, filterItems []memoryModels.FilterItem, searchPartition memoryModels.SearchPartition, getOperationParams memoryModels.GetOperationParams) ([]diskModels.PageRecord, error)
	AddRecords(db string, blob string, records []diskModels.PageRecord) ([]diskModels.PageRecord, error)
	UpdateRecordByIndex(db string, blob string, index string, updateRecord diskModels.PageRecord) error
	UpdateRecords(db string, blob string, filterItems []memoryModels.FilterItem, searchPartition memoryModels.SearchPartition, updateRecord diskModels.PageRecord) error
	DeleteRecordByIndex(db string, blob string, index string) error
	DeleteRecords(db string, blob string, filterItems []memoryModels.FilterItem, searchPartition memoryModels.SearchPartition) error
	DBExists(db string) bool
	BlobExists(db string, blob string) bool
}

type operationManager struct {
	dbMap *memoryModels.DBMap
}

func CreateOperationManager(dbMap *memoryModels.DBMap) OperationManager {
	return &operationManager{
		dbMap: dbMap,
	}
}

func (om *operationManager) CreateDB(db string) error {
	_, err := om.dbMap.Add(db)
	return err
}

func (om *operationManager) DeleteDB(db string) error {
	return om.dbMap.Delete(db)
}

func (om *operationManager) CreateBlob(db string, blob string, format diskModels.Format, partition *diskModels.Partition) error {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return err
	}
	_, err = blobMap.Add(blob, format, partition)
	return err
}

func (om *operationManager) DeleteBlob(db string, blob string) error {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return err
	}
	return blobMap.Delete(blob)
}

func (om *operationManager) GetRecordByIndex(db string, blob string, index string) (diskModels.PageRecord, error) {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return diskModels.PageRecord{}, err
	}
	blobObj, err := blobMap.Get(blob)
	if err != nil {
		return diskModels.PageRecord{}, err
	}
	pageRecordsMap, err := blobObj.GetByRecordId(index)
	if err != nil {
		return diskModels.PageRecord{}, err
	}
	pageRecordArray := om.buildPageRecords(pageRecordsMap)
	if len(pageRecordArray) == 0 {
		return diskModels.PageRecord{}, nil
	}
	return pageRecordArray[0], nil
}

func (om *operationManager) GetRecords(db string, blob string, filterItems []memoryModels.FilterItem, searchPartition memoryModels.SearchPartition, getOperationParams memoryModels.GetOperationParams) ([]diskModels.PageRecord, error) {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return nil, err
	}
	blobObj, err := blobMap.Get(blob)
	if err != nil {
		return nil, err
	}
	if !blobObj.IsPartition() {
		pageRecordsMap, err := blobObj.GetFullScan(filterItems)
		if err != nil {
			return nil, err
		}
		return om.buildPageRecords(pageRecordsMap), nil
	} else {
		pageRecordsMap, err := blobObj.GetByPartition(searchPartition, filterItems)
		if err != nil {
			return nil, err
		}
		return om.buildPageRecords(pageRecordsMap), nil
	}
}

func (om *operationManager) AddRecords(db string, blob string, records []diskModels.PageRecord) ([]diskModels.PageRecord, error) {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return nil, err
	}
	blobObj, err := blobMap.Get(blob)
	if err != nil {
		return nil, err
	}
	var pageRecordsMap memoryModels.PageRecordsMap
	var addError error
	if blobObj.IsPartition() {
		pageRecordsMap, addError = blobObj.AddWithPartition(records)
	} else {
		pageRecordsMap, addError = blobObj.Add(records)
	}
	return om.buildPageRecords(pageRecordsMap), addError
}

func (om *operationManager) UpdateRecordByIndex(db string, blob string, index string, updateRecord diskModels.PageRecord) error {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return err
	}
	blobObj, err := blobMap.Get(blob)
	if err != nil {
		return err
	}
	_, err = blobObj.UpdateByIndex(index, updateRecord)
	return err
}

func (om *operationManager) UpdateRecords(db string, blob string, filterItems []memoryModels.FilterItem, searchPartition memoryModels.SearchPartition, updateRecord diskModels.PageRecord) error {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return err
	}
	blobObj, err := blobMap.Get(blob)
	if err != nil {
		return err
	}
	var updateError error
	if blobObj.IsPartition() {
		_, updateError = blobObj.UpdateByPartition(updateRecord, searchPartition, filterItems)
	} else {
		_, updateError = blobObj.Update(updateRecord, filterItems)
	}
	return updateError
}
func (om *operationManager) DeleteRecordByIndex(db string, blob string, index string) error {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return err
	}
	blobObj, err := blobMap.Get(blob)
	if err != nil {
		return err
	}
	_, err = blobObj.DeleteByIndex(index)
	return err
}

func (om *operationManager) DeleteRecords(db string, blob string, filterItems []memoryModels.FilterItem, searchPartition memoryModels.SearchPartition) error {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return err
	}
	blobObj, err := blobMap.Get(blob)
	if err != nil {
		return err
	}
	var deleteError error
	if blobObj.IsPartition() {
		_, deleteError = blobObj.DeleteByPartition(searchPartition, filterItems)
	} else {
		_, deleteError = blobObj.Delete(filterItems)
	}
	return deleteError
}

func (om *operationManager) DBExists(db string) bool {
	_, err := om.dbMap.GetBlobMap(db)
	return err == nil
}

func (om *operationManager) BlobExists(db string, blob string) bool {
	blobMap, err := om.dbMap.GetBlobMap(db)
	if err != nil {
		return false
	}
	_, err = blobMap.Get(blob)
	return err == nil
}

func (om *operationManager) buildPageRecords(pageRecordsMap memoryModels.PageRecordsMap) []diskModels.PageRecord {
	formattedPageRecords := []diskModels.PageRecord{}
	for _, pageRecords := range pageRecordsMap {
		for pageRecordId, pageRecord := range pageRecords {
			newPageRecord := diskModels.PageRecord{}
			for key, value := range pageRecord {
				newPageRecord[key] = value
			}
			newPageRecord[memoryConstants.IdKey] = pageRecordId
			formattedPageRecords = append(formattedPageRecords, newPageRecord)
		}
	}
	return formattedPageRecords
}
