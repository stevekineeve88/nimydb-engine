package queryManagers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/models"
	"strings"
)

type QueryManager interface {
	Query(query queryModels.Query) queryModels.QueryResult
}

type queryManager struct {
	operationManager memoryManagers.OperationManager
}

func CreateQueryManager(operationManager memoryManagers.OperationManager) QueryManager {
	return &queryManager{
		operationManager: operationManager,
	}
}

func (qm *queryManager) Query(query queryModels.Query) queryModels.QueryResult {
	switch query.Action {
	case queryConstants.ActionCreate:
		return qm.handleActionCreate(query)
	case queryConstants.ActionDelete:
		queryResult := qm.handleActionDelete(query)
		return queryResult
	case queryConstants.ActionUpdate:
		queryResult := qm.handleActionUpdate(query)
		return queryResult
	case queryConstants.ActionGet:
		return qm.handleActionGet(query)
	default:
		return queryModels.QueryResult{
			ErrorMessage: fmt.Sprintf("action %s does not exist", query.Action),
		}
	}
}

func (qm *queryManager) handleActionCreate(query queryModels.Query) queryModels.QueryResult {
	switch query.On {
	case queryConstants.OnDB:
		errMessage := ""
		err := qm.operationManager.CreateDB(query.Name)
		if err != nil {
			errMessage = err.Error()
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
		}
	case queryConstants.OnBlob:
		nameSplit, err := qm.getSplitName(query.Name)
		if err != nil {
			return queryModels.QueryResult{
				ErrorMessage: err.Error(),
			}
		}
		if queryConstants.IsSystemDB(nameSplit.DB) {
			return queryModels.QueryResult{
				ErrorMessage: "cannot use system database",
			}
		}
		errMessage := ""
		err = qm.operationManager.CreateBlob(
			nameSplit.DB,
			nameSplit.Blob,
			qm.buildFormat(query.With.Format),
			qm.buildPartition(query.With.Partition),
		)
		if err != nil {
			errMessage = err.Error()
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
		}
	case queryConstants.OnData:
		nameSplit, err := qm.getSplitName(query.Name)
		if err != nil {
			return queryModels.QueryResult{
				ErrorMessage: err.Error(),
			}
		}
		if queryConstants.IsSystemDB(nameSplit.DB) {
			return queryModels.QueryResult{
				ErrorMessage: "cannot use system database",
			}
		}
		errMessage := ""
		records, err := qm.operationManager.AddRecords(
			nameSplit.DB,
			nameSplit.Blob,
			query.With.Records,
		)
		if err != nil {
			errMessage = err.Error()
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
			Records:      records,
		}
	default:
		return queryModels.QueryResult{
			ErrorMessage: fmt.Sprintf("%s not allowed on action %s", query.On, query.Action),
		}
	}
}

func (qm *queryManager) handleActionDelete(query queryModels.Query) queryModels.QueryResult {
	switch query.On {
	case queryConstants.OnDB:
		if queryConstants.IsSystemDB(query.Name) {
			return queryModels.QueryResult{
				ErrorMessage: "cannot use system database",
			}
		}
		errMessage := ""
		err := qm.operationManager.DeleteDB(query.Name)
		if err != nil {
			errMessage = err.Error()
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
		}
	case queryConstants.OnBlob:
		nameSplit, err := qm.getSplitName(query.Name)
		if err != nil {
			return queryModels.QueryResult{
				ErrorMessage: err.Error(),
			}
		}
		if queryConstants.IsSystemDB(nameSplit.DB) {
			return queryModels.QueryResult{
				ErrorMessage: "cannot use system database",
			}
		}
		errMessage := ""
		err = qm.operationManager.DeleteBlob(
			nameSplit.DB,
			nameSplit.Blob,
		)
		if err != nil {
			errMessage = err.Error()
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
		}
	case queryConstants.OnData:
		nameSplit, err := qm.getSplitName(query.Name)
		if err != nil {
			return queryModels.QueryResult{
				ErrorMessage: err.Error(),
			}
		}
		if queryConstants.IsSystemDB(nameSplit.DB) {
			return queryModels.QueryResult{
				ErrorMessage: "cannot use system database",
			}
		}
		errMessage := ""
		if query.With.Index != "" {
			err = qm.operationManager.DeleteRecordByIndex(
				nameSplit.DB,
				nameSplit.Blob,
				query.With.Index,
			)
			if err != nil {
				errMessage = err.Error()
			}
		} else {
			err = qm.operationManager.DeleteRecords(
				nameSplit.DB,
				nameSplit.Blob,
				query.With.Filter,
				query.With.SearchPartition,
			)
			if err != nil {
				errMessage = err.Error()
			}
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
		}
	default:
		return queryModels.QueryResult{
			ErrorMessage: fmt.Sprintf("%s not allowed on action %s", query.On, query.Action),
		}
	}
}

func (qm *queryManager) handleActionUpdate(query queryModels.Query) queryModels.QueryResult {
	switch query.On {
	case queryConstants.OnData:
		nameSplit, err := qm.getSplitName(query.Name)
		if err != nil {
			return queryModels.QueryResult{
				ErrorMessage: err.Error(),
			}
		}
		if queryConstants.IsSystemDB(nameSplit.DB) {
			return queryModels.QueryResult{
				ErrorMessage: "cannot use system database",
			}
		}
		errMessage := ""
		if query.With.Index != "" {
			err = qm.operationManager.UpdateRecordByIndex(
				nameSplit.DB,
				nameSplit.Blob,
				query.With.Index,
				query.With.UpdateRecord,
			)
			if err != nil {
				errMessage = err.Error()
			}
		} else {
			err = qm.operationManager.UpdateRecords(
				nameSplit.DB,
				nameSplit.Blob,
				query.With.Filter,
				query.With.SearchPartition,
				query.With.UpdateRecord,
			)
			if err != nil {
				errMessage = err.Error()
			}
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
		}
	default:
		return queryModels.QueryResult{
			ErrorMessage: fmt.Sprintf("%s not allowed on action %s", query.On, query.Action),
		}
	}
}

func (qm *queryManager) handleActionGet(query queryModels.Query) queryModels.QueryResult {
	switch query.On {
	case queryConstants.OnData:
		nameSplit, err := qm.getSplitName(query.Name)
		if err != nil {
			return queryModels.QueryResult{
				ErrorMessage: err.Error(),
			}
		}
		errMessage := ""
		var records []diskModels.PageRecord
		if query.With.Index != "" {
			record, err := qm.operationManager.GetRecordByIndex(
				nameSplit.DB,
				nameSplit.Blob,
				query.With.Index,
			)
			if err != nil {
				errMessage = err.Error()
			}
			records = append(records, record)
		} else {
			records, err = qm.operationManager.GetRecords(
				nameSplit.DB,
				nameSplit.Blob,
				query.With.Filter,
				query.With.SearchPartition,
				memoryModels.GetOperationParams{},
			)
			if err != nil {
				errMessage = err.Error()
			}
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
			Records:      records,
		}
	default:
		return queryModels.QueryResult{
			ErrorMessage: fmt.Sprintf("%s not allowed on action %s", query.On, query.Action),
		}
	}
}

func (qm *queryManager) getSplitName(name string) (queryModels.NameSplit, error) {
	items := strings.Split(name, ".")
	if len(items) != 2 {
		return queryModels.NameSplit{}, fmt.Errorf("%s is not valid", name)
	}
	return queryModels.NameSplit{
		DB:   items[0],
		Blob: items[1],
	}, nil
}

func (qm *queryManager) buildFormat(format map[string]string) diskModels.Format {
	formatObj := make(map[string]diskModels.FormatItem)
	for key, keyType := range format {
		formatObj[key] = diskModels.FormatItem{KeyType: keyType}
	}
	return formatObj
}

func (qm *queryManager) buildPartition(partition []string) *diskModels.Partition {
	if partition == nil || len(partition) == 0 {
		return nil
	}
	return &diskModels.Partition{Keys: partition}
}
