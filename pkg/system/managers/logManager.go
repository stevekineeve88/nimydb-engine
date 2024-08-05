package systemManagers

import (
	"encoding/hex"
	"encoding/json"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/constants"
	"sort"
)

type LogManager interface {
	AddLog(query queryModels.Query) error
	GetLogs(filterItems []memoryModels.FilterItem) ([]queryModels.Log, error)
	GetCurrent() queryModels.Log
}

type logManager struct {
	operationManager memoryManagers.OperationManager
}

func CreateLogManager(operationManager memoryManagers.OperationManager) LogManager {
	return &logManager{
		operationManager: operationManager,
	}
}

func (lm *logManager) AddLog(query queryModels.Query) error {
	currentLog := lm.GetCurrent()
	if currentLog.Version == 0 {
		hexString, err := lm.convertToHex(query)
		if err != nil {
			return err
		}
		_, err = lm.operationManager.AddRecords(systemConstants.DBSys, systemConstants.BlobSysLog, []diskModels.PageRecord{
			{
				"is_current": true,
				"version":    1,
				"query_hex":  hexString,
			},
		})
		return err
	}
	err := lm.operationManager.UpdateRecordByIndex(systemConstants.DBSys, systemConstants.BlobSysLog, currentLog.Id, diskModels.PageRecord{
		"is_current": false,
	})
	if err != nil {
		panic(err)
	}
	hexString, err := lm.convertToHex(query)
	if err != nil {
		return err
	}
	_, err = lm.operationManager.AddRecords(systemConstants.DBSys, systemConstants.BlobSysLog, []diskModels.PageRecord{
		{
			"is_current": true,
			"version":    currentLog.Version + 1,
			"query_hex":  hexString,
		},
	})
	return err
}

func (lm *logManager) GetLogs(filterItems []memoryModels.FilterItem) ([]queryModels.Log, error) {
	records, err := lm.operationManager.GetRecords(
		systemConstants.DBSys,
		systemConstants.BlobSysLog,
		filterItems,
		memoryModels.SearchPartition{},
		memoryModels.GetOperationParams{},
	)
	if err != nil {
		return nil, err
	}
	logs := []queryModels.Log{}
	for _, record := range records {
		if query, err := lm.convertToQuery(record["query_hex"].(string)); err == nil {
			logs = append(logs, queryModels.Log{
				Id:      record["_id"].(string),
				Version: record["version"].(int),
				Query:   query,
			})
		}
	}
	sort.Slice(logs[:], func(i, j int) bool {
		return logs[i].Version < logs[j].Version
	})
	return logs, nil
}

func (lm *logManager) GetCurrent() queryModels.Log {
	records, err := lm.operationManager.GetRecords(
		systemConstants.DBSys,
		systemConstants.BlobSysLog,
		[]memoryModels.FilterItem{{
			Key:   "is_current",
			Op:    "=",
			Value: true,
		}},
		memoryModels.SearchPartition{},
		memoryModels.GetOperationParams{},
	)
	if err != nil {
		panic(err.Error())
	}
	logs := []queryModels.Log{}
	for _, record := range records {
		if query, err := lm.convertToQuery(record["query_hex"].(string)); err == nil {
			logs = append(logs, queryModels.Log{
				Id:      record["_id"].(string),
				Version: record["version"].(int),
				Query:   query,
			})
		}
	}
	if len(logs) > 1 {
		panic("current log is corrupt")
	}
	if len(logs) == 0 {
		return queryModels.Log{}
	}
	return logs[0]
}

func (lm *logManager) convertToHex(query queryModels.Query) (string, error) {
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(queryBytes), nil
}

func (lm *logManager) convertToQuery(hexString string) (queryModels.Query, error) {
	queryBytes, err := hex.DecodeString(hexString)
	if err != nil {
		return queryModels.Query{}, err
	}
	var query queryModels.Query
	err = json.Unmarshal(queryBytes, &query)
	if err != nil {
		return queryModels.Query{}, err
	}
	return query, err
}
