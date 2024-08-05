package system

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/constants"
)

func InitDB(operationManager memoryManagers.OperationManager) {
	_buildDB(operationManager)
	_buildSysLogs(operationManager)
}

func _buildDB(operationManager memoryManagers.OperationManager) {
	if operationManager.DBExists(systemConstants.DBSys) {
		return
	}
	if err := operationManager.CreateDB(systemConstants.DBSys); err != nil {
		panic(err)
	}
}

func _buildSysLogs(operationManager memoryManagers.OperationManager) {
	if operationManager.BlobExists(systemConstants.DBSys, systemConstants.BlobSysLog) {
		return
	}
	if err := operationManager.CreateBlob(systemConstants.DBSys, systemConstants.BlobSysLog, diskModels.Format{
		"is_current": diskModels.FormatItem{KeyType: memoryConstants.Bool},
		"version":    diskModels.FormatItem{KeyType: memoryConstants.Int},
		"query_hex":  diskModels.FormatItem{KeyType: memoryConstants.String},
	}, nil); err != nil {
		panic(err)
	}
}
