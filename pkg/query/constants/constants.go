package queryConstants

import "github.com/stevekineeve88/nimydb-engine/pkg/system/constants"

const (
	ActionCreate = "create"
	ActionDelete = "delete"
	ActionUpdate = "update"
	ActionGet    = "get"

	OnDB         = "db"
	OnBlob       = "blob"
	OnData       = "data"
	OnLogs       = "logs"
	OnConnection = "connection"
)

func IsSystemDB(db string) bool {
	return db == systemConstants.DBSys
}
