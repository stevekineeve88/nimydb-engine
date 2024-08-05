package systemModels

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/query/models"
)

type Log struct {
	Id      string            `json:"id"`
	Version int               `json:"version"`
	Query   queryModels.Query `json:"query"`
}
