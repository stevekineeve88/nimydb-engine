package queryModels

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/models"
)

type Query struct {
	Action string `json:"action"`
	On     string `json:"on"`
	Name   string `json:"name"`
	With   With   `json:"with"`
}

type With struct {
	Format          map[string]string            `json:"format,omitempty"`
	Partition       []string                     `json:"partition,omitempty"`
	UpdateRecord    diskModels.PageRecord        `json:"updateRecord,omitempty"`
	Records         []diskModels.PageRecord      `json:"records,omitempty"`
	Index           string                       `json:"index,omitempty"`
	SearchPartition memoryModels.SearchPartition `json:"searchPartition,omitempty"`
	Filter          []memoryModels.FilterItem    `json:"filter,omitempty"`
	UserConnection  systemModels.UserConnection  `json:"userConnection,omitempty"`
}

type QueryResult struct {
	Records        []diskModels.PageRecord `json:"records,omitempty"`
	ConnectionUser systemModels.User       `json:"connectionUser,omitempty"`
	ErrorMessage   string                  `json:"error_message,omitempty"`
}

type NameSplit struct {
	DB   string
	Blob string
}

type Log struct {
	Id      string
	Version int
	Query   Query
}

func (l *Log) ConvertToPageRecord() diskModels.PageRecord {
	return diskModels.PageRecord{
		memoryConstants.IdKey: l.Id,
		"version":             l.Version,
		"query":               l.Query,
	}
}
