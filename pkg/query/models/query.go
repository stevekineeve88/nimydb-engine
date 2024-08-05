package queryModels

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/models"
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
}

type QueryResult struct {
	Records      []diskModels.PageRecord `json:"records,omitempty"`
	ErrorMessage string                  `json:"error_message,omitempty"`
}

type NameSplit struct {
	DB   string
	Blob string
}
