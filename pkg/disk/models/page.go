package diskModels

type Pages []PageItem

type PageItem struct {
	FileName string `json:"fileName"`
}

type PageRecords map[string]PageRecord

type PageRecord map[string]any
