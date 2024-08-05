package diskModels

type Indexes map[string]IndexItem

type IndexItem struct {
	FileNames []string `json:"fileNames"`
}

type IndexRecords map[string]string
