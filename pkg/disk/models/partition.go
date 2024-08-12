package diskModels

type Partition struct {
	Keys []string `json:"keys"`
}

type PartitionPages []PartitionPageItem

type PartitionPageItem struct {
	FileName string `json:"fileName"`
}

func (p Partition) ConvertToPageRecords() []PageRecord {
	pageRecords := []PageRecord{}
	for _, key := range p.Keys {
		pageRecords = append(pageRecords, PageRecord{
			"key": key,
		})
	}
	return pageRecords
}
