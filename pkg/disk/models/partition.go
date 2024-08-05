package diskModels

type Partition struct {
	Keys []string `json:"keys"`
}

type PartitionPages []PartitionPageItem

type PartitionPageItem struct {
	FileName string `json:"fileName"`
}
