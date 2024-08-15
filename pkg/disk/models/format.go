package diskModels

type Format map[string]FormatItem

type FormatItem struct {
	KeyType string `json:"keyType"`
}

func (f Format) ConvertToPageRecords() []PageRecord {
	pageRecords := []PageRecord{}
	for key, data := range f {
		pageRecords = append(pageRecords, PageRecord{
			"key":      key,
			"key_type": data.KeyType,
		})
	}
	return pageRecords
}
