package diskModels

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnit_ConvertToPageRecords_ConvertsToFormatPageRecords(t *testing.T) {
	format := Format{
		"key_1": FormatItem{KeyType: "string"},
		"key_2": FormatItem{KeyType: "int"},
	}
	pageRecords := format.ConvertToPageRecords()

	assert.Equal(t, len(format), len(pageRecords))
	assert.Equal(t, "key_1", pageRecords[0]["key"].(string))
	assert.Equal(t, format["key_1"].KeyType, pageRecords[0]["key_type"].(string))
	assert.Equal(t, "key_2", pageRecords[1]["key"].(string))
	assert.Equal(t, format["key_2"].KeyType, pageRecords[1]["key_type"].(string))
}
