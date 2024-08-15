package diskModels

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnit_ConvertToPageRecords_ConvertsToPartitionPageRecords(t *testing.T) {
	partition := Partition{Keys: []string{"key_1", "key_2"}}
	pageRecords := partition.ConvertToPageRecords()

	assert.Equal(t, len(partition.Keys), len(pageRecords))
	assert.Equal(t, "key_1", pageRecords[0]["key"].(string))
	assert.Equal(t, "key_2", pageRecords[1]["key"].(string))
}
