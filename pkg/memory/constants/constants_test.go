package memoryConstants

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnit_GetFormatTypes_GetsFormatTypes(t *testing.T) {
	assert.Equal(t, GetFormatTypes(), []string{
		String,
		Int,
		Bool,
		DateTime,
		Date,
		Float,
	})
}
