package memoryConstants

const (
	String   = "string"
	Int      = "int"
	Bool     = "bool"
	DateTime = "datetime"
	Date     = "date"
	Float    = "float"

	SearchThreadCount = 10

	BlobMaxLength = 25
	BlobRegex     = "^[a-z_]*$"
	BlobRegexDesc = "snake case"

	DBMaxLength = 25
	DBRegex     = "^[a-z_]*$"
	DBRegexDesc = "snake case"

	KeyMaxLength = 45
	KeyRegex     = "^[a-z_]*$"
	KeyRegexDesc = "snake case"

	MaxPageSize  = 1024 * 50
	MaxIndexSize = 5024 * 100

	IdKey = "_id"
)

func GetFormatTypes() []string {
	return []string{
		String,
		Int,
		Bool,
		DateTime,
		Date,
		Float,
	}
}
