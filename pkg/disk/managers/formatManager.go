package diskManagers

import (
	"encoding/json"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
)

const (
	formatFile = "format.json"
)

type FormatManager interface {
	Create(db string, blob string, format diskModels.Format) error
	Get(db string, blob string) (diskModels.Format, error)
}

type formatManager struct {
	dataLocation string
}

var formatManagerInstance *formatManager

func CreateFormatManager(dataLocation string) FormatManager {
	if formatManagerInstance == nil {
		formatManagerInstance = &formatManager{dataLocation: dataLocation}
	}
	return formatManagerInstance
}

func (fdm *formatManager) Create(db string, blob string, format diskModels.Format) error {
	formatData, err := json.Marshal(format)
	if err != nil {
		return err
	}
	filePath := fmt.Sprintf("%s/%s/%s/%s", fdm.dataLocation, db, blob, formatFile)
	err = diskUtils.CreateFile(filePath)
	if err != nil {
		return err
	}
	return diskUtils.WriteFile(filePath, formatData)
}

func (fdm *formatManager) Get(db string, blob string) (diskModels.Format, error) {
	var format diskModels.Format
	file, err := diskUtils.GetFile(fmt.Sprintf("%s/%s/%s/%s", fdm.dataLocation, db, blob, formatFile))
	if err != nil {
		return format, err
	}

	err = json.Unmarshal(file, &format)
	return format, err
}
