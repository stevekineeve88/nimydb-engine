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
	dataLocation   string
	createFileFunc func(filePath string) error
	writeFileFunc  func(filePath string, fileData []byte) error
	getFileFunc    func(filePath string) ([]byte, error)
}

var formatManagerInstance FormatManager

func CreateFormatManager(dataLocation string) FormatManager {
	if formatManagerInstance == nil {
		formatManagerInstance = &formatManager{
			dataLocation:   dataLocation,
			createFileFunc: diskUtils.CreateFile,
			writeFileFunc:  diskUtils.WriteFile,
			getFileFunc:    diskUtils.GetFile,
		}
	}
	return formatManagerInstance
}

func DestructFormatManager() {
	formatManagerInstance = nil
}

func (fdm *formatManager) Create(db string, blob string, format diskModels.Format) error {
	formatData, _ := json.Marshal(format)
	filePath := fmt.Sprintf("%s/%s/%s/%s", fdm.dataLocation, db, blob, formatFile)
	err := fdm.createFileFunc(filePath)
	if err != nil {
		return err
	}
	return fdm.writeFileFunc(filePath, formatData)
}

func (fdm *formatManager) Get(db string, blob string) (diskModels.Format, error) {
	var format diskModels.Format
	file, err := fdm.getFileFunc(fmt.Sprintf("%s/%s/%s/%s", fdm.dataLocation, db, blob, formatFile))
	if err != nil {
		return format, err
	}

	err = json.Unmarshal(file, &format)
	return format, err
}
