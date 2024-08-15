package diskManagers

import (
	"encoding/json"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
)

const (
	pagesFile      = "pages.json"
	pagesDirectory = "pages"
)

type PageManager interface {
	Initialize(db string, blob string) error
	Create(db string, blob string) (string, error)
	GetAll(db string, blob string) (diskModels.Pages, error)
	GetData(db string, blob string, pageFileName string) (diskModels.PageRecords, error)
	WriteData(db string, blob string, pageFileName string, data diskModels.PageRecords) error
	Delete(db string, blob string, pageFileName string) (bool, error)
}

type pageManager struct {
	dataLocation   string
	createFileFunc func(filePath string) error
	createDirFunc  func(directory string) error
	writeFileFunc  func(filePath string, fileData []byte) error
	getFileFunc    func(filePath string) ([]byte, error)
	deleteFileFunc func(filePath string) error
	uuidFunc       func() string
}

var pageManagerInstance *pageManager

func CreatePageManager(dataLocation string) PageManager {
	if pageManagerInstance == nil {
		pageManagerInstance = &pageManager{
			dataLocation:   dataLocation,
			createFileFunc: diskUtils.CreateFile,
			createDirFunc:  diskUtils.CreateDir,
			writeFileFunc:  diskUtils.WriteFile,
			getFileFunc:    diskUtils.GetFile,
			deleteFileFunc: diskUtils.DeleteFile,
			uuidFunc:       diskUtils.GetUUID,
		}
	}
	return pageManagerInstance
}

func (pdm *pageManager) Initialize(db string, blob string) error {
	pagesFilePath := pdm.getPagesFileName(db, blob)
	if err := pdm.createFileFunc(pagesFilePath); err != nil {
		return err
	}

	pagesData, _ := json.Marshal(diskModels.Pages{})
	if err := pdm.writeFileFunc(pagesFilePath, pagesData); err != nil {
		return err
	}

	return pdm.createDirFunc(pdm.getPagesDirectoryName(db, blob))
}

func (pdm *pageManager) Create(db string, blob string) (string, error) {
	newPageFile := fmt.Sprintf("%s.json", pdm.uuidFunc())
	newPageFilePath := fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), newPageFile)
	if err := pdm.createFileFunc(newPageFilePath); err != nil {
		return "", err
	}
	pageRecordsData, _ := json.Marshal(diskModels.PageRecords{})
	if err := pdm.writeFileFunc(newPageFilePath, pageRecordsData); err != nil {
		return newPageFile, err
	}

	pages, err := pdm.GetAll(db, blob)
	if err != nil {
		return newPageFile, err
	}
	pages = append(pages, diskModels.PageItem{FileName: newPageFile})
	pagesData, _ := json.Marshal(pages)
	err = pdm.writeFileFunc(pdm.getPagesFileName(db, blob), pagesData)
	return newPageFile, err
}

func (pdm *pageManager) GetAll(db string, blob string) (diskModels.Pages, error) {
	var pages diskModels.Pages
	pagesFilePath := pdm.getPagesFileName(db, blob)
	file, err := pdm.getFileFunc(pagesFilePath)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(file, &pages)
	return pages, err
}

func (pdm *pageManager) GetData(db string, blob string, pageFileName string) (diskModels.PageRecords, error) {
	var pageRecords diskModels.PageRecords
	file, err := pdm.getFileFunc(fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), pageFileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &pageRecords)
	return pageRecords, err
}

func (pdm *pageManager) WriteData(db string, blob string, pageFileName string, data diskModels.PageRecords) error {
	dataBytes, _ := json.Marshal(data)
	return pdm.writeFileFunc(fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), pageFileName), dataBytes)
}

func (pdm *pageManager) Delete(db string, blob string, pageFileName string) (bool, error) {
	pages, err := pdm.GetAll(db, blob)
	if err != nil {
		return false, err
	}
	for i := 0; i < len(pages); i++ {
		if pages[i].FileName == pageFileName {
			copy(pages[i:], pages[i+1:])
			pages[len(pages)-1] = diskModels.PageItem{}
			pages = pages[:len(pages)-1]
			pagesData, _ := json.Marshal(pages)
			err = pdm.writeFileFunc(pdm.getPagesFileName(db, blob), pagesData)
			if err != nil {
				return false, err
			}
			break
		}
	}
	err = pdm.deleteFileFunc(fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), pageFileName))
	return err != nil, err
}

func (pdm *pageManager) getPagesFileName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, pagesFile)
}

func (pdm *pageManager) getPagesDirectoryName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, pagesDirectory)
}
