package diskManagers

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/utils"
	"sync"
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
	dataLocation string
}

var pageManagerInstance *pageManager

func CreatePageManager(dataLocation string) PageManager {
	sync.OnceFunc(func() {
		pageManagerInstance = &pageManager{dataLocation: dataLocation}
	})()
	return pageManagerInstance
}

func (pdm *pageManager) Initialize(db string, blob string) error {
	pagesFilePath := pdm.getPagesFileName(db, blob)
	if err := diskUtils.CreateFile(pagesFilePath); err != nil {
		return err
	}

	pages := diskModels.Pages{}
	pagesData, _ := json.Marshal(pages)
	if err := diskUtils.WriteFile(pagesFilePath, pagesData); err != nil {
		return nil
	}

	return diskUtils.CreateDir(pdm.getPagesDirectoryName(db, blob))
}

func (pdm *pageManager) Create(db string, blob string) (string, error) {
	newPageFile := fmt.Sprintf("%s.json", uuid.New().String())
	newPageFilePath := fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), newPageFile)
	if err := diskUtils.CreateFile(newPageFilePath); err != nil {
		return "", err
	}
	pageRecords := diskModels.PageRecords{}
	pageRecordsData, _ := json.Marshal(pageRecords)
	if err := diskUtils.WriteFile(newPageFilePath, pageRecordsData); err != nil {
		return newPageFile, err
	}

	pages, err := pdm.GetAll(db, blob)
	if err != nil {
		return newPageFile, err
	}
	pages = append(pages, diskModels.PageItem{FileName: newPageFile})
	pagesData, _ := json.Marshal(pages)
	err = diskUtils.WriteFile(pdm.getPagesFileName(db, blob), pagesData)
	return newPageFile, err
}

func (pdm *pageManager) GetAll(db string, blob string) (diskModels.Pages, error) {
	var pages diskModels.Pages
	pagesFilePath := pdm.getPagesFileName(db, blob)
	file, err := diskUtils.GetFile(pagesFilePath)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(file, &pages)
	return pages, err
}

func (pdm *pageManager) GetData(db string, blob string, pageFileName string) (diskModels.PageRecords, error) {
	var pageRecords diskModels.PageRecords
	file, err := diskUtils.GetFile(fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), pageFileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &pageRecords)
	return pageRecords, err
}

func (pdm *pageManager) WriteData(db string, blob string, pageFileName string, data diskModels.PageRecords) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return diskUtils.WriteFile(fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), pageFileName), dataBytes)
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
			err = diskUtils.WriteFile(pdm.getPagesFileName(db, blob), pagesData)
			if err != nil {
				return false, err
			}
			break
		}
	}
	err = diskUtils.DeleteFile(fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), pageFileName))
	return err != nil, err
}

func (pdm *pageManager) getPagesFileName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, pagesFile)
}

func (pdm *pageManager) getPagesDirectoryName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, pagesDirectory)
}
