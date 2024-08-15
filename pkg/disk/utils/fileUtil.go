package diskUtils

import (
	"github.com/google/uuid"
	"os"
)

func CreateDir(directory string) error {
	return os.Mkdir(directory, 0600)
}

func DeleteDirectory(directory string) error {
	return os.RemoveAll(directory)
}

func GetDirectoryContents(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	contents := []string{}
	for _, e := range entries {
		contents = append(contents, e.Name())
	}
	return contents, nil
}

func CreateFile(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return nil
	}
	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()
	return nil
}

func WriteFile(filePath string, fileData []byte) error {
	return os.WriteFile(filePath, fileData, 0777)
}

func GetFile(filePath string) ([]byte, error) {
	return os.ReadFile(filePath)
}

func DeleteFile(filePath string) error {
	return os.Remove(filePath)
}

func GetUUID() string {
	return uuid.New().String()
}
