package memoryModels

import (
	"errors"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/utils"
	"regexp"
	"slices"
	"time"
)

type DBFormatter struct {
	Name string
}

func (dbf *DBFormatter) HasDBNameConvention() error {
	if len(dbf.Name) > memoryConstants.KeyMaxLength {
		return errors.New(fmt.Sprintf("Name length on %s exceeds %d", dbf.Name, memoryConstants.DBMaxLength))
	}
	match, _ := regexp.MatchString(memoryConstants.DBRegex, dbf.Name)
	if !match {
		return errors.New(fmt.Sprintf("Name %s does not match %s", dbf.Name, memoryConstants.DBRegexDesc))
	}
	return nil
}

type BlobFormatter struct {
	Name      string               `json:"name,required"`
	Format    diskModels.Format    `json:"format,required"`
	Partition diskModels.Partition `json:"partition,omitempty"`
}

func CreateFormatter(blob string, format diskModels.Format) BlobFormatter {
	return BlobFormatter{
		Name:      blob,
		Format:    format,
		Partition: diskModels.Partition{Keys: []string{}},
	}
}

func CreateFormatterWithPartition(blob string, format diskModels.Format, partition diskModels.Partition) BlobFormatter {
	return BlobFormatter{
		Name:      blob,
		Format:    format,
		Partition: partition,
	}
}

func (f *BlobFormatter) HasBlobNameConvention() error {
	if len(f.Name) > memoryConstants.BlobMaxLength {
		return errors.New(fmt.Sprintf("Name length on %s exceeds %d", f.Name, memoryConstants.BlobMaxLength))
	}
	match, _ := regexp.MatchString(memoryConstants.BlobRegex, f.Name)
	if !match {
		return errors.New(fmt.Sprintf("Name %s does not match %s", f.Name, memoryConstants.BlobRegexDesc))
	}
	return nil
}

func (f *BlobFormatter) HasFormatStructure() error {
	for key, formatItem := range f.Format {
		if len(key) > memoryConstants.KeyMaxLength {
			return errors.New(fmt.Sprintf("key length on %s exceeds %d", key, memoryConstants.KeyMaxLength))
		}
		match, _ := regexp.MatchString(memoryConstants.KeyRegex, key)
		if !match {
			return errors.New(fmt.Sprintf("key %s does not match %s", key, memoryConstants.KeyRegexDesc))
		}
		if err := f.checkFormatItem(key, formatItem); err != nil {
			return err
		}
	}
	return nil
}

func (f *BlobFormatter) HasPartitionStructure() error {
	for _, partitionKey := range f.Partition.Keys {
		_, ok := f.Format[partitionKey]
		if !ok {
			return errors.New(fmt.Sprintf("Partition key %s not found in Format", partitionKey))
		}
	}
	return nil
}

func (f *BlobFormatter) FormatRecord(pageRecord diskModels.PageRecord) (diskModels.PageRecord, error) {
	if len(f.Format) != len(pageRecord) {
		return nil, errors.New("record does not match Format length")
	}
	newRecord := make(map[string]any)
	for key, value := range pageRecord {
		formatItem, ok := f.Format[key]
		if !ok {
			return nil, errors.New(fmt.Sprintf("key %s does not exist in %s", key, f.Name))
		}
		newValue, err := f.convertRecordValue(value, formatItem)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("error on key %s: %s", key, err.Error()))
		}
		newRecord[key] = newValue
	}
	return newRecord, nil
}

func (f *BlobFormatter) FormatUpdateRecord(pageRecord diskModels.PageRecord) (diskModels.PageRecord, error) {
	newRecord := diskModels.PageRecord{}
	for key, value := range pageRecord {
		formatItem, ok := f.Format[key]
		if !ok {
			return nil, errors.New(fmt.Sprintf("key %s does not exist in %s", key, f.Name))
		}
		for _, partitionKey := range f.Partition.Keys {
			if key == partitionKey {
				return nil, errors.New(fmt.Sprintf("key %s cannot be updated because belongs to partition", key))
			}
		}
		newValue, err := f.convertRecordValue(value, formatItem)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("error on key %s: %s", key, err.Error()))
		}
		newRecord[key] = newValue
	}
	return newRecord, nil
}

func (f *BlobFormatter) checkFormatItem(key string, formatItem diskModels.FormatItem) error {
	if !slices.Contains(memoryConstants.GetFormatTypes(), formatItem.KeyType) {
		return errors.New(fmt.Sprintf("key type %s does not exist on key %s", formatItem.KeyType, key))
	}
	return nil
}

func (f *BlobFormatter) convertRecordValue(value any, formatItem diskModels.FormatItem) (any, error) {
	switch formatItem.KeyType {
	case memoryConstants.String:
		converted, ok := value.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("%+v could not be converted to string", value))
		}
		return converted, nil
	case memoryConstants.Int:
		return memoryUtils.ConvertToInt(value)
	case memoryConstants.Float:
		return memoryUtils.ConvertToFloat64(value)
	case memoryConstants.Bool:
		converted, ok := value.(bool)
		if !ok {
			return nil, errors.New(fmt.Sprintf("%+v could not convert to bool", value))
		}
		return converted, nil
	case memoryConstants.Date:
		fallthrough
	case memoryConstants.DateTime:
		timeValueInt, err := memoryUtils.ConvertToInt(value)
		if err != nil {
			return nil, err
		}
		timeValue := time.Unix(int64(timeValueInt), 0)
		if formatItem.KeyType == memoryConstants.Date {
			return timeValue.Format(time.DateOnly), nil
		} else {
			return timeValue.Format(time.DateTime), nil
		}
	}
	return nil, errors.New("type not handled")
}
