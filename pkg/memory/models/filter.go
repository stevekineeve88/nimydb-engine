package memoryModels

import (
	"errors"
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/utils"
	"strings"
	"time"
)

type SearchPartition map[string]any

type FilterItem struct {
	Key   string `json:"key,required"`
	Op    string `json:"op,required"`
	Value any    `json:"value,required"`
}

type Filter struct {
	FilterItems []FilterItem
	Format      diskModels.Format
	converted   bool
}

type GetOperationParams struct {
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
	Sort   string `json:"sort"`
}

func (f *Filter) Passes(record diskModels.PageRecord) (bool, error) {
	if f.FilterItems == nil {
		return true, nil
	}
	for _, filterItem := range f.FilterItems {
		value, ok := record[filterItem.Key]
		if !ok {
			return false, errors.New(fmt.Sprintf("'%s' not found in record", filterItem.Key))
		}
		result := true
		switch f.Format[filterItem.Key].KeyType {
		case memoryConstants.String:
			_, ok = value.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("record is corrupt value %+v", value))
			}
			result = f.checkString(filterItem.Value.(string), value.(string), filterItem.Op)
		case memoryConstants.Int:
			value, err := memoryUtils.ConvertToInt(value)
			if err != nil {
				return false, errors.New(fmt.Sprintf("corrupt record with value %+v: %s", value, err.Error()))
			}
			result = f.checkInt(filterItem.Value.(int), value, filterItem.Op)
		case memoryConstants.Float:
			value, err := memoryUtils.ConvertToFloat64(value)
			if err != nil {
				return false, errors.New(fmt.Sprintf("corrupt record with value %+v: %s", value, err.Error()))
			}
			result = f.checkFloat(filterItem.Value.(float64), value, filterItem.Op)
		case memoryConstants.Date:
			_, ok = value.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("record is corrupt value %+v", value))
			}
			result = f.checkDate(filterItem.Value.(string), value.(string), filterItem.Op)
		case memoryConstants.DateTime:
			_, ok = value.(string)
			if !ok {
				return false, errors.New(fmt.Sprintf("record is corrupt value %+v", value))
			}
			compare, err := memoryUtils.ConvertToInt(filterItem.Value)
			if err != nil {
				return false, errors.New(fmt.Sprintf("could not convert %+v to int in filter", compare))
			}
			result = f.checkDateTime(filterItem.Value.(int64), value.(string), filterItem.Op)
		case memoryConstants.Bool:
			_, ok = value.(bool)
			if !ok {
				return false, errors.New(fmt.Sprintf("record is corrupt value %+v", value))
			}
			result = f.checkBool(filterItem.Value.(bool), value.(bool), filterItem.Op)
		default:
			return false, errors.New(fmt.Sprintf("format type %s not known in filter", f.Format[filterItem.Key].KeyType))
		}

		if !result {
			return false, nil
		}
	}
	return true, nil
}

func (f *Filter) ConvertFilterItems() error {
	i := 0
	for _, filterItem := range f.FilterItems {
		switch f.Format[filterItem.Key].KeyType {
		case memoryConstants.Date:
			fallthrough
		case memoryConstants.String:
			value, ok := filterItem.Value.(string)
			if !ok {
				return errors.New(fmt.Sprintf("%+v could not be converted to string", filterItem.Value))
			}
			f.FilterItems[i].Value = value
		case memoryConstants.Int:
			value, err := memoryUtils.ConvertToInt(filterItem.Value)
			if err != nil {
				return errors.New(fmt.Sprintf("could not convert %+v to int in filter", filterItem.Value))
			}
			f.FilterItems[i].Value = value
		case memoryConstants.Float:
			value, err := memoryUtils.ConvertToFloat64(filterItem.Value)
			if err != nil {
				return errors.New(fmt.Sprintf("could not convert %+v to int in filter", filterItem.Value))
			}
			f.FilterItems[i].Value = value
		case memoryConstants.DateTime:
			value, err := memoryUtils.ConvertToInt(filterItem.Value)
			if err != nil {
				return errors.New(fmt.Sprintf("could not convert %+v to int in filter", filterItem.Value))
			}
			f.FilterItems[i].Value = int64(value)
		case memoryConstants.Bool:
			value, ok := filterItem.Value.(bool)
			if !ok {
				return errors.New(fmt.Sprintf("%+v could not be converted to bool", filterItem.Value))
			}
			f.FilterItems[i].Value = value
		}
		i++
	}
	return nil
}

func (f *Filter) checkString(compare string, value string, op string) bool {
	switch op {
	case "CONTAINS_CS":
		return strings.Contains(value, compare)
	case "CONTAINS":
		return strings.Contains(strings.ToLower(value), strings.ToLower(compare))
	case "PREFIX_CS":
		return strings.HasPrefix(value, compare)
	case "PREFIX":
		return strings.HasPrefix(strings.ToLower(value), strings.ToLower(compare))
	case "SUFFIX_CS":
		return strings.HasSuffix(value, compare)
	case "SUFFIX":
		return strings.HasSuffix(strings.ToLower(value), strings.ToLower(compare))
	case "=":
		return value == compare
	default:
		return false
	}
}

func (f *Filter) checkInt(compare int, value int, op string) bool {
	switch op {
	case "=":
		return compare == value
	case ">":
		return value > compare
	case ">=":
		return value >= compare
	case "<":
		return value < compare
	case "<=":
		return value <= compare
	default:
		return false
	}
}

func (f *Filter) checkFloat(compare float64, value float64, op string) bool {
	switch op {
	case "=":
		return compare == value
	case ">":
		return value > compare
	case ">=":
		return value >= compare
	case "<":
		return value < compare
	case "<=":
		return value <= compare
	default:
		return false
	}
}

func (f *Filter) checkDate(compare string, value string, op string) bool {
	filterValueDate, err := time.Parse("2006-01-02", compare)
	if err != nil {
		return false
	}
	valueDate, err := time.Parse("2006-01-02", value)
	if err != nil {
		return false
	}
	switch op {
	case "=":
		return filterValueDate == valueDate
	case ">":
		return valueDate.After(filterValueDate)
	case ">=":
		return valueDate.After(filterValueDate) || filterValueDate == valueDate
	case "<":
		return valueDate.Before(filterValueDate)
	case "<=":
		return valueDate.Before(filterValueDate) || filterValueDate == valueDate
	default:
		return false
	}
}

func (f *Filter) checkDateTime(compare int64, value string, op string) bool {
	filterValueDateTime := time.Unix(compare, 0)
	valueDateTime, err := time.Parse(time.DateTime, value)
	if err != nil {
		return false
	}
	switch op {
	case "=":
		return filterValueDateTime == valueDateTime
	case ">":
		return valueDateTime.After(filterValueDateTime)
	case ">=":
		return valueDateTime.After(filterValueDateTime) || filterValueDateTime == valueDateTime
	case "<":
		return valueDateTime.Before(filterValueDateTime)
	case "<=":
		return valueDateTime.Before(filterValueDateTime) || filterValueDateTime == valueDateTime
	default:
		return false
	}
}

func (f *Filter) checkBool(compare bool, value bool, op string) bool {
	switch op {
	case "=":
		return compare == value
	default:
		return false
	}
}
