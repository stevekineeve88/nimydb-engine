package systemModels

import (
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
)

type UserConnection struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

type User struct {
	Id         string `json:"id,omitempty"`
	User       string `json:"user,omitempty"`
	Permission string `json:"permission,omitempty"`
	Password   string `json:"-"`
}

func (u *User) ConvertToPageRecord() diskModels.PageRecord {
	return diskModels.PageRecord{
		memoryConstants.IdKey: u.Id,
		"user":                u.User,
		"permission":          u.Permission,
	}
}
