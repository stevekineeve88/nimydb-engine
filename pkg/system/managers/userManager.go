package systemManagers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/managers"
	"github.com/stevekineeve88/nimydb-engine/pkg/memory/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/models"
)

type UserManager interface {
	InitRoot(password string) systemModels.User
	Authenticate(user string, password string) (systemModels.User, error)
	GetUsers(filter []memoryModels.FilterItem) ([]systemModels.User, error)
}

type userManager struct {
	operationManager memoryManagers.OperationManager
}

func CreateUserManager(operationManager memoryManagers.OperationManager) UserManager {
	return &userManager{
		operationManager: operationManager,
	}
}

func (um *userManager) InitRoot(password string) systemModels.User {
	users, err := um.GetUsers([]memoryModels.FilterItem{
		{
			Key:   "user",
			Op:    "=",
			Value: "root",
		},
	})
	if err != nil {
		panic(err)
	}
	if len(users) == 1 {
		return users[0]
	}
	records, err := um.operationManager.AddRecords(systemConstants.DBSys, systemConstants.BlobSysUser, []diskModels.PageRecord{
		{
			"user":       "root",
			"password":   password,
			"permission": systemConstants.PermissionSuper,
		},
	})
	if err != nil {
		panic(err)
	}
	return systemModels.User{
		Id:         records[0][memoryConstants.IdKey].(string),
		User:       "root",
		Permission: systemConstants.PermissionSuper,
		Password:   records[0]["password"].(string),
	}
}

func (um *userManager) Authenticate(user string, password string) (systemModels.User, error) {
	users, err := um.GetUsers([]memoryModels.FilterItem{
		{
			Key:   "user",
			Op:    "=",
			Value: user,
		},
	})
	if err != nil {
		return systemModels.User{}, err
	}
	if len(users) == 0 {
		return systemModels.User{}, fmt.Errorf("%s not found in %s", user, systemConstants.BlobSysUser)
	}
	if password != users[0].Password {
		return systemModels.User{}, fmt.Errorf("authentication failed on user %s", user)
	}
	return users[0], nil
}

func (um *userManager) GetUsers(filter []memoryModels.FilterItem) ([]systemModels.User, error) {
	records, err := um.operationManager.GetRecords(
		systemConstants.DBSys,
		systemConstants.BlobSysUser,
		filter,
		memoryModels.SearchPartition{},
		memoryModels.GetOperationParams{},
	)
	if err != nil {
		return nil, err
	}
	users := []systemModels.User{}
	for _, record := range records {
		users = append(users, systemModels.User{
			Id:         record[memoryConstants.IdKey].(string),
			User:       record["user"].(string),
			Permission: record["permission"].(string),
			Password:   record["password"].(string),
		})
	}
	return users, nil
}
