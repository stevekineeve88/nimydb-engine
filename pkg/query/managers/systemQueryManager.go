package queryManagers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-engine/pkg/disk/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/constants"
	"github.com/stevekineeve88/nimydb-engine/pkg/query/models"
	"github.com/stevekineeve88/nimydb-engine/pkg/system/managers"
)

type systemQueryManager struct {
	logsManager systemManagers.LogManager
	userManager systemManagers.UserManager
}

func CreateSystemQueryManager(logManager systemManagers.LogManager, userManager systemManagers.UserManager) QueryManager {
	return &systemQueryManager{
		logsManager: logManager,
		userManager: userManager,
	}
}

func (sm *systemQueryManager) Query(query queryModels.Query) queryModels.QueryResult {
	switch query.Action {
	case queryConstants.ActionCreate:
		return sm.handleActionCreate(query)
	case queryConstants.ActionGet:
		return sm.handleActionGet(query)
	default:
		return queryModels.QueryResult{
			ErrorMessage: fmt.Sprintf("action %s does not exist", query.Action),
		}
	}
}

func (sm *systemQueryManager) handleActionGet(query queryModels.Query) queryModels.QueryResult {
	switch query.On {
	case queryConstants.OnLogs:
		errMessage := ""
		logs, err := sm.logsManager.GetLogs(query.With.Filter)
		if err != nil {
			errMessage = err.Error()
		}
		records := []diskModels.PageRecord{}
		for _, log := range logs {
			records = append(records, log.ConvertToPageRecord())
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
			Records:      records,
		}
	case queryConstants.OnUsers:
		errMessage := ""
		users, err := sm.userManager.GetUsers(query.With.Filter)
		if err != nil {
			errMessage = err.Error()
		}
		records := []diskModels.PageRecord{}
		for _, user := range users {
			records = append(records, user.ConvertToPageRecord())
		}
		return queryModels.QueryResult{
			ErrorMessage: errMessage,
			Records:      records,
		}
	default:
		return queryModels.QueryResult{
			ErrorMessage: fmt.Sprintf("%s not allowed on action %s", query.On, query.Action),
		}
	}
}

func (sm *systemQueryManager) handleActionCreate(query queryModels.Query) queryModels.QueryResult {
	switch query.On {
	case queryConstants.OnConnection:
		errMessage := ""
		user, err := sm.userManager.Authenticate(
			query.With.UserConnection.User,
			query.With.UserConnection.Password,
		)
		if err != nil {
			errMessage = err.Error()
		}
		return queryModels.QueryResult{
			ErrorMessage:   errMessage,
			ConnectionUser: user,
		}
	default:
		return queryModels.QueryResult{
			ErrorMessage: fmt.Sprintf("%s not allowed on action %s", query.On, query.Action),
		}
	}
}
