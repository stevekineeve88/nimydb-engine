package systemConstants

import (
	"strings"
)

const (
	DBSys       = "sys"
	BlobSysLog  = "sys_log"
	BlobSysUser = "sys_user"

	PermissionRead      = "r"   //read permission (non-system)
	PermissionReadWrite = "rw"  //global permission (non-system)
	PermissionReadSuper = "*r"  //read permission (non-system and system)
	PermissionSuper     = "*rw" //global permission (non-system and system)
)

var permissionRankMap = map[string]int{
	PermissionRead:      0,
	PermissionReadWrite: 1,
	PermissionReadSuper: 2,
	PermissionSuper:     3,
}

var sysBlobs = []string{
	BlobSysLog,
	BlobSysUser,
}

func IsSystemName(name string) bool {
	items := strings.Split(name, ".")
	if len(items) > 2 {
		return false
	}
	if len(items) == 1 {
		return items[0] == DBSys
	}
	return items[0] == DBSys && _isSystemBlob(items[1])
}

func HasRead(permission string) bool {
	return _hasPermission(permission, PermissionRead)
}

func HasReadWrite(permission string) bool {
	return _hasPermission(permission, PermissionReadWrite)
}

func HasSuperRead(permission string) bool {
	return _hasPermission(permission, PermissionReadSuper)
}

func HasSuper(permission string) bool {
	return _hasPermission(permission, PermissionSuper)
}

func _hasPermission(permission string, comparePerm string) bool {
	if rank, ok := permissionRankMap[permission]; ok {
		return rank >= permissionRankMap[comparePerm]
	}
	return false
}

func _isSystemBlob(name string) bool {
	for _, sysBlob := range sysBlobs {
		if name == sysBlob {
			return true
		}
	}
	return false
}
