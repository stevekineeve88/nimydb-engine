package systemConstants

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
