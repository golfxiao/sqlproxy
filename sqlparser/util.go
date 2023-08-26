package sqlparser

func IsDualTable(node SQLNode) bool {
	isDual := false
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case TableIdent:
			if node.(TableIdent).v == "dual" {
				isDual = true
			}
			return false, nil
		default:
			return true, nil
		}
	}
	Walk(visit, node)
	return isDual
}

func BuildColumn(node SQLNode) (fieldName string, aliasName string) {
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case *ColName:
			fieldName = node.(*ColName).Name.val
			return false, nil
		case ColIdent:
			aliasName = node.(ColIdent).String()
			return false, nil
		default:
			return true, nil
		}
	}
	Walk(visit, node)
	return
}
