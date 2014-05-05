package whanau

func IsInList(val string, array []string) bool {
	for _, v := range array {
		if v == val {
			return true
		}
	}

	return false
}
