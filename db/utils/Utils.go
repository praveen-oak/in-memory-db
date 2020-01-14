package utils

const (
	EQUAL = iota
	GREATER_THAN
	LESSER_THAN
	LESS_THAN_EQUAL_TO
	GREATER_THAN_EQUAL_TO
	NOT_EQUAL
)

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func GetColumnListNumber(headerList []string, columnList []string) []int {
	var colNumList []int
	for _, columnName := range columnList {
		colNumList = append(colNumList, GetColumnNumber(headerList, columnName))
	}

	return colNumList
}

func GetColumnNumber(columnList []string, columnName string) int {

	for index, element := range columnList {
		if element == columnName {
			return index
		}
	}

	return -1
}

func CheckCondition(leftVal string, rightVal string, filterComparisionExpression int) bool {
	switch filterComparisionExpression {
	case EQUAL:
		return leftVal == rightVal
	case GREATER_THAN:
		return leftVal > rightVal
	case GREATER_THAN_EQUAL_TO:
		return leftVal >= rightVal
	case LESSER_THAN:
		return leftVal < rightVal
	case LESS_THAN_EQUAL_TO:
		return leftVal <= rightVal
	case NOT_EQUAL:
		return leftVal != rightVal
	default:
		return false
	}
}
