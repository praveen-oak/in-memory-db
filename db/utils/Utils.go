package utils

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
