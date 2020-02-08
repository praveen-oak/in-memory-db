package core

import (
	"github.com/praveen-oak/db/model"
	"github.com/praveen-oak/db/utils"
)

type HashIndexAction struct {
	dataObject *model.DataObject
	indexName  string
	columnName string
}

func CreateHashIndexAction(dataObject *model.DataObject, indexName string, columnName string) *HashIndexAction {
	action := new(HashIndexAction)
	action.dataObject = dataObject
	action.columnName = columnName
	action.indexName = indexName
	return action
}

func (action HashIndexAction) RunAction() map[string][]int {
	hashMap := make(map[string][]int)
	colNumber := utils.GetColumnNumber(action.dataObject.Headers, action.columnName)
	if colNumber == -1 {
		return hashMap
	}

	var tempColVal string
	for index, row := range action.dataObject.ObjectList {
		tempColVal = row[colNumber]

		if _, ok := hashMap[tempColVal]; !ok {
			hashMap[tempColVal] = []int{}
		}

		hashMap[tempColVal] = append(hashMap[tempColVal], index)
	}

	return hashMap

}
