package core

import (
	"github.com/praveen-oak/db/model"
	"github.com/praveen-oak/db/utils"
)

type SelectAction struct {
	queryType        int
	dataObject       *model.DataObject
	outputTableName  string
	filterType       int
	filterConditions []FilterCondition
}

func CreateSelectAction(dataObject *model.DataObject,
	filterConditions []FilterCondition, filterType int, outputTableName string) *SelectAction {
	action := new(SelectAction)
	action.dataObject = dataObject
	action.filterConditions = filterConditions
	action.filterType = filterType
	action.queryType = Select
	action.outputTableName = outputTableName
	return action
}

func (action SelectAction) RunAction() *model.DataObject {
	var objList [][]string
	switch action.filterType {
	case AND:
		objList = selectAndAction(action)
	case OR:
		objList = selectOrAction(action)
	}

	returnObject := model.DataObject{}
	returnObject.Headers = action.dataObject.Headers
	returnObject.Rows = len(objList)
	returnObject.Columns = action.dataObject.Columns
	returnObject.TableName = action.outputTableName
	returnObject.ObjectList = objList
	return &returnObject
}

func selectAndAction(action SelectAction) [][]string {
	it_channel := make(chan map[int]bool)

	var finMap map[int]bool = make(map[int]bool)
	var tempMap map[int]bool
	go selectObjects(action.dataObject, action.filterConditions[0], it_channel)
	tempMap = <-it_channel
	for k, _ := range tempMap {
		finMap[k] = true
	}
	for index := 1; index < len(action.filterConditions); index++ {
		go selectObjects(action.dataObject, action.filterConditions[index], it_channel)
		tempMap = <-it_channel
		for k, _ := range finMap {
			if _, ok := tempMap[k]; ok {
				finMap[k] = true
			} else {
				delete(finMap, k)
			}
		}
	}

	retObjList := make([][]string, len(finMap))
	index := 0
	for k, _ := range finMap {
		retObjList[index] = action.dataObject.ObjectList[k]
		index++
	}
	return retObjList
}

func selectOrAction(action SelectAction) [][]string {
	it_channel := make(chan map[int]bool)

	// var map_list []map[int][]int
	for index := 0; index < len(action.filterConditions); index++ {
		go selectObjects(action.dataObject, action.filterConditions[index], it_channel)
	}
	var finMap map[int]bool = make(map[int]bool)
	var tempMap map[int]bool
	for index := 0; index < len(action.filterConditions); index++ {
		tempMap = <-it_channel
		for k, _ := range tempMap {
			if _, ok := finMap[k]; !ok {
				finMap[k] = true
			}
		}
	}
	retObjList := make([][]string, len(finMap))
	index := 0
	for k, _ := range finMap {
		retObjList[index] = action.dataObject.ObjectList[k]
		index++
	}
	return retObjList
}

func selectObjects(dataObj *model.DataObject,
	condition FilterCondition,
	ch chan<- map[int]bool) {

	colIndex := utils.GetColumnNumber(dataObj.Headers, condition.leftValue)

	retMap := make(map[int]bool)
	for index, value := range dataObj.ObjectList {
		if utils.CheckCondition(value[colIndex], condition.rightValue, condition.filterComparisionExpression) {
			retMap[index] = true
		}
	}
	ch <- retMap
}
