package core

import (
	"github.com/praveen-oak/db/model"
	"github.com/praveen-oak/db/utils"
)

const (
	AND = iota
	OR
	NOT
)

type JoinAction struct {
	queryType        int
	leftDataObject   *model.DataObject
	rightDataObject  *model.DataObject
	outputTableName  string
	filterType       int
	filterConditions []FilterCondition
}

type FilterCondition struct {
	leftValue                   string
	rightValue                  string
	filterComparisionExpression int
}

func CreateFilterCondition(leftValue string, rightValue string, filterComparisionExpression int) *FilterCondition {
	condition := new(FilterCondition)
	condition.leftValue = leftValue
	condition.rightValue = rightValue
	condition.filterComparisionExpression = filterComparisionExpression
	return condition
}

func CreateJoinAction(leftDataObject *model.DataObject, rightDataObject *model.DataObject,
	filterConditions []FilterCondition, filterType int, outputTableName string) *JoinAction {
	action := new(JoinAction)
	action.leftDataObject = leftDataObject
	action.rightDataObject = rightDataObject
	action.filterConditions = filterConditions
	action.filterType = filterType
	action.queryType = Join
	action.outputTableName = outputTableName
	return action
}

func (action JoinAction) RunAction() *model.DataObject {

	var retMap map[int][]int
	switch action.filterType {
	case AND:
		retMap = runAndAction(action)
	case OR:
		retMap = runOrAction(action)
	}
	return concatObjects(action, retMap)
}

func runAndAction(action JoinAction) map[int][]int {
	it_channel := make(chan map[int][]int)
	go runAllObjects(action.leftDataObject, action.rightDataObject, action.filterConditions[0], it_channel)
	if len(action.filterConditions) == 1 {
		return <-it_channel
	}
	for index := 1; index < len(action.filterConditions); index++ {
		go runSelectObjects(action.leftDataObject, action.rightDataObject, action.filterConditions[index], <-it_channel, it_channel)
	}
	return <-it_channel
}

func runOrAction(action JoinAction) map[int][]int {
	it_channel := make(chan map[int][]int)

	// var map_list []map[int][]int
	for index := 0; index < len(action.filterConditions); index++ {
		go runAllObjects(action.leftDataObject, action.rightDataObject, action.filterConditions[index], it_channel)
	}
	var fin_map map[int][]int = make(map[int][]int)
	var temp_map map[int][]int
	for index := 0; index < len(action.filterConditions); index++ {
		temp_map = <-it_channel
		for k, v := range temp_map {
			if _, ok := fin_map[k]; !ok {
				fin_map[k] = []int{}
			}
			fin_map[k] = append(fin_map[k], v...)
		}
	}
	return fin_map
}

func concatObjects(action JoinAction, it_map map[int][]int) *model.DataObject {

	returnObject := model.DataObject{}
	returnObject.Headers = append(action.leftDataObject.Headers, action.rightDataObject.Headers...)
	returnObject.Rows = getSize(it_map)
	returnObject.Columns = action.leftDataObject.Columns + action.rightDataObject.Columns
	returnObject.TableName = action.outputTableName
	returnObject.ObjectList = make([][]string, returnObject.Rows)

	retIndex := 0
	for li, rlist := range it_map {
		for _, ri := range rlist {
			tempObj := make([]string, returnObject.Columns)
			tempObj = append(tempObj, action.leftDataObject.ObjectList[li]...)
			tempObj = append(tempObj, action.rightDataObject.ObjectList[ri]...)
			returnObject.ObjectList[retIndex] = tempObj
			retIndex++
		}
	}

	return &returnObject
}

func getSize(it_map map[int][]int) int {
	size := 0
	for _, v := range it_map {
		size += len(v)
	}
	return size
}

func runAllObjects(leftObj *model.DataObject,
	rightObj *model.DataObject,
	condition FilterCondition,
	ch chan<- map[int][]int) {

	leftIndex := utils.GetColumnNumber(leftObj.Headers, condition.leftValue)
	rightIndex := utils.GetColumnNumber(rightObj.Headers, condition.rightValue)

	ret_map := make(map[int][]int)
	for li, _ := range leftObj.ObjectList {
		for ri, _ := range rightObj.ObjectList {
			if utils.CheckCondition(leftObj.ObjectList[li][leftIndex], rightObj.ObjectList[ri][rightIndex], condition.filterComparisionExpression) {
				if _, ok := ret_map[li]; ok {
					ret_map[li] = []int{}
				}
				ret_map[li] = append(ret_map[li], ri)
			}
		}
	}
	ch <- ret_map
}
func runSelectObjects(leftObj *model.DataObject,
	rightObj *model.DataObject,
	condition FilterCondition,
	it_map map[int][]int,
	ch chan<- map[int][]int) {
	leftIndex := utils.GetColumnNumber(leftObj.Headers, condition.leftValue)
	rightIndex := utils.GetColumnNumber(rightObj.Headers, condition.rightValue)
	ret_map := make(map[int][]int)

	for li, rlist := range it_map {
		for _, ri := range rlist {
			if utils.CheckCondition(leftObj.ObjectList[li][leftIndex], rightObj.ObjectList[ri][rightIndex], condition.filterComparisionExpression) {
				if _, ok := ret_map[li]; !ok {
					ret_map[li] = []int{}
				}
				ret_map[li] = append(ret_map[li], ri)
			}
		}
	}
	ch <- ret_map
}
