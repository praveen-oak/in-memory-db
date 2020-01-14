package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/praveen-oak/db/model"
	"github.com/praveen-oak/db/utils"
)

const (
	COUNT = iota
	AVG
	SUM
	MOVSUM
	MOVAVG
	COLUMN_JOINER = ","
)

type AggAction struct {
	queryType       int
	dataObject      *model.DataObject
	aggType         int
	groupColumns    []string
	aggColumn       string
	outputTableName string
}

func CreateAggAction(dataObject *model.DataObject, aggType int,
	groupColumns []string, aggColumn string, outputTableName string) *AggAction {

	aggAction := AggAction{}
	aggAction.dataObject = dataObject
	aggAction.aggType = aggType
	aggAction.groupColumns = groupColumns
	aggAction.aggColumn = aggColumn
	aggAction.outputTableName = outputTableName

	return &aggAction
}

func (action AggAction) RunAction() *model.DataObject {
	groupedRows := runGroupBy(action.groupColumns, action.dataObject.ObjectList, action.dataObject.Headers)

	var retObjList [][]string = make([][]string, len(groupedRows))
	var groupedTuples []string
	var aggValue float32
	index := 0
	for key, value := range groupedRows {
		groupedTuples = strings.Split(key, COLUMN_JOINER)
		aggValue = runAggFunc(value, action.aggColumn, action.dataObject.Headers, action.aggType)
		groupedTuples = append(groupedTuples, fmt.Sprintf("%f", aggValue))
		retObjList[index] = groupedTuples
		index++
	}

	returnObject := model.DataObject{}
	returnObject.Headers = append(action.groupColumns, action.aggColumn)
	returnObject.Rows = len(retObjList)
	returnObject.Columns = len(returnObject.Headers)
	returnObject.TableName = action.outputTableName
	returnObject.ObjectList = retObjList

	return &returnObject

}

func sum(objList [][]string, column string, headers []string) float32 {
	var start float32 = 0

	colIndex := utils.GetColumnNumber(headers, column)
	for _, row := range objList {
		fv, _ := strconv.ParseFloat(row[colIndex], 32)
		start += float32(fv)
	}
	return start
}

func runAggFunc(objList [][]string, column string, headers []string, aggType int) float32 {

	switch aggType {
	case AVG:
		return sum(objList, column, headers) / float32(len(objList))
	case SUM:
		return sum(objList, column, headers)
	case COUNT:
		return float32(len(objList))
	default:
		return 0
	}
}

func runGroupBy(groupColList []string, objList [][]string, headers []string) map[string][][]string {
	var groupByObject map[string][][]string
	var tempGroupByObject map[string][][]string
	var secondTempGroupByObject map[string][][]string
	for index, column := range groupColList {
		if index == 0 {
			groupByObject = group(objList, column, headers)
		} else {
			tempGroupByObject = make(map[string][][]string)

			for groupObjKey, groupObjValue := range groupByObject {
				secondTempGroupByObject = group(groupObjValue, column, headers)
				for secondGroupObjKey, secondTempObjValue := range secondTempGroupByObject {
					tempGroupByObject[groupObjKey+COLUMN_JOINER+secondGroupObjKey] = secondTempObjValue
				}
			}
			groupByObject = tempGroupByObject
		}
	}
	return groupByObject

}

func group(objectList [][]string, column string, headers []string) map[string][][]string {
	columnIndex := utils.GetColumnNumber(headers, column)

	groupMap := make(map[string][][]string)

	for _, row := range objectList {
		// fmt.Println(row[columnIndex])
		if _, ok := groupMap[row[columnIndex]]; !ok {
			groupMap[row[columnIndex]] = [][]string{}
		}
		groupMap[row[columnIndex]] = append(groupMap[row[columnIndex]], row)
	}
	return groupMap
}
