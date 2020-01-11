package core

import (
	"github.com/praveen-oak/db/model"
	"github.com/praveen-oak/db/utils"
)

type ProjectAction struct {
	queryType       string
	columnList      []string
	inputTableName  string
	outputTableName string
	dataObject      *model.DataObject
}

func CreateProjectAction(dataObject *model.DataObject, columnList []string,
	inputTableName string, outputTableName string) *ProjectAction {

	action := new(ProjectAction)
	action.dataObject = dataObject
	action.queryType = "project"
	action.inputTableName = inputTableName
	action.outputTableName = outputTableName
	action.columnList = columnList
	return action
}

func (action ProjectAction) GetQuerytype() string {
	return action.queryType
}

func (action ProjectAction) RunAction() model.DataObject {
	colNumList := utils.GetColumnListNumber(action.dataObject.Headers, action.columnList)

	retColumns := len(colNumList)
	var returnObjList [][]string = make([][]string, action.dataObject.Rows)

	for outer_index, object := range action.dataObject.ObjectList {
		var projObj []string = make([]string, retColumns)

		for index, val := range colNumList {
			projObj[index] = object[val]
		}
		returnObjList[outer_index] = projObj
	}

	return model.DataObject{
		Headers:    action.columnList,
		TableName:  action.outputTableName,
		ObjectList: returnObjList,
		Indexes:    action.dataObject.Indexes,
		Rows:       action.dataObject.Rows,
		Columns:    retColumns,
	}
}
