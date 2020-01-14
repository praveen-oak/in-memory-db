package core

import (
	"github.com/praveen-oak/db/model"
)

//constants
const (
	Select     = 1
	Project    = 2
	Countgroup = 3
	Sumgroup   = 4
	Avggroup   = 5
	Join       = 6
	Sort       = 7
	Sum        = 8
	Count      = 9
	Avg        = 10
	Movavg     = 11
	Movsum     = 12
	Concat     = 13
)

//Action type
type Action interface {
	GetQuerytype() string
	RunAction(dataObject model.DataObject) model.DataObject
}
