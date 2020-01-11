package core

import (
	"github.com/praveen-oak/db/model"
)

type Runner interface {
	RunAction(string) model.DataObject
}
