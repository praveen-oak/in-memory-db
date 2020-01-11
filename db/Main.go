package main

import (
	"fmt"

	core "github.com/praveen-oak/db/core/actions"
	"github.com/praveen-oak/db/dbio"
)

func main() {
	sales1 := dbio.ReadFile("/Users/praveenoak/Desktop/sales1")
	sales2 := dbio.ReadFile("/Users/praveenoak/Desktop/sales2")
	fmt.Println(sales1.Headers)
	fmt.Println(sales2.Headers)
	fmt.Println(sales2.ObjectList[5][1])

	filterCondition := core.CreateFilterCondition("saleid", "saleid", core.EQUAL)

	filterConditions := make([]core.FilterCondition, 2)
	filterConditions[0] = *filterCondition
	filterConditions[1] = *core.CreateFilterCondition("saleid", "C", core.EQUAL)
	joinAction := core.CreateJoinAction(sales1, sales2, filterConditions, core.AND, "temp")
	retObject := joinAction.RunAction()
	fmt.Println(len(retObject.ObjectList))
}
