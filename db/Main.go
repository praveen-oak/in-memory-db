package main

import (
	"fmt"

	core "github.com/praveen-oak/db/core/actions"
	"github.com/praveen-oak/db/dbio"
	"github.com/praveen-oak/db/utils"
)

func main() {
	sales1 := dbio.ReadFile("/Users/praveenoak/Desktop/sales1")
	sales2 := dbio.ReadFile("/Users/praveenoak/Desktop/sales2")
	fmt.Println(sales1.Headers)
	fmt.Println(sales2.Headers)
	fmt.Println(sales2.ObjectList[5][1])

	filterCondition := core.CreateFilterCondition("saleid", "saleid", utils.EQUAL)

	filterConditions := make([]core.FilterCondition, 2)
	filterConditions[0] = *filterCondition
	filterConditions[1] = *core.CreateFilterCondition("saleid", "C", utils.EQUAL)
	joinAction := core.CreateJoinAction(sales1, sales2, filterConditions, core.AND, "temp")
	retObject := joinAction.RunAction()
	fmt.Println(len(retObject.ObjectList))

	aggAction := core.CreateAggAction(sales1, core.AVG, []string{"pricerange"}, "qty", "op")
	ret := aggAction.RunAction()
	fmt.Println(ret.ObjectList)

	filterConditions = make([]core.FilterCondition, 2)
	filterCondition = core.CreateFilterCondition("saleid", "1", utils.EQUAL)

	filterConditions[0] = *filterCondition
	filterConditions[1] = *core.CreateFilterCondition("pricerange", "outrageous", utils.EQUAL)
	selectAction := core.CreateSelectAction(sales1,
		filterConditions, core.AND, "temp")

	res := selectAction.RunAction()
	fmt.Println(res.ObjectList)

	indexAction := core.CreateHashIndexAction(sales1, "index", "itemid")
	index := indexAction.RunAction()

	for key, value := range index {
		fmt.Println(key, len(value))
	}
	// fmt.Println(index)
}
