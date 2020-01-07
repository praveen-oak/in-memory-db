package main

import (
	"fmt"

	"github.com/praveen-oak/db/dbio"
)

func main() {
	sales1 := dbio.ReadFile("/Users/praveenoak/Desktop/sales1")
	sales2 := dbio.ReadFile("/Users/praveenoak/Desktop/sales2")
	fmt.Println(sales1.ObjectList[5][1])
	fmt.Println(sales2.ObjectList[5][1])
}
