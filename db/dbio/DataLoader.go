package dbio

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/praveen-oak/db/model"

	"github.com/praveen-oak/db/utils"
)

func ReadFile(fileName string) *model.DataObject {
	fp, err := os.Open(fileName)
	utils.Check(err)

	defer fp.Close()

	scanner := bufio.NewScanner(fp)

	var dataObj *model.DataObject
	var columns int
	var rows int

	lineNumber := 0
	for scanner.Scan() {
		line := scanner.Text()
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
			panic("Error reading file")
		}

		switch {
		case lineNumber == 0:
			rows, err = strconv.Atoi(line)
			utils.Check(err)
			dataObj = new(model.DataObject)
			dataObj.Rows = rows

		case lineNumber == 1:
			tuples := strings.Split(line, "|")
			columns = len(tuples)
			dataObj.Headers = tuples
			dataObj.ObjectList = make([][]string, rows)
			dataObj.Columns = columns
		default:
			tuples := strings.Split(line, "|")
			if len(tuples) != columns {
				panic("Not enough columns on row number = " + string(lineNumber))
			}

			if lineNumber-2 > rows {
				panic("More rows present than mentioned in top of file")
			}
			dataObj.ObjectList[lineNumber-2] = tuples
		}
		lineNumber++
	}
	if lineNumber-2 != rows {
		panic("Fewer rows present than mentioned in top of file")
	}
	return dataObj
}
