package model

type DataObject struct {
	TableName  string
	Headers    []string
	ObjectList [][]string
	Indexes    []IndexObject
	Rows       int
	Columns    int
}

type IndexObject struct {
	IndexName  string
	ColumnName string
	HashIndex  map[string][]int
}
