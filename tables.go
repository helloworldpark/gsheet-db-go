package gosheet

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"google.golang.org/api/sheets/v4"
)

/*
 * Table api
 */

// CreateTable Creates a new sheet(a.k.a. table) with `tableName` on the given Spreadsheet(a.k.a. database).
// Case handling:
// database == nil: return nil
// database != nil: log as creating, and return the created sheet with true
func (db *Database) CreateTable(scheme interface{}, constraint ...*Constraint) *Table {
	// 1. Create new
	if db.Spreadsheet() == nil {
		// todo: log
		return nil
	}
	tableName := reflect.TypeOf(scheme).Name()
	for _, sheet := range db.spreadsheet.Sheets {
		if sheet.Properties.Title == tableName {
			panic(fmt.Sprintf("Has table with name %s", tableName))
		}
	}
	// add sheet
	request := make([]*sheets.Request, 1)
	request[0] = &sheets.Request{}
	request[0].AddSheet = &sheets.AddSheetRequest{}
	request[0].AddSheet.Properties = &sheets.SheetProperties{}
	request[0].AddSheet.Properties.Title = tableName
	newDatabase, _, statusCode := db.batchUpdate(request)
	if statusCode/100 != 2 {
		return nil
	}
	var newSheet *sheets.Sheet
	for _, sheet := range newDatabase.Sheets() {
		if sheet.Properties.Title == tableName {
			newSheet = sheet
			break
		}
	}
	// Apply requests
	requests := db.manager.createColumnsFromStruct(newSheet, scheme, constraint...)
	updated, _, statusCode := db.batchUpdate(requests)
	if updated == nil {
		return nil
	}
	for _, updatedSheet := range updated.Sheets() {
		if updatedSheet.Properties.SheetId == newSheet.Properties.SheetId {
			newTable := db.NewTableFromSheet(updatedSheet)
			return newTable
		}
	}
	return nil
}

// FindTable Gets an existing sheet(a.k.a. table) with `tableName` on the given Spreadsheet(a.k.a. database)
// If exists, returns the existed one
// If not existing, returns nil
func (db *Database) FindTable(str interface{}) *Table {
	tableName := reflect.TypeOf(str).Name()
	tables := db.ListTables()
	for _, table := range tables {
		if table.Name() == tableName {
			return table
		}
	}
	return nil
}

// ListTables Gets an existing sheets(a.k.a. table) in the given Spreadsheet(a.k.a. database).
// If exists, returns the existings
// If not existing, returns nil
func (db *Database) ListTables() []*Table {
	db.Manager().SynchronizeFromGoogle(db)
	sheets := db.Sheets()
	tables := make([]*Table, 0)
	for i := range sheets {
		if !db.isValidTable(sheets[i]) {
			continue
		}
		newTable := db.NewTableFromSheet(sheets[i])
		if newTable != nil {
			tables = append(tables, newTable)
		}
	}
	return tables
}

func (db *Database) batchUpdate(requests []*sheets.Request) (*Database, []*sheets.Response, int) {
	spreadsheet, responses, statusCode := newSpreadsheetBatchUpdateRequest(db.manager, db.Spreadsheet().SpreadsheetId, requests...).Do()
	updated := &Database{
		manager:     db.manager,
		spreadsheet: spreadsheet,
	}
	return updated, responses, statusCode
}

// Table Wrapper of the table(spreadsheet.sheet)
type Table struct {
	manager  *SheetManager
	database *Database
	sheet    *sheets.Sheet
	scheme   *TableScheme
	index    *tableIndex
}

// TableScheme Metadata of the table
type TableScheme struct {
	Name        string
	Columns     []string
	ColumnMap   map[string]int64
	Types       []reflect.Kind
	Rows        int64
	Constraints *Constraint
}

// Predicate Check if the given interface fits the condition
type Predicate func(interface{}) bool

// ArrayPredicate Check if the given interface fits the condition
type ArrayPredicate func([]interface{}) bool

// Drop Drops an existing sheet(a.k.a. table) with `tableName` on the given Spreadsheet(a.k.a. database)
// If exists, deletes and returns true
// If not existing, logs and returns false
// Always update
func (table *Table) Drop() bool {
	request := make([]*sheets.Request, 1)
	request[0] = &sheets.Request{}
	request[0].DeleteSheet = &sheets.DeleteSheetRequest{}
	request[0].DeleteSheet.SheetId = table.sheetID()
	resp, _, _ := table.database.batchUpdate(request)
	table.manager.SynchronizeFromGoogle(table.database)
	return resp != nil
}

// Select Selects all the rows from the table
func (table *Table) Select(rows int64) ([][]interface{}, *TableScheme) {
	// sync
	metadata := table.header()
	if metadata == nil {
		fmt.Println("Select: Metadata is nil")
		return nil, nil
	}
	if metadata.Rows == 0 {
		fmt.Println("Select: Metadata.rows is 0")
		return nil, metadata
	}
	if rows == 0 {
		fmt.Println("Select: rows is 0")
		return nil, metadata
	} else if rows == -1 {
		rows = metadata.Rows
	}

	// 3행~, 모든 열을 읽는다
	req := newSpreadsheetValuesRequest(table.manager, table.spreadsheet().SpreadsheetId, metadata.Name)
	req.updateRange(metadata.Name, 3, 0, 3+rows, int64(len(metadata.Columns)))
	valueRange := req.Do()

	return valueRange.Values, metadata
}

// SelectAndFilter Select rows satisfying filter
// filters.key: int, column index
// filters.value: Predicate, whether to select or not
func (table *Table) SelectAndFilter(filters map[int]Predicate) ([][]interface{}, *TableScheme) {
	fullData, _ := table.Select(-1)
	metadata := table.header()
	if len(filters) == 0 {
		return fullData, metadata
	}
	if len(fullData) == 0 {
		return fullData, metadata
	}

	filtered := make([][]interface{}, 0)
	for i := range fullData {
		appropriate := true
		for j := range fullData[i] {
			if predicate, ok := filters[j]; ok {
				appropriate = appropriate && predicate(fullData[i][j])
			}
		}
		if appropriate {
			filtered = append(filtered, fullData[i])
		}
	}
	return filtered, metadata
}

// UpsertIf Upserts given `values`. Returns true if success.
// condition.key: column index
func (table *Table) UpsertIf(values []interface{}, appendData bool, conditions ...map[int]Predicate) bool {
	if len(values) == 0 {
		return false
	}

	defer func() {
		// sync
		table.updatedHeader()
		table.updateIndex()
	}()

	scheme := table.header()
	if scheme == nil {
		return false
	}
	if !scheme.fitsScheme(values[0]) {
		fmt.Println("Input does not match table's scheme")
		return false
	}

	newValues := make([][]interface{}, 0)
	for i := range values {
		columnValues, ok := values[i].([]interface{})
		if !ok {
			columnwiseAnalyse := analyseStruct(values[i])
			for _, v := range columnwiseAnalyse {
				columnValues = append(columnValues, v.cvalue)
			}
		}
		// constraint check
		hasIndex, _ := table.hasIndexOf(columnValues)
		if !hasIndex {
			newValues = append(newValues, columnValues)
		}
	}

	var filteredValues [][]interface{}
	if len(conditions) == 0 {
		filteredValues = newValues
	} else {
		for i := range newValues {
			didPass := true
		TESTITEM:
			for _, condition := range conditions {
				for j, f := range condition {
					if !f(newValues[i][j]) {
						didPass = false
						break TESTITEM
					}
				}
			}
			if didPass {
				filteredValues = append(filteredValues, newValues[i])
			}
		}
	}
	if len(filteredValues) > 0 {
		// 데이터를 덧붙인다면 마지막 행부터
		// 처음부터라면 첫 행부터
		// 기록한 행 업데이트도 같이 한다
		req := newSpreadsheetValuesBatchUpdateRequest(table.manager, table.spreadsheet().SpreadsheetId, scheme.Name)
		req.updateRange(scheme, appendData, newValues)
		req.updateRows(scheme, len(newValues))
		if req.Do()/100 != 2 {
			return false
		}
	}

	return true
}

// Delete Deletes and returns deleted rows
// deleteThis: input - array of row values
// returns: array of rows starting from 0
func (table *Table) Delete(deleteThis ArrayPredicate) []int64 {
	defer func() {
		// sync
		table.updatedHeader()
		table.updateIndex()
	}()
	// call data
	data, metadata := table.Select(-1)
	if data == nil {
		return nil
	}

	// delete if predicate==true
	newData := make([]interface{}, 0)
	deletedIndex := make([]int64, 0)
	for i, values := range data {
		if deleteThis(values) {
			// add to deleted index
			deletedIndex = append(deletedIndex, int64(i))
		} else {
			newData = append(newData, values)
		}
	}

	if len(deletedIndex) == 0 {
		return nil
	}

	// update deleted data
	table.UpsertIf(newData, false)

	// reorder data: batch clear
	startRow := int64(3 + len(newData))
	startCol := int64(0)
	endRow := int64(3 + len(data))
	endCol := int64(len(metadata.Columns))
	ranges := newCellRange(metadata.Name, startRow, startCol, endRow, endCol)
	clearRequest := newClearValuesRequest(table.manager, table.spreadsheet().SpreadsheetId, ranges)
	if clearRequest.Do()/100 != 2 {
		panic("Cannot clear old data")
	}

	// subtract row counts
	syncRowCount := newSpreadsheetValuesBatchUpdateRequest(table.manager, table.spreadsheet().SpreadsheetId, metadata.Name)
	syncRowCount.updateRows(metadata, -len(deletedIndex))
	if syncRowCount.Do()/100 != 2 {
		panic("Cannot update row")
	}

	// return index
	return deletedIndex
}

// Name name of the table
func (table *Table) Name() string {
	return table.sheet.Properties.Title
}

// sheetID alias of sheet id
func (table *Table) sheetID() int64 {
	return table.sheet.Properties.SheetId
}

// spreadsheet spreadsheet holding this table
func (table *Table) spreadsheet() *sheets.Spreadsheet {
	return table.database.Spreadsheet()
}

// header Metadata of the table
func (table *Table) header() *TableScheme {
	return table.scheme
}

// updatedHeader Reads table's metadata from the server and sync
func (table *Table) updatedHeader() *TableScheme {
	// sync
	table.manager.SynchronizeFromGoogle(table.database)
	tableName := table.Name()
	tableCols := int64(len(table.header().Columns))

	// 0행~2행, 모든 열을 읽는다
	// 0행
	req := newSpreadsheetValuesRequest(table.manager, table.spreadsheet().SpreadsheetId, tableName)
	req.updateRange(tableName, 0, 0, 3, tableCols)
	valueRange := req.Do()

	colnames := make([]string, len(valueRange.Values[0]))
	for i := range colnames {
		colnames[i] = valueRange.Values[0][i].(string)
	}
	// 1행
	types := make([]reflect.Kind, len(valueRange.Values[1]))
	for i := range colnames {
		kindString := valueRange.Values[1][i].(string)
		types[i] = primitiveStringToKind[kindString]
	}
	// 2행
	rowsString, ok := valueRange.Values[2][0].(string)
	var rows int64
	if ok {
		rows, _ = strconv.ParseInt(rowsString, 10, 64)
	}

	var constraint = ""
	if len(valueRange.Values[2]) >= 3 {
		constraint = valueRange.Values[2][2].(string)
	}
	metadata := &TableScheme{
		Name:        tableName,
		Columns:     colnames,
		Types:       types,
		Rows:        rows,
		Constraints: newConstraintFromString(constraint),
	}
	table.scheme = metadata
	return metadata
}

// value: a struct splitted with columns
func (table *Table) hasIndexOf(value []interface{}) (bool, []int64) {
	if table.scheme.Constraints == nil {
		return false, nil
	}

	// unique constraint
	columns := table.scheme.Constraints.uniqueColumns
	columnIndex := table.scheme.columnsToIndices(columns)
	return table.index.hasIndex(value, columnIndex...)
}

func (m *SheetManager) createColumnsFromStruct(table *sheets.Sheet, structInstance interface{}, constraint ...*Constraint) []*sheets.Request {
	if table == nil {
		return nil
	}
	fields := analyseStruct(structInstance)
	if fields == nil {
		return nil
	}

	requests := make([]*sheets.Request, 1)
	requests[0] = &sheets.Request{}
	requests[0].UpdateCells = &sheets.UpdateCellsRequest{}
	requests[0].UpdateCells.Fields = "*"
	requests[0].UpdateCells.Range = &sheets.GridRange{}
	requests[0].UpdateCells.Range.SheetId = table.Properties.SheetId
	requests[0].UpdateCells.Range.EndRowIndex = 3

	data := make([]*sheets.RowData, 3)
	// Row 0: Column names
	data[0] = &sheets.RowData{}
	data[0].Values = make([]*sheets.CellData, len(fields))
	for i := range data[0].Values {
		field := fields[i]
		if _, ok := primitiveKindToString[field.ckind]; !ok {
			// todo: log not a primitive field
			return nil
		}
		data[0].Values[i] = &sheets.CellData{}
		data[0].Values[i].UserEnteredValue = &sheets.ExtendedValue{}
		data[0].Values[i].UserEnteredValue.StringValue = field.cname
	}

	// Row 1: Column datatype
	data[1] = &sheets.RowData{}
	data[1].Values = make([]*sheets.CellData, len(fields))
	for i := range data[1].Values {
		data[1].Values[i] = &sheets.CellData{}
		data[1].Values[i].UserEnteredValue = &sheets.ExtendedValue{}
		data[1].Values[i].UserEnteredValue.StringValue = fields[i].ctype
	}

	// Row 2, Col 0: How many data(numrows)
	// Row 2, Col 1: How many columns(numcols)
	// Row 2, Col 2: Constraints(optional)
	data[2] = &sheets.RowData{}
	data[2].Values = make([]*sheets.CellData, 2)
	data[2].Values[0] = &sheets.CellData{}
	data[2].Values[1] = &sheets.CellData{}
	if len(constraint) > 0 {
		data[2].Values = append(data[2].Values, &sheets.CellData{})
		data[2].Values[2] = &sheets.CellData{}
	}
	data[2].Values[0].UserEnteredValue = &sheets.ExtendedValue{}
	data[2].Values[0].UserEnteredValue.StringValue = "0"
	data[2].Values[1].UserEnteredValue = &sheets.ExtendedValue{}
	data[2].Values[1].UserEnteredValue.NumberValue = float64(len(fields))
	if len(constraint) > 0 {
		data[2].Values[2].UserEnteredValue = &sheets.ExtendedValue{}
		constraintBytes, err := json.Marshal(constraint[0].toMap())
		if err != nil {
			panic(err)
		}
		data[2].Values[2].UserEnteredValue.StringValue = string(constraintBytes)
	}

	requests[0].UpdateCells.Rows = data
	return requests
}

func (table *Table) updateIndex() {
	// if no constraint, no index update
	if table.header().Constraints == nil {
		fmt.Println("No constraint to create or maintain index")
		return
	}
	// if no index, create
	if table.index == nil {
		table.index = newTableIndex()
	}

	// call data
	data, scheme := table.Select(-1)
	if scheme == nil {
		panic("Metadata must not be nil when updating indices")
	}

	// build index
	table.index.build(data, scheme)
}

func (metadata *TableScheme) columnsToIndices(columns []string) []int64 {
	if metadata.ColumnMap == nil {
		tmp := make(map[string]int64)
		for i, c := range metadata.Columns {
			tmp[c] = int64(i)
		}
		metadata.ColumnMap = tmp
	}
	result := make([]int64, len(columns))
	for i, c := range columns {
		result[i] = metadata.ColumnMap[c]
	}
	return result
}

func (metadata *TableScheme) fitsScheme(value interface{}) bool {
	refl := reflect.ValueOf(value)
	switch refl.Kind() {
	case reflect.Slice:
		if len(metadata.Types) != refl.Len() {
			fmt.Printf("Invalid input: column count mismatching(table: %d, struct: %d)\n", len(metadata.Types), refl.Len())
			return false
		}
		// real type 찾는 거 너무 힘드니 다음에 구현한다
	case reflect.Struct:
		if len(metadata.Types) != refl.NumField() {
			return false
		}
		for i := 0; i < refl.NumField(); i++ {
			if refl.Field(i).Kind() != metadata.Types[i] {
				return false
			}
		}
	default:
		panic("Cannot handle this case")
	}
	return true
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// colnames := metadata.Columns
// 	types := metadata.Types
// 	createStruct := func() dstruct.DynamicStruct {
// 		instance := dstruct.NewStruct()
// 		for i := 0; i < len(colnames); i++ {
// 			switch types[i] {
// 			case reflect.Bool:
// 				instance.AddField(colnames[i], false, "")
// 			case reflect.Int:
// 				instance.AddField(colnames[i], int(0), "")
// 			case reflect.Int8:
// 				instance.AddField(colnames[i], int8(0), "")
// 			case reflect.Int16:
// 				instance.AddField(colnames[i], int16(0), "")
// 			case reflect.Int32:
// 				instance.AddField(colnames[i], int32(0), "")
// 			case reflect.Int64:
// 				instance.AddField(colnames[i], int64(0), "")
// 			case reflect.Uint:
// 				instance.AddField(colnames[i], uint(0), "")
// 			case reflect.Uint8:
// 				instance.AddField(colnames[i], uint8(0), "")
// 			case reflect.Uint16:
// 				instance.AddField(colnames[i], uint16(0), "")
// 			case reflect.Uint32:
// 				instance.AddField(colnames[i], uint32(0), "")
// 			case reflect.Uint64:
// 				instance.AddField(colnames[i], uint64(0), "")
// 			case reflect.Float32:
// 				instance.AddField(colnames[i], float32(0), "")
// 			case reflect.Float64:
// 				instance.AddField(colnames[i], float64(0), "")
// 			case reflect.String:
// 				instance.AddField(colnames[i], "", "")
// 			}
// 		}
// 		return instance.Build()
// 	}()

//
// for i := range data[0].Values {
// 	f := fields[i]
// 	if _, ok := primitiveKindToString[f.ckind]; !ok {
// 		// todo: log not a primitive field
// 		return false
// 	}
// 	v := &sheets.CellData{}
// 	v.UserEnteredValue = &sheets.ExtendedValue{}
// 	if f.isBool() {
// 		v.UserEnteredValue.BoolValue = f.cvalue.(bool)
// 	} else if f.isNumeric() {
// 		var value float64
// 		switch f.ckind {
// 		case reflect.Int:
// 			value = float64(f.cvalue.(int))
// 		case reflect.Int8:
// 			value = float64(f.cvalue.(int8))
// 		case reflect.Int16:
// 			value = float64(f.cvalue.(int16))
// 		case reflect.Int32:
// 			value = float64(f.cvalue.(int32))
// 		case reflect.Int64:
// 			value = float64(f.cvalue.(int64))
// 		case reflect.Uint:
// 			value = float64(f.cvalue.(uint))
// 		case reflect.Uint8:
// 			value = float64(f.cvalue.(uint8))
// 		case reflect.Uint16:
// 			value = float64(f.cvalue.(uint16))
// 		case reflect.Uint32:
// 			value = float64(f.cvalue.(uint32))
// 		case reflect.Uint64:
// 			value = float64(f.cvalue.(uint64))
// 		case reflect.Float32:
// 			value = float64(f.cvalue.(float32))
// 		case reflect.Float64:
// 			value = float64(f.cvalue.(float64))
// 		}
// 		v.UserEnteredValue.NumberValue = value
// 	} else if f.isString() {
// 		v.UserEnteredValue.StringValue = f.cvalue.(string)
// 	} else {
// 		// todo log not supported
// 		return false
// 	}

// 	data[0].Values[i] = v
// }
