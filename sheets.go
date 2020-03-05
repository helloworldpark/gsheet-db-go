package main

import (
	"fmt"
	"reflect"
	"strconv"

	"google.golang.org/api/sheets/v4"
)

/*
 * Table api
 */

// CreateEmptyTable Creates a new sheet(a.k.a. table) with `tableName` on the given Spreadsheet(a.k.a. database).
// Case handling:
// database == nil: return nil
// database != nil && has sheet with tableName: log as existing, and return the existing sheet with false
// database != nil && does not have sheet with tableName: log as creating, and return the created sheet with true
func (m *SheetManager) CreateEmptyTable(database *sheets.Spreadsheet, tableName string) (*sheets.Sheet, bool) {
	if database == nil {
		return nil, false
	}
	for _, sheet := range database.Sheets {
		if sheet.Properties.Title == tableName {
			// todo: log
			return sheet, false
		}
	}
	request := make([]*sheets.Request, 1)
	request[0] = &sheets.Request{}
	request[0].AddSheet = &sheets.AddSheetRequest{}
	request[0].AddSheet.Properties = &sheets.SheetProperties{}
	request[0].AddSheet.Properties.Title = tableName
	newSheet, _, statusCode := m.batchUpdate(database, request)
	if statusCode/100 != 2 {
		return nil, false
	}
	return newSheet.Sheets[len(newSheet.Sheets)-1], true
}

// GetTable Gets an existing sheet(a.k.a. table) with `tableName` on the given Spreadsheet(a.k.a. database)
// If exists, returns the existed one
// If not existing, returns nil
func (m *SheetManager) GetTable(database *sheets.Spreadsheet, tableName string) *sheets.Sheet {
	tables := m.GetTableList(database)
	for _, table := range tables {
		if table.Properties.Title == tableName {
			return table
		}
	}
	return nil
}

// GetTableList Gets an existing sheets(a.k.a. table) in the given Spreadsheet(a.k.a. database).
// If exists, returns the existings
// If not existing, returns nil
func (m *SheetManager) GetTableList(database *sheets.Spreadsheet) []*sheets.Sheet {
	if database == nil {
		return nil
	}
	return database.Sheets
}

// DeleteTable Deletes an existing sheet(a.k.a. table) with `tableName` on the given Spreadsheet(a.k.a. database)
// If exists, deletes and returns true
// If not existing, logs and returns false
func (m *SheetManager) DeleteTable(database *sheets.Spreadsheet, tableName string) bool {
	sheetToDelete := m.GetTable(database, tableName)
	if sheetToDelete == nil {
		// todo: log
		return false
	}

	request := make([]*sheets.Request, 1)
	request[0] = &sheets.Request{}
	request[0].DeleteSheet = &sheets.DeleteSheetRequest{}
	request[0].DeleteSheet.SheetId = sheetToDelete.Properties.SheetId
	resp, _, _ := m.batchUpdate(database, request)
	return resp != nil
}

func (m *SheetManager) batchUpdate(database *sheets.Spreadsheet, requests []*sheets.Request) (*sheets.Spreadsheet, []*sheets.Response, int) {
	return newSpreadsheetBatchUpdateRequest(m, database.SpreadsheetId, requests...).Do()
}

var primitiveStringToKind = make(map[string]reflect.Kind)
var primitiveKindToString = make(map[reflect.Kind]string)

func initPrimitiveKind() {
	if len(primitiveStringToKind) > 0 {
		return
	}
	for k := reflect.Bool; k <= reflect.Uint64; k++ {
		primitiveStringToKind[k.String()] = k
		primitiveKindToString[k] = k.String()
	}
	for k := reflect.Float32; k <= reflect.Float64; k++ {
		primitiveStringToKind[k.String()] = k
		primitiveKindToString[k] = k.String()
	}
	primitiveStringToKind[reflect.String.String()] = reflect.String
	primitiveKindToString[reflect.String] = reflect.String.String()
}

func isPrimitive(i interface{}) bool {
	_, ok := primitiveKindToString[reflect.TypeOf(i).Kind()]
	return ok
}

type structField struct {
	cname  string
	ctype  string
	ckind  reflect.Kind
	cvalue interface{}
}

func (f structField) isBool() bool {
	return f.ckind == reflect.Bool
}

func (f structField) isString() bool {
	return f.ckind == reflect.String
}

func (f structField) isNumeric() bool {
	return reflect.Int8 <= f.ckind && f.ckind <= reflect.Float64
}

func analyseStruct(structInstance interface{}) []structField {
	initPrimitiveKind()

	reflected := reflect.TypeOf(structInstance)
	if reflected.Kind() != reflect.Struct {
		return nil
	}
	reflectedValue := reflect.ValueOf(structInstance)

	n := reflected.NumField()
	fields := make([]structField, n)

	for i := 0; i < n; i++ {
		value := reflectedValue.Field(i)
		valueType := reflectedValue.Type().Field(i)
		valueKind := valueType.Type.Kind()

		typestring, ok := primitiveKindToString[valueKind]
		if !ok {
			fmt.Println("Not a struct: ", value)
			return nil
		}

		fields[i].ctype = typestring
		fields[i].cname = valueType.Name
		fields[i].ckind = valueKind
		fields[i].cvalue = value.Interface()
	}
	return fields
}

func (m *SheetManager) createColumnsFromStruct(table *sheets.Sheet, structInstance interface{}, constraints ...interface{}) []*sheets.Request {
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
	if len(constraints) > 0 {
		data[2].Values = append(data[2].Values, &sheets.CellData{})
		data[2].Values[2] = &sheets.CellData{}
	}
	data[2].Values[0].UserEnteredValue = &sheets.ExtendedValue{}
	data[2].Values[0].UserEnteredValue.NumberValue = 0.0
	data[2].Values[1].UserEnteredValue = &sheets.ExtendedValue{}
	data[2].Values[1].UserEnteredValue.NumberValue = float64(len(fields))
	if len(constraints) > 0 {
		data[2].Values[2].UserEnteredValue = &sheets.ExtendedValue{}
		// data[2].Values[2].UserEnteredValue.StringValue =
		// todo
	}

	requests[0].UpdateCells.Rows = data
	return requests
}

func (m *SheetManager) CreateTableFromStruct(database *sheets.Spreadsheet, structInstance interface{}, constraints ...interface{}) *sheets.Sheet {
	tableName := reflect.TypeOf(structInstance).Name()
	newSheet, isNew := m.CreateEmptyTable(database, tableName)
	if !isNew {
		return newSheet
	}

	requests := m.createColumnsFromStruct(newSheet, structInstance, constraints...)
	updated, _, _ := m.batchUpdate(database, requests)
	if updated == nil {
		return nil
	}
	for _, sheet := range updated.Sheets {
		if sheet.Properties.SheetId == newSheet.Properties.SheetId {
			return sheet
		}
	}
	return nil
}

type TableMetadata struct {
	Name        string
	Columns     []string
	Types       []reflect.Kind
	Rows        int64
	Constraints string
}

func (m *SheetManager) ReadTableMetadataFromStruct(db *sheets.Spreadsheet, s interface{}) *TableMetadata {
	tableName := reflect.TypeOf(s).Name()
	tableCols := reflect.TypeOf(s).NumField()

	// DB를 갱신
	db = m.SynchronizeFromGoogle(db)
	table := m.GetTable(db, tableName)
	if table == nil {
		return nil
	}

	// 0행~2행, 모든 열을 읽는다
	// 0행
	req := newSpreadsheetValuesRequest(m, db.SpreadsheetId, tableName)
	req.updateRange(tableName, 0, 0, 2, int64(tableCols))
	valueRange := req.Do()

	fmt.Println("READ META")
	fmt.Println(valueRange.Values)

	colnames := make([]string, len(table.Data[0].RowData[0].Values))
	for i := range colnames {
		colnames[i] = valueRange.Values[0][i].(string)
	}
	// 1행
	types := make([]reflect.Kind, len(table.Data[0].RowData[1].Values))
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

	var constraints = ""
	if len(valueRange.Values[2]) >= 3 {
		constraints = valueRange.Values[2][2].(string)
	}
	metadata := &TableMetadata{
		Name:        tableName,
		Columns:     colnames,
		Types:       types,
		Rows:        rows,
		Constraints: constraints,
	}
	return metadata
}

func (m *SheetManager) ReadTableDataFromStruct(db *sheets.Spreadsheet, s interface{}, rows int64) ([][]interface{}, *TableMetadata) {
	metadata := m.ReadTableMetadataFromStruct(db, s)
	if metadata == nil {
		fmt.Println("ReadTableDataFromStruct: Metadata is nil")
		return nil, nil
	}
	if metadata.Rows == 0 {
		fmt.Println("ReadTableDataFromStruct: Metadata.rows is 0")
		return nil, nil
	}
	if rows == 0 {
		fmt.Println("ReadTableDataFromStruct: rows is 0")
		return nil, nil
	} else if rows == -1 {
		rows = metadata.Rows
	}
	table := m.GetTable(db, metadata.Name)
	if table == nil {
		fmt.Println("ReadTableDataFromStruct: table is nil")
		return nil, nil
	}

	// 3행~, 모든 열을 읽는다
	req := newSpreadsheetValuesRequest(m, db.SpreadsheetId, metadata.Name)
	req.updateRange(metadata.Name, 3, 0, 3+rows, int64(len(metadata.Columns))-1)
	valueRange := req.Do()

	return valueRange.Values, nil
}

type Predicate func(interface{}) bool

func (m *SheetManager) ReadTableDataWithFilter(db *sheets.Spreadsheet, s interface{}, filters map[int]Predicate) [][]interface{} {
	fullData, _ := m.ReadTableDataFromStruct(db, s, -1)
	if len(filters) == 0 {
		return fullData
	}
	if len(fullData) == 0 {
		return fullData
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
	return filtered
}

func (m *SheetManager) WriteTableData(db *sheets.Spreadsheet, values []interface{}) bool {
	if len(values) == 0 {
		return false
	}
	metadata := m.ReadTableMetadataFromStruct(db, values[0])
	if metadata == nil {
		return false
	}
	table := m.GetTable(db, metadata.Name)
	if table == nil {
		return false
	}

	// 4행~, 쓸 수 있는만큼 쓴다
	req := newSpreadsheetValuesUpdateRequest(m, db.SpreadsheetId, metadata.Name)
	req.updateRange(metadata, values)
	if req.Do()/100 != 2 {
		return false
	}

	// 기록한 행 업데이트
	req.updateRows(metadata, len(values))
	if req.Do()/100 != 2 {
		return false
	}
	fmt.Println("Updated rows ", len(values))
	return true
}

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
