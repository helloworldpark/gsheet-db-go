package main

import (
	"fmt"
	"reflect"

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
	newSheet := m.batchUpdate(database, request)
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
	resp := m.batchUpdate(database, request)
	return resp != nil
}

func (m *SheetManager) batchUpdate(database *sheets.Spreadsheet, requests []*sheets.Request) *sheets.Spreadsheet {
	batchRequest := &sheets.BatchUpdateSpreadsheetRequest{}
	batchRequest.IncludeSpreadsheetInResponse = true
	batchRequest.Requests = requests
	req := m.service.Spreadsheets.BatchUpdate(database.SpreadsheetId, batchRequest)
	req.Header().Add("Authorization", "Bearer "+m.token.AccessToken)
	resp, err := req.Do()
	if err != nil {
		panic(err)
	}
	return resp.UpdatedSpreadsheet
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

	requests[0].UpdateCells.Range = &sheets.GridRange{}
	requests[0].UpdateCells.Range.SheetId = table.Properties.SheetId
	requests[0].UpdateCells.Range.StartRowIndex = 0
	requests[0].UpdateCells.Range.EndRowIndex = 2

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
	if len(constraints) > 0 {
		data[2].Values = make([]*sheets.CellData, 3)
	} else {
		data[2].Values = make([]*sheets.CellData, 2)
	}
	data[2] = &sheets.RowData{}
	data[2].Values[0].UserEnteredValue = &sheets.ExtendedValue{}
	data[2].Values[0].UserEnteredValue.NumberValue = 0
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
	updated := m.batchUpdate(database, requests)
	if updated != nil {
		return newSheet
	}
	return nil
}

type TableMetadata struct {
	Columns     []string
	Types       []reflect.Kind
	Rows        int64
	Constraints string
}

func (m *SheetManager) ReadTableMetadataFromStruct(db *sheets.Spreadsheet, s interface{}) *TableMetadata {
	tableName := reflect.TypeOf(s).Name()
	table := m.GetTable(db, tableName)
	if table == nil {
		return nil
	}

	// DB를 갱신

	// 0행~2행, 모든 열을 읽는다
	// requests[0].

	return nil
}

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
