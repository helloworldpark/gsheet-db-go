package gosheet

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"

	"golang.org/x/oauth2"
	"google.golang.org/api/sheets/v4"
)

const dbFileStart = "database_file_"
const tableDataStartRowIndex = 3
const tableDataStartColumnIndex = 0

func init() {
	initPrimitiveKind()
}

/*
 * SheetManager api
 */

// SheetManager Manage OAuth2 token lifecycle
type SheetManager struct {
	client  *http.Client
	service *sheets.Service

	token          *oauth2.Token
	credentialJSON string
}

// NewSheetManager Create new SheetManager
func NewSheetManager(jsonPath string) *SheetManager {
	client := http.DefaultClient
	service, _ := sheets.New(client)
	token := CreateJWTToken(jsonPath)

	m := &SheetManager{
		client:         client,
		token:          token,
		service:        service,
		credentialJSON: jsonPath,
	}
	return m
}

// RefreshToken refreshes token if not valid
func (m *SheetManager) RefreshToken() {
	if !m.token.Valid() {
		token := CreateJWTToken(m.credentialJSON)
		m.token = token
	}
	return
}

// CreateDatabase creates a new database with the given database_file_`title`.
// Be careful, 'database_file_' tag is on the start of the title.
func (m *SheetManager) CreateDatabase(title string) *Database {
	db := m.createSpreadsheet(dbFileStart + title)
	return &Database{
		manager:     m,
		spreadsheet: db,
	}
}

// FindDatabase gets a new database with the given database_file_`title`, if exists.
// If not existing, it will return nil.
// Be careful, 'database_file_' tag is on the start of the title finding for.
func (m *SheetManager) FindDatabase(title string) *Database {
	if db := m.findSpreadsheet(dbFileStart + title); db != nil {
		return &Database{
			manager:     m,
			spreadsheet: db,
		}
	}
	return nil
}

// DropDatabase deletes a database with the given database_file_`title`, if exists.
// If not existing, or failed to delete, it will log and return false.
// Be careful, 'database_file_' tag is implicitly on the start of the title finding for.
func (m *SheetManager) DropDatabase(title string) bool {
	db := m.FindDatabase(title)
	if db == nil {
		return false
	}
	return m.deleteSpreadsheet(db.spreadsheet.SpreadsheetId)
}

// SynchronizeFromGoogle Synchronize data from google
func (m *SheetManager) SynchronizeFromGoogle(db *Database) *Database {
	if db == nil {
		return nil
	}

	rawDB := m.getSpreadsheet(db.spreadsheet.SpreadsheetId)
	if rawDB == nil {
		fmt.Printf("[SheetManager] SyncDatabaseToGoogle: DB is nil")
		return nil
	}
	db.spreadsheet = rawDB
	return db
}

/*
 * Database api
 */

// Database Wrapper for *sheets.Spreadsheet
type Database struct {
	manager     *SheetManager
	spreadsheet *sheets.Spreadsheet
}

// Manager Proxy
func (m *Database) Manager() *SheetManager {
	return m.manager
}

// Spreadsheet Proxy
func (m *Database) Spreadsheet() *sheets.Spreadsheet {
	return m.spreadsheet
}

// Sheets Proxy
func (m *Database) Sheets() []*sheets.Sheet {
	return m.spreadsheet.Sheets
}

func (m *Database) IsValidTable(sheet *sheets.Sheet) bool {
	if sheet.Properties.Title == "Sheet1" {
		return false
	}
	return true
}

// NewTableFromSheet creates new *Table instance
func (m *Database) NewTableFromSheet(sheet *sheets.Sheet) *Table {
	req := newSpreadsheetValuesRequest(m.manager, m.Spreadsheet().SpreadsheetId, sheet.Properties.Title)
	req.updateRange(sheet.Properties.Title, 0, 0, 3, 25) // todo: hardcoding
	valueRange := req.Do()

	if valueRange == nil {
		return nil
	}

	// todo: error check
	rows, _ := strconv.ParseInt(valueRange.Values[2][0].(string), 10, 64)
	cols, _ := strconv.ParseInt(valueRange.Values[2][1].(string), 10, 64)
	constraints := ""
	if len(valueRange.Values[2]) > 2 {
		constraints = valueRange.Values[2][2].(string)
	}

	colnames := make([]string, cols)
	dtypes := make([]reflect.Kind, cols)
	for i := range colnames {
		colnames[i] = valueRange.Values[0][i].(string)
		dtypes[i] = primitiveStringToKind[valueRange.Values[1][i].(string)]
	}

	table := &Table{}
	table.sheet = sheet
	table.database = m
	table.manager = m.manager
	table.metadata = &TableMetadata{}
	table.metadata.Columns = colnames
	table.metadata.Constraints = constraints
	table.metadata.Name = sheet.Properties.Title
	table.metadata.Rows = rows
	table.metadata.Types = dtypes
	return table
}

/*
 * Database api implementations
 */

// createSpreadsheet creates a single spreadsheet file
func (m *SheetManager) createSpreadsheet(title string) *sheets.Spreadsheet {
	// check duplicated title
	if m.findSpreadsheet(title) != nil {
		return nil
	}

	return newSpreadsheetCreateRequest(m, title).Do()
}

// getSpreadsheet gets a single spreadsheet file with id, if exists.
func (m *SheetManager) getSpreadsheet(spreadsheetID string) *sheets.Spreadsheet {
	req := m.service.Spreadsheets.Get(spreadsheetID).IncludeGridData(true)
	req.Header().Add("Authorization", "Bearer "+m.token.AccessToken)
	resp, err := req.Do()
	if err != nil {
		panic(err)
	}
	return resp
}

// deleteSpreadsheet deletes spreadsheet file with `spreadsheetId`
// Returns true if deleted(status code 20X)
//         false if else
// https://stackoverflow.com/questions/46836393/how-do-i-delete-a-spreadsheet-file-using-google-spreadsheets-api
// https://stackoverflow.com/questions/46310113/consume-a-delete-endpoint-from-golang
func (m *SheetManager) deleteSpreadsheet(spreadsheetID string) bool {
	resp := newURLRequest(m, delete, fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s", spreadsheetID)).Do()
	return resp.StatusCode/100 == 2
}

// listSpreadsheets lists spreadsheets' id by []string
func (m *SheetManager) listSpreadsheets() []string {
	req := newURLRequest(m, get, "https://www.googleapis.com/drive/v3/files")
	// https://stackoverflow.com/questions/30652577/go-doing-a-get-request-and-building-the-querystring
	// https://developers.google.com/drive/api/v3/mime-types
	req.AddQuery("q", "mimeType='application/vnd.google-apps.spreadsheet'")
	resp := req.Do()
	read, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		panic(err)
	}

	var readJSON interface{}
	json.Unmarshal(read, &readJSON)

	mapJSON, ok := readJSON.(map[string]interface{})
	if !ok {
		return nil
	}

	files, ok := mapJSON["files"]
	if !ok {
		return nil
	}
	fileArr, ok := files.([]interface{})
	if !ok {
		return nil
	}

	var sheetArr []string
	for _, f := range fileArr {
		fmap, ok := f.(map[string]interface{})
		if !ok {
			continue
		}
		trashed, ok := fmap["trashed"].(bool)
		if ok && trashed {
			continue
		}
		fileID, ok := fmap["id"].(string)
		if !ok {
			continue
		}
		sheetArr = append(sheetArr, fileID)

	}
	return sheetArr
}

// findSpreadsheet finds a spreadsheet with `title`
func (m *SheetManager) findSpreadsheet(title string) *sheets.Spreadsheet {
	sheetIDs := m.listSpreadsheets()
	for _, sheetID := range sheetIDs {
		s := m.getSpreadsheet(sheetID)
		if s.Properties.Title == title {
			return s
		}
	}
	return nil
}

/*
 * Internal methods
 */

type httpRequest interface {
	Header() http.Header
}

type httpMethod string

const (
	get    httpMethod = "GET"
	post   httpMethod = "POST"
	delete httpMethod = "DELETE"
	put    httpMethod = "PUT"
)

type httpURLRequest struct {
	req     *http.Request
	manager *SheetManager
}

func newURLRequest(manager *SheetManager, method httpMethod, url string) *httpURLRequest {
	req, err := http.NewRequest(string(method), url, nil)
	if err != nil {
		panic(err)
	}
	return &httpURLRequest{
		req:     req,
		manager: manager,
	}
}

// interface httpRequest
func (r *httpURLRequest) Header() http.Header {
	return r.req.Header
}

func (r *httpURLRequest) AddQuery(key, value string) *httpURLRequest {
	values := r.req.URL.Query()
	values.Add(key, value)
	r.req.URL.RawQuery = values.Encode()
	return r
}

func (r *httpURLRequest) Do() *http.Response {
	r.manager.RefreshToken()
	r.req.Header.Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	resp, err := r.manager.client.Do(r.req)
	if err != nil {
		panic(err)
	}
	return resp
}

type httpSpreadsheetCreateRequest struct {
	req     *sheets.SpreadsheetsCreateCall
	manager *SheetManager
}

func newSpreadsheetCreateRequest(manager *SheetManager, title string) *httpSpreadsheetCreateRequest {
	rb := &sheets.Spreadsheet{
		Properties: &sheets.SpreadsheetProperties{
			Title:      title,
			TimeZone:   "Asia/Seoul",
			AutoRecalc: "ON_CHANGE",
		},
	}
	req := manager.service.Spreadsheets.Create(rb)
	return &httpSpreadsheetCreateRequest{
		req:     req,
		manager: manager,
	}
}

// interface httpSpreadsheetCreateRequest
func (r *httpSpreadsheetCreateRequest) Header() http.Header {
	return r.req.Header()
}

func (r *httpSpreadsheetCreateRequest) Do() *sheets.Spreadsheet {
	r.manager.RefreshToken()
	r.req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	resp, err := r.req.Do()
	if err != nil {
		panic(err)
	}
	return resp
}

type httpBatchUpdateRequest struct {
	batchRequest  *sheets.BatchUpdateSpreadsheetRequest
	manager       *SheetManager
	spreadsheetID string
}

func newSpreadsheetBatchUpdateRequest(manager *SheetManager, spreadsheetID string, requests ...*sheets.Request) *httpBatchUpdateRequest {
	batchRequest := &sheets.BatchUpdateSpreadsheetRequest{}
	batchRequest.IncludeSpreadsheetInResponse = true
	batchRequest.Requests = requests
	return &httpBatchUpdateRequest{
		batchRequest:  batchRequest,
		manager:       manager,
		spreadsheetID: spreadsheetID,
	}
}

func (r *httpBatchUpdateRequest) updateRequest(req *sheets.Request) {
	if req == nil {
		return
	}
	r.batchRequest.Requests = append(r.batchRequest.Requests, req)
}

func (r *httpBatchUpdateRequest) Do() (*sheets.Spreadsheet, []*sheets.Response, int) {
	r.manager.RefreshToken()
	req := r.manager.service.Spreadsheets.BatchUpdate(r.spreadsheetID, r.batchRequest)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	resp, err := req.Do()
	if err != nil {
		panic(err)
	}
	return resp.UpdatedSpreadsheet, resp.Replies, resp.HTTPStatusCode
}

type httpValueRangeRequest struct {
	manager       *SheetManager
	ranges        string
	spreadsheetID string
}

func newSpreadsheetValuesRequest(manager *SheetManager, spreadsheetID, tableName string) *httpValueRangeRequest {
	return &httpValueRangeRequest{
		manager:       manager,
		ranges:        defaultRange,
		spreadsheetID: spreadsheetID,
	}
}

// updateRange does not include end
func (r *httpValueRangeRequest) updateRange(tablename string, startRow, startCol, endRow, endCol int64) bool {
	ranges := newCellRange(tablename, startRow, startCol, endRow, endCol)
	r.manager.RefreshToken()
	r.ranges = ranges.String()
	return true
}

func (r *httpValueRangeRequest) Do() *sheets.ValueRange {
	r.manager.RefreshToken()
	req := r.manager.service.Spreadsheets.Values.Get(r.spreadsheetID, r.ranges)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	valueRange, err := req.Do()
	if err != nil {
		panic(err)
	}
	return valueRange
}

type httpUpdateValuesRequest struct {
	manager        *SheetManager
	ranges         string
	spreadsheetID  string
	updatingValues [][]interface{}
}

func newSpreadsheetValuesUpdateRequest(manager *SheetManager, spreadsheetID, tableName string) *httpUpdateValuesRequest {
	return &httpUpdateValuesRequest{
		manager:       manager,
		ranges:        defaultRange,
		spreadsheetID: spreadsheetID,
	}
}

// updateRange does not include end
func (r *httpUpdateValuesRequest) updateRange(metadata *TableMetadata, values []interface{}) bool {
	if len(values) == 0 {
		return false
	}
	startRow := metadata.Rows + tableDataStartRowIndex
	endRow := startRow + int64(len(values))
	const startCol = tableDataStartColumnIndex
	endCol := int64(startCol + len(metadata.Columns))

	for i := range values {
		r.updatingValues = append(r.updatingValues, make([]interface{}, 0))
		reflected := reflect.ValueOf(values[i])
		for j := 0; j < len(metadata.Columns); j++ {
			r.updatingValues[i] = append(r.updatingValues[i], reflected.FieldByName(metadata.Columns[j]).Interface())
		}
	}

	ranges := newCellRange(metadata.Name, startRow, startCol, endRow, endCol)
	r.ranges = ranges.String()
	return true
}

func (r *httpUpdateValuesRequest) updateRows(metadata *TableMetadata, adding int) bool {
	unpackedValues := make([][]interface{}, 1)
	unpackedValues[0] = make([]interface{}, 1)
	unpackedValues[0][0] = metadata.Rows + int64(adding)

	r.ranges = fmt.Sprintf("%s!A3", metadata.Name)
	r.updatingValues = unpackedValues
	return true
}

func (r *httpUpdateValuesRequest) Do() int {
	r.manager.RefreshToken()
	rangeValues := &sheets.ValueRange{}
	rangeValues.Range = r.ranges
	rangeValues.Values = r.updatingValues
	req := r.manager.service.Spreadsheets.Values.Update(r.spreadsheetID, r.ranges, rangeValues)
	req.ValueInputOption("RAW")
	req.IncludeValuesInResponse(true)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	updatedRange, err := req.Do()
	if err != nil {
		fmt.Println(r)
		panic(err)
	}
	return updatedRange.HTTPStatusCode
}
