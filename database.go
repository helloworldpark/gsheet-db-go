package gosheet

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/sheets/v4"
)

const dbFileStart = "database_file_"
const tableDataStartRowIndex int64 = 3
const tableDataStartColumnIndex int64 = 0

/*
 * SheetManager api
 */

// SheetManager Manage OAuth2 token lifecycle
type SheetManager struct {
	client  *http.Client
	service *sheets.Service

	token          *oauth2.Token
	credentialJSON string

	apiUsage      int64
	lastQuotaTime int64
}

// NewSheetManager Create new SheetManager
// api count: 1
func NewSheetManager(jsonPath string) *SheetManager {
	client := http.DefaultClient
	service, _ := sheets.New(client)
	token := createJWTToken(jsonPath)

	m := &SheetManager{
		client:         client,
		token:          token,
		service:        service,
		credentialJSON: jsonPath,
	}
	m.enqueueAPIUsage(1, false)
	return m
}

// refreshToken refreshes token if not valid
func (m *SheetManager) refreshToken() {
	if !m.token.Valid() {
		token := createJWTToken(m.credentialJSON)
		m.token = token
	}
	return
}

// CreateDatabase creates a new database with the given database_file_`title`.
// Be careful, 'database_file_' tag is on the start of the title.
// api count: 1
func (m *SheetManager) CreateDatabase(title string) *Database {
	db, sheetCount := m.createSpreadsheet(dbFileStart + title)
	m.enqueueAPIUsage(sheetCount+1, false)
	return &Database{
		manager:     m,
		spreadsheet: db,
	}
}

// FindDatabase gets a new database with the given database_file_`title`, if exists.
// If not existing, it will return nil.
// Be careful, 'database_file_' tag is on the start of the title finding for.
// api count: 1
func (m *SheetManager) FindDatabase(title string) *Database {
	db, sheetCount := m.findSpreadsheet(dbFileStart + title)
	m.enqueueAPIUsage(sheetCount+1, false)
	if db != nil {
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
	m.enqueueAPIUsage(1, false)
	return m.deleteSpreadsheet(db.spreadsheet.SpreadsheetId)
}

// synchronizeFromGoogle Synchronize data from google
// api count: 1
func (m *SheetManager) synchronizeFromGoogle(db *Database) *Database {
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
 * google sheets api queue
 */

func (m *SheetManager) enqueueAPIUsage(task int64, blockIfQuota bool) {
	// 출처: https://hakurei.tistory.com/193 [Reimu's Development Blog])
	now := time.Now().In(time.FixedZone("GMT-7", -7*60*60)).Unix()
	if m.lastQuotaTime == 0 {
		m.lastQuotaTime = (now / 100) * 100
	} else if m.lastQuotaTime+100 <= now {
		m.lastQuotaTime = (now / 100) * 100
	}
	// check if api usage enough
	fmt.Println("API Usage: ", m.apiUsage)
	if m.apiUsage >= 90 {
		if blockIfQuota {
			timeToWait := time.Second * time.Duration(m.lastQuotaTime+100-now)
			fmt.Printf("[%v] Pending %v seconds, api usage: %v\n", now, m.lastQuotaTime+100-now, m.apiUsage)
			<-time.NewTimer(timeToWait).C
		}

		now = time.Now().In(time.FixedZone("GMT-7", -7*60*60)).Unix()
		fmt.Printf("[%v] Resetting quota\n", now)
		m.lastQuotaTime += 100
		m.apiUsage = 0
	}
	m.apiUsage += task
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

func (m *Database) isValidTable(sheet *sheets.Sheet) bool {
	if strings.HasPrefix(sheet.Properties.Title, "Sheet") {
		return false
	}
	return true
}

// newTableFromSheet creates new *Table instance
// api count: 2
func (m *Database) newTableFromSheet(sheet *sheets.Sheet) *Table {
	req := newSpreadsheetValuesRequest(m.manager, m.Spreadsheet().SpreadsheetId, sheet.Properties.Title)
	req.updateRange(sheet.Properties.Title, 0, 0, 3, 25) // todo: hardcoding
	valueRange := req.Do()

	if valueRange == nil {
		return nil
	}

	// todo: error check
	rows, err := strconv.ParseInt(valueRange.Values[2][0].(string), 10, 64)
	if err != nil {
		panic(err)
	}
	cols, err := strconv.ParseInt(valueRange.Values[2][1].(string), 10, 64)
	if err != nil {
		panic(err)
	}
	constraint := ""
	if len(valueRange.Values[2]) > 2 {
		constraint = valueRange.Values[2][2].(string)
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
	table.scheme = &TableScheme{}
	table.scheme.Columns = colnames

	table.scheme.Name = sheet.Properties.Title
	table.scheme.Rows = rows
	table.scheme.Types = dtypes

	table.scheme.Constraints = newConstraintFromString(constraint)
	// update index
	// will update if constraints is valid
	table.updatedHeader()
	table.updateIndex()
	return table
}

/*
 * Database api implementations
 */

// createSpreadsheet creates a single spreadsheet file
// api count: 1 + sheetIDs
func (m *SheetManager) createSpreadsheet(title string) (*sheets.Spreadsheet, int64) {
	// check duplicated title
	db, sheetCount := m.findSpreadsheet(title)
	if db != nil {
		return nil, sheetCount
	}

	return newSpreadsheetCreateRequest(m, title).Do(), sheetCount
}

// getSpreadsheet gets a single spreadsheet file with id, if exists.
// api count: 1
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
// api count: 1
func (m *SheetManager) deleteSpreadsheet(spreadsheetID string) bool {
	resp := newURLRequest(m, delete, fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s", spreadsheetID)).Do()
	return resp.StatusCode/100 == 2
}

// listSpreadsheets lists spreadsheets' id by []string
// api count: 1
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
// api count: 1 + sheetIDs
func (m *SheetManager) findSpreadsheet(title string) (*sheets.Spreadsheet, int64) {
	sheetIDs := m.listSpreadsheets()
	for _, sheetID := range sheetIDs {
		s := m.getSpreadsheet(sheetID)
		if s.Properties.Title == title {
			return s, int64(len(sheetIDs))
		}
	}
	return nil, 0
}

/*
 * Internal methods
 */

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

func (r *httpURLRequest) AddQuery(key, value string) *httpURLRequest {
	values := r.req.URL.Query()
	values.Add(key, value)
	r.req.URL.RawQuery = values.Encode()
	return r
}

func (r *httpURLRequest) Do() *http.Response {
	r.manager.refreshToken()
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
	r.manager.refreshToken()
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
	r.manager.refreshToken()
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
	r.manager.refreshToken()
	r.ranges = ranges.String()
	return true
}

func (r *httpValueRangeRequest) Do() *sheets.ValueRange {
	r.manager.refreshToken()
	req := r.manager.service.Spreadsheets.Values.Get(r.spreadsheetID, r.ranges)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	valueRange, err := req.Do()
	if err != nil {
		panic(err)
	}
	return valueRange
}

type spreadsheetValuesBatchUpdateRequest struct {
	manager        *SheetManager
	spreadsheetID  string
	rangeValues    string
	updatingValues [][]interface{}
	rangeRows      string
	updatingRows   [][]interface{}
}

func newSpreadsheetValuesBatchUpdateRequest(manager *SheetManager, spreadsheetID, tableName string) *spreadsheetValuesBatchUpdateRequest {
	return &spreadsheetValuesBatchUpdateRequest{
		manager:       manager,
		rangeValues:   "",
		spreadsheetID: spreadsheetID,
	}
}

// rangeString does not include end
func rangeString(metadata *TableScheme, startRow, appendingRow int64) cellRange {
	endRow := startRow + appendingRow
	const startCol = tableDataStartColumnIndex
	endCol := startCol + int64(len(metadata.Columns))

	ranges := newCellRange(metadata.Name, startRow, startCol, endRow, endCol)
	return ranges
}

// updateRange does not include end
// values: values[0]: 0th struct, values[0][4]: 4th column value of 0th struct
func (r *spreadsheetValuesBatchUpdateRequest) updateRange(scheme *TableScheme, appendData bool, values [][]interface{}) bool {
	if len(values) == 0 {
		return false
	}

	r.updatingValues = nil
	for i := range values {
		r.updatingValues = append(r.updatingValues, make([]interface{}, 0))
		for j := 0; j < len(scheme.Columns); j++ {
			r.updatingValues[i] = append(r.updatingValues[i], values[i][j])
		}
	}

	startRow := tableDataStartRowIndex
	if appendData {
		startRow += scheme.Rows
	}
	ranges := rangeString(scheme, startRow, int64(len(values)))
	r.rangeValues = ranges.String()
	return true
}

func (r *spreadsheetValuesBatchUpdateRequest) updateRows(scheme *TableScheme, appendData bool, newRows int) bool {
	unpackedValues := make([][]interface{}, 1)
	unpackedValues[0] = make([]interface{}, 1)
	unpackedValues[0][0] = int64(newRows)
	if appendData {
		unpackedValues[0][0] = int64(newRows) + scheme.Rows
	}

	r.rangeRows = fmt.Sprintf("%s!A3", scheme.Name)
	r.updatingRows = unpackedValues
	return true
}

func (r *spreadsheetValuesBatchUpdateRequest) Do() int {
	r.manager.refreshToken()

	batchRequest := &sheets.BatchUpdateValuesRequest{}
	batchRequest.IncludeValuesInResponse = true
	batchRequest.ValueInputOption = "RAW"
	if len(r.rangeValues) == 0 {
		// no data to update
		batchRequest.Data = make([]*sheets.ValueRange, 1)

		rangeRows := &sheets.ValueRange{}
		rangeRows.Range = r.rangeRows
		rangeRows.Values = r.updatingRows

		batchRequest.Data[0] = rangeRows
	} else {
		batchRequest.Data = make([]*sheets.ValueRange, 2)
		rangeValues := &sheets.ValueRange{}
		rangeValues.Range = r.rangeValues
		rangeValues.Values = r.updatingValues

		rangeRows := &sheets.ValueRange{}
		rangeRows.Range = r.rangeRows
		rangeRows.Values = r.updatingRows

		batchRequest.Data[0] = rangeValues
		batchRequest.Data[1] = rangeRows
	}

	req := r.manager.service.Spreadsheets.Values.BatchUpdate(r.spreadsheetID, batchRequest)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	updatedRange, err := req.Do()

	if err != nil {
		// gerr, ok := err.(*googleapi.Error)
		// if ok {
		// 	for _, em := range gerr.Errors {
		// 		fmt.Printf("-ErrorItem Message: %v Reason: %v\n", em.Message, em.Reason)
		// 	}
		// }
		panic(err)
	}
	return updatedRange.HTTPStatusCode
}

type clearValuesRequest struct {
	manager       *SheetManager
	ranges        cellRange
	spreadsheetID string
}

func newClearValuesRequest(manager *SheetManager, spreadsheetID string, ranges cellRange) *clearValuesRequest {
	return &clearValuesRequest{
		manager:       manager,
		ranges:        ranges,
		spreadsheetID: spreadsheetID,
	}
}

func (r *clearValuesRequest) Do() int {
	r.manager.refreshToken()
	clearRequest := &sheets.ClearValuesRequest{}
	req := r.manager.service.Spreadsheets.Values.Clear(r.spreadsheetID, r.ranges.String(), clearRequest)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	resp, err := req.Do()
	if err != nil {
		fmt.Println(err.Error())
		return 400
	}
	return resp.HTTPStatusCode
}
