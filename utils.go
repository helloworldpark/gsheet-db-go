package main

import "fmt"

const leftmostCol = "A"
const rightmostCol = "D"
const defaultRange = "A1D3"

func base26(x int64) string {
	if x < 0 {
		panic(fmt.Sprintf("x should not be negative: %d", x))
	}
	if x > 26 {
		panic(fmt.Sprintf("Unsupported number %d", x))
	}
	return string('@' + x)
}

// cellRange Leftmost: from 0, Upmost: from 0
// In excel style, Row 0, Column 0 is A1
// A <-> 0
// Z <-> 25
// endRow, endCol is not included in the range
type cellRange struct {
	sheetName                          string
	startRow, endRow, startCol, endCol int64
}

func newCellRange(sheetName string, startRow, startCol, endRow, endCol int64) cellRange {
	c := cellRange{
		startRow:  startRow,
		endRow:    endRow,
		startCol:  startCol,
		endCol:    endCol,
		sheetName: sheetName,
	}
	if (c.startCol < 0 || c.startCol >= c.endCol) && (c.startRow < 0 || c.startRow >= c.endRow) {
		err := fmt.Sprintf("Invalid cellRange: %+v", c)
		panic(err)
	}
	return c
}

func (c cellRange) String() string {

	leftmost := base26(c.startCol + 1)
	rightmost := base26(c.endCol + 1)

	fmt.Printf("Left(%s, %d) Right(%s, %d)\n", leftmost, c.startCol, rightmost, c.endCol)

	ranges := fmt.Sprintf("%s%d:%s%d", leftmost, c.startRow+1, rightmost, c.endRow+1)
	ranges = fmt.Sprintf("%s!%s", c.sheetName, ranges)
	return ranges
}

func mininum64(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func maximum64(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}
