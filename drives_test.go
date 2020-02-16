package main

import (
	"fmt"
	"testing"
)

func TestListDrives(t *testing.T) {
	fmt.Println(ListSharedDrives())
}
