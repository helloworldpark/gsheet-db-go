package main

import (
	"fmt"
	"testing"
)

func TestListDriveFiles(t *testing.T) {
	DriveServiceFromFile("credentials.json")
	fmt.Println(ListMyDriveFiles(nil))
}

func TestListDrives(t *testing.T) {
	service := DriveServiceFromFile("../ticklemetadrive.json")
	fmt.Println("Drives", service)
	fmt.Println(ListMyDriveFiles(service))
}

func TestListSheets(t *testing.T) {
	service := SheetServiceFromFile("/Users/shp/Documents/projects/ticklemeta-20200216.json")
	fmt.Println("Sheet", service)
}
