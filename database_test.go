package gosheet

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestMiniServer(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	for i := 0; i < 100; i++ {
		rand.Seed(time.Now().Unix())
		usage := rand.Int63n(5) + 1
		fmt.Println(i, "Usage: ", usage)
		manager.enqueueAPIUsage(usage, true)
	}
}
