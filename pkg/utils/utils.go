package utils

import (
	"fmt"
	"time"
)

// GenerateID Helper function to generate unique IDs
func GenerateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
