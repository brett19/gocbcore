package gocbcore

import (
	"fmt"
	"testing"
)

func TestAppendUleb128_32(t *testing.T) {
	bytes := make([]byte, 5)
	encoded := appendUleb128_32(bytes, 9)
	fmt.Print(encoded)
}
