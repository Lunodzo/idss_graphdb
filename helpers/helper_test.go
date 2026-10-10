package helpers

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestDelimitedMessagePreservesAdjacentFrames(t *testing.T) {
	var stream bytes.Buffer
	first := strings.Repeat("row,value,", 700)
	second := []byte("second frame")
	if err := WriteDelimitedMessage(&stream, []byte(first)); err != nil {
		t.Fatal(err)
	}
	if err := WriteDelimitedMessage(&stream, second); err != nil {
		t.Fatal(err)
	}
	gotFirst, err := ReadDelimitedMessage(&stream, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gotSecond, err := ReadDelimitedMessage(&stream, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(gotFirst) != first || string(gotSecond) != string(second) {
		t.Fatal("adjacent framed messages were not preserved")
	}
}
