package main

import (
	"os"
	"testing"
)

func TestDatabase_init(t *testing.T) {
	DB_PATH := "test_db"
	SAMPLE_DATA := "sample_data.json"
	Database_init(SAMPLE_DATA)

	// Check if the database is created
	if _, err := os.Stat(DB_PATH); os.IsNotExist(err) {
		t.Errorf("Database not created")
	}

	os.RemoveAll(DB_PATH)
}