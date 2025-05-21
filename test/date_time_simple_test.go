/* Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package test

import (
	"regexp"
	"testing"
	"time"
)

func TestRegexMatching(t *testing.T) {
	// Test date regex
	dateRegex := `^\d{4}-\d{2}-\d{2}$`
	dateStr := "2023-05-15"

	matched, _ := regexp.MatchString(dateRegex, dateStr)
	if !matched {
		t.Errorf("Expected date string '%s' to match regex '%s'", dateStr, dateRegex)
	}

	// Test time regex
	timeRegex := `^\d{2}:\d{2}:\d{2}$`
	timeStr := "14:30:00"

	matched, _ = regexp.MatchString(timeRegex, timeStr)
	if !matched {
		t.Errorf("Expected time string '%s' to match regex '%s'", timeStr, timeRegex)
	}
}

func TestDateParsing(t *testing.T) {
	dateStr := "2023-05-15"
	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		t.Fatalf("Failed to parse date: %v", err)
	}

	expectedDate := time.Date(2023, 5, 15, 0, 0, 0, 0, time.UTC)
	if !date.Equal(expectedDate) {
		t.Errorf("Expected date %v but got %v", expectedDate, date)
	}
}

func TestTimeParsing(t *testing.T) {
	timeStr := "14:30:00"
	timeVal, err := time.Parse("15:04:05", timeStr)
	if err != nil {
		t.Fatalf("Failed to parse time: %v", err)
	}

	if timeVal.Hour() != 14 || timeVal.Minute() != 30 || timeVal.Second() != 0 {
		t.Errorf("Expected time 14:30:00 but got %d:%d:%d",
			timeVal.Hour(), timeVal.Minute(), timeVal.Second())
	}
}
