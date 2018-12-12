package parser

import (
	"fmt"
	"time"
)

// DateLongFormat is same as time.Time#String(), except this does not include monotonic time section
const DateLongFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

// DateShortFormat is a short format
const DateShortFormat = "2006-Jan-02"

const DateNumberFormat = "2006-01-02"

// ParseDate intends to parse all SQL date format
func ParseDate(data string) (*time.Time, error) {
	t, err := time.Parse(DateLongFormat, data)
	if err == nil {
		return &t, nil
	}

	t, err = time.Parse(time.RFC3339, data)
	if err == nil {
		return &t, nil
	}

	t, err = time.Parse(DateShortFormat, data)
	if err == nil {
		return &t, nil
	}

	t, err = time.Parse(DateNumberFormat, data)
	if err == nil {
		return &t, nil
	}

	return nil, fmt.Errorf("not a date")
}
