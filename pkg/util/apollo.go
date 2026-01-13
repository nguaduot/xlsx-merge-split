package util

import (
	"fmt"
	"path/filepath"
	"regexp"
	"time"
)

// ParseApollo14633Name
// ~\14633_14633-0_2026-01-09T08_10_25.029_wb_shijiankui.xlsx
// ~\14633_14633-1_2026-01-09T08_10_25.023_wb_shijiankui.xlsx
// => ~\14633-0108.xlsx
func ParseApollo14633Name(srcPaths []string) (string, error) {
	re := regexp.MustCompile(`^14633_14633-\d_(\d{4}-\d{2}-\d{2})T.+\.xlsx$`)
	var date time.Time
	for _, file := range srcPaths {
		m := re.FindStringSubmatch(filepath.Base(file))
		if m == nil {
			return "", nil
		}
		t, err := time.Parse("2006-01-02", m[1])
		if err != nil {
			return "", err
		}
		if date.IsZero() {
			date = t
		}
		if t != date {
			return "", nil
		}
	}
	if date.IsZero() {
		return "", nil
	}
	date = date.AddDate(0, 0, -1)
	return filepath.Join(filepath.Dir(srcPaths[0]), fmt.Sprintf("14633-%s", date.Format("0102"))), nil
}
