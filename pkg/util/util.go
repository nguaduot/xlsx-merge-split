package util

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
)

func InitLog(outPath string) (func(), error) {
	if outPath == "" {
		return nil, nil
	}
	ext := filepath.Ext(outPath)
	logPath := strings.TrimSuffix(outPath, ext) + ".log"
	file, err := os.OpenFile(
		logPath,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	log.SetOutput(file)
	log.SetFlags(log.LstdFlags) // Ldate | Ltime
	return func() {
		file.Sync() // 同步缓冲区到磁盘
		file.Close()
		info, err := os.Stat(logPath)
		if err != nil {
			return
		}
		if info.Size() == 0 {
			_ = os.Remove(logPath)
		}
	}, nil
}

func WaitForExit() {
	// var m runtime.MemStats
	// runtime.ReadMemStats(&m)
	// fmt.Printf("HeapAlloc=%.2fMB\n", float64(m.HeapAlloc)/1024/1024)
	// fmt.Printf("HeapSys=%.2fMB\n", float64(m.HeapSys)/1024/1024)
	// fmt.Printf("Sys=%.2fMB\n", float64(m.Sys)/1024/1024)

	fmt.Print(color.HiBlackString("程序已中止，按回车键关闭…"))
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

func SizeReadable(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%dByte", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.1fMB", float64(bytes)/1024/1024)
	}
	return fmt.Sprintf("%.2fGB", float64(bytes)/1024/1024/1024)
}

func CostReadable(sec float64) string {
	sec = math.Ceil(sec)
	if sec < 60 {
		return fmt.Sprintf("%.0f秒", sec)
	}
	if sec < 60*60 {
		m := int(sec / 60)
		s := sec - float64(m)*60
		if s < 0.5 {
			return fmt.Sprintf("%d分钟", m)
		}
		return fmt.Sprintf("%d分%.0f秒", m, s)
	}
	h := int(sec / 60 / 60)
	m := (sec - float64(h)*60*60) / 60
	if m < 0.5 {
		return fmt.Sprintf("%d小时", h)
	}
	return fmt.Sprintf("%d时%.0f分", h, m)
}

func Cost(start time.Time) string {
	sec := time.Since(start).Seconds()
	return CostReadable(sec)
}

func RelativePath2Abs(path string) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Clean(filepath.Join(filepath.Dir(exe), path)), nil
}

func IsExcelFile(file string) bool {
	if file == "" {
		return false
	}
	if !strings.HasSuffix(strings.ToLower(file), ".xlsx") {
		return false
	}
	info, err := os.Stat(file)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	return err
}

func XlsxColIndexFromAxis(axis string) int {
	col := strings.TrimRightFunc(axis, func(r rune) bool {
		return r >= '0' && r <= '9'
	})
	result := 0
	for _, c := range col {
		result = result*26 + int(c-'A'+1)
	}
	return result
}

func IsFile(path string) (bool, error) {
	if path == "" {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !info.IsDir(), nil
}

func IsDir(path string) (bool, error) {
	if path == "" {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func IsDirAndNotEmpty(path string) (bool, error) {
	if path == "" {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if !info.IsDir() {
		return false, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	return len(entries) > 0, nil
}
