package csv

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitee.com/nguaduot/split-xlsx-go/pkg/util"
	"github.com/fatih/color"
	"github.com/xuri/excelize/v2"
)

func getRows(file string) (int, error) {
	f, err := excelize.OpenFile(file, excelize.Options{
		UnzipSizeLimit:    8 << 30, // 8GB
		UnzipXMLSizeLimit: 4 << 30, // 4GB
	})
	if err != nil {
		return 0, err
	}
	defer f.Close()
	rows, err := f.Rows(f.GetSheetName(0))
	if err != nil {
		return 0, err
	}
	count := 0
	for rows.Next() {
		count++
	}
	if count > 0 { // 减去行首
		count--
	}
	return count, nil
}

// MergeXlsx2csv
// Excel 本身是 zip + XML，磁盘和解压是瓶颈，并发通常收益不大，因此不采用并发读
func MergeXlsx2csv(srcPaths []string, tarPath string, ctx context.Context) error {
	start := time.Now()
	fmt.Println("正在解析…")

	// 获取文件大小，用于估算进度
	srcSizes := make([]int64, len(srcPaths))
	for i, file := range srcPaths {
		f, err := os.Stat(file)
		if err != nil {
			return err
		}
		srcSizes[i] = f.Size()
		fmt.Printf("数据文件%d：%s，%s\n", i+1, color.HiYellowString(filepath.Base(file)), util.SizeReadable(srcSizes[i]))
	}

	tarFile, err := os.Create(tarPath)
	if err != nil {
		return err
	}
	// Go 全局默认 UTF-8，写 UTF-8 BOM，确保 Windows Excel 能正常打开
	tarFile.Write([]byte{0xEF, 0xBB, 0xBF})

	// 不直接使用 writer := csv.NewWriter(tarFile)
	// 使用 bufio.Writer 减少 syscall
	bufWriter := bufio.NewWriterSize(tarFile, 1<<20)
	writer := csv.NewWriter(bufWriter)
	// writer.Comma = '\t' // 默认为 ,
	// writer.UseCRLF = true // 默认为 LF

	fmt.Printf("正在合并… %s\n", color.HiBlackString("(停止：Ctrl+C)"))
	wroteHeader := false
	totalRows := 0
	for i, file := range srcPaths {
		select {
		case <-ctx.Done():
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			return ctx.Err()
		default:
		} // 响应 Ctrl+C 打断
		f, err := excelize.OpenFile(file, excelize.Options{
			UnzipSizeLimit:    8 << 30, // 8GB
			UnzipXMLSizeLimit: 4 << 30, // 4GB
		})
		if err != nil {
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			return err
		}
		// defer f.Close() // 循环中不使用该方法
		sheet := f.GetSheetName(0) // 只读第一张表
		iter, err := f.Rows(sheet) // 流式读取（不会一次性加载整表）
		if err != nil {
			f.Close()
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			return err
		}
		fileRows := 0
		for iter.Next() {
			select {
			case <-ctx.Done():
				f.Close()
				writer.Flush()
				bufWriter.Flush()
				tarFile.Close()
				return ctx.Err()
			default:
			} // 响应 Ctrl+C 打断
			fileRows++
			totalRows++
			row, err := iter.Columns()
			if err != nil {
				f.Close()
				writer.Flush()
				bufWriter.Flush()
				tarFile.Close()
				return err
			}
			if fileRows == 1 { // 控制只写一次行首
				if wroteHeader {
					continue
				}
				wroteHeader = true
			}
			if err = writer.Write(row); err != nil {
				f.Close()
				writer.Flush()
				bufWriter.Flush()
				tarFile.Close()
				return err
			}
			if totalRows-i-1 > 0 && (totalRows-i-1)%10000 == 0 {
				if i > 0 {
					fmt.Printf("数据文件%d：已读取%d行；累计合并%d行，耗时%s\n", i+1, fileRows-1, totalRows-i-1, util.Cost(start))
				} else {
					fmt.Printf("数据文件%d：已读取%d行；累计耗时%s\n", i+1, fileRows-1, util.Cost(start))
				}
			}
		}
		f.Close()
		sizeDone, sizeTodo := int64(0), int64(0)
		for j := range srcSizes {
			if j <= i {
				sizeDone += srcSizes[j]
			} else {
				sizeTodo += srcSizes[j]
			}
		}
		if sizeTodo > 0 {
			fmt.Printf("数据文件%d：读取完成，共%s；预计剩余%s\n", i+1, color.HiYellowString("%d行", fileRows-1),
				util.CostReadable(float64(sizeTodo)/float64(sizeDone)*time.Since(start).Seconds()))
		} else {
			fmt.Printf("数据文件%d：读取完成，共%s\n", i+1, color.HiYellowString("%d行", fileRows-1))
		}
	}
	writer.Flush()
	bufWriter.Flush()
	tarFile.Close()
	info, err := os.Stat(tarPath)
	if err != nil {
		return err
	}
	fmt.Printf("合并完成，%s，共%s数据，耗时%s\n",
		util.SizeReadable(info.Size()), color.HiYellowString("%d行", totalRows-len(srcPaths)), util.Cost(start))
	fmt.Printf("合并文件：%s%s\n", strings.TrimSuffix(tarPath, filepath.Base(tarPath)),
		color.HiYellowString(filepath.Base(tarPath)))
	return nil
}

func SplitXlsx2csvByLine(srcPath string, tarDir string, lineCount int, ctx context.Context) error {
	start := time.Now()
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	fmt.Printf("数据文件：%s，%s\n", color.HiYellowString(filepath.Base(srcPath)), util.SizeReadable(info.Size()))

	fmt.Printf("正在按每%s拆分… %s\n", color.HiYellowString("%d行", lineCount), color.HiBlackString("(停止：Ctrl+C)"))
	srcFile, err := excelize.OpenFile(srcPath, excelize.Options{
		UnzipSizeLimit:    8 << 30, // 8GB
		UnzipXMLSizeLimit: 4 << 30, // 4GB
	})
	if err != nil {
		return err
	}
	srcSheet := srcFile.GetSheetName(0)
	iter, err := srcFile.Rows(srcSheet)
	iter.Next()
	rowHeader, err := iter.Columns()
	if err != nil {
		srcFile.Close()
		return err
	}
	var (
		tarFile    *os.File
		bufWriter  *bufio.Writer
		writer     *csv.Writer
		tarPath    string
		tarPathIdx int
		totalRows  int
		fileRows   int
	)
	for iter.Next() {
		if totalRows%lineCount == 0 {
			if tarPathIdx > 0 {
				writer.Flush()
				bufWriter.Flush()
				tarFile.Close()
				fmt.Printf("数据文件%d：写入完成，共%d行\n", tarPathIdx, fileRows)
			}
			tarPathIdx++
			tarPath = filepath.Join(tarDir, fmt.Sprintf("%s-%d.csv", filepath.Base(tarDir), tarPathIdx))
			tarFile, err = os.Create(tarPath)
			if err != nil {
				return err
			}
			// Go 全局默认 UTF-8，写 UTF-8 BOM，确保 Windows Excel 能正常打开
			tarFile.Write([]byte{0xEF, 0xBB, 0xBF})
			bufWriter = bufio.NewWriterSize(tarFile, 1<<20)
			writer = csv.NewWriter(bufWriter)
			if err = writer.Write(rowHeader); err != nil {
				writer.Flush()
				bufWriter.Flush()
				tarFile.Close()
				srcFile.Close()
				return err
			}
			fileRows = 0
		}
		select {
		case <-ctx.Done():
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			srcFile.Close()
			return ctx.Err()
		default:
		} // 响应 Ctrl+C 打断
		totalRows++
		fileRows++
		row, err := iter.Columns()
		if err != nil {
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			srcFile.Close()
			return err
		}
		if err = writer.Write(row); err != nil {
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			srcFile.Close()
			return err
		}
		if totalRows%10000 == 0 {
			if tarPathIdx > 1 {
				fmt.Printf("数据文件%d：已写入%d行；累计拆分%d行，耗时%s\n", tarPathIdx, fileRows, totalRows, util.Cost(start))
			} else {
				fmt.Printf("数据文件%d：已写入%d行；累计耗时%s\n", tarPathIdx, fileRows, util.Cost(start))
			}
		}
	}
	if tarPathIdx > 0 {
		writer.Flush()
		bufWriter.Flush()
		tarFile.Close()
		fmt.Printf("数据文件%d：写入完成，共%d行\n", tarPathIdx, fileRows)
	}
	srcFile.Close()
	fmt.Printf("拆分完成，共%s，分为%s文件，耗时%s\n",
		color.HiYellowString("%d行", totalRows), color.HiYellowString("%d个", tarPathIdx), util.Cost(start))
	fmt.Printf("拆分文件夹：%s%s\n", strings.TrimSuffix(tarDir, filepath.Base(tarDir)),
		color.HiYellowString(filepath.Base(tarDir)))
	return nil
}

func SplitXlsx2csvByFile(srcPath string, tarDir string, fileCount int, ctx context.Context) error {
	start := time.Now()
	fmt.Println("正在解析…")

	srcRows, err := getRows(srcPath)
	if err != nil {
		return err
	}
	if srcRows < fileCount {
		return fmt.Errorf("数据行数（%d）小于拆分文件数（%d），无法拆分", srcRows, fileCount)
	}
	lineCount := int(math.Ceil(float64(srcRows) / float64(fileCount)))
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	fmt.Printf("数据文件：%s，%s，%d行\n",
		color.HiYellowString(filepath.Base(srcPath)), util.SizeReadable(info.Size()), srcRows)

	fmt.Printf("正在拆分为%s文件… %s\n", color.HiYellowString("%d个", fileCount), color.HiBlackString("(停止：Ctrl+C)"))
	srcFile, err := excelize.OpenFile(srcPath, excelize.Options{
		UnzipSizeLimit:    8 << 30, // 8GB
		UnzipXMLSizeLimit: 4 << 30, // 4GB
	})
	if err != nil {
		return err
	}
	srcSheet := srcFile.GetSheetName(0)
	iter, err := srcFile.Rows(srcSheet)
	iter.Next()
	rowHeader, err := iter.Columns()
	if err != nil {
		srcFile.Close()
		return err
	}
	var (
		tarFile    *os.File
		bufWriter  *bufio.Writer
		writer     *csv.Writer
		tarPath    string
		tarPathIdx int
		totalRows  int
		fileRows   int
		startFile  time.Time
	)
	for iter.Next() {
		if totalRows%lineCount == 0 {
			if tarPathIdx > 0 {
				writer.Flush()
				bufWriter.Flush()
				tarFile.Close()
				fmt.Printf("数据文件%d：写入完成，共%s；预计剩余%s\n", tarPathIdx, color.HiYellowString("%d行", fileRows),
					util.CostReadable(time.Since(startFile).Seconds()*float64(fileCount-tarPathIdx)))
			}
			startFile = time.Now()
			tarPathIdx++
			nameFmt := fmt.Sprintf("%%s-%%0%dd.csv", len(strconv.Itoa(fileCount)))
			tarPath = filepath.Join(tarDir, fmt.Sprintf(nameFmt, filepath.Base(tarDir), tarPathIdx))
			tarFile, err = os.Create(tarPath)
			if err != nil {
				return err
			}
			// Go 全局默认 UTF-8，写 UTF-8 BOM，确保 Windows Excel 能正常打开
			tarFile.Write([]byte{0xEF, 0xBB, 0xBF})
			bufWriter = bufio.NewWriterSize(tarFile, 1<<20)
			writer = csv.NewWriter(bufWriter)
			if err = writer.Write(rowHeader); err != nil {
				writer.Flush()
				bufWriter.Flush()
				tarFile.Close()
				srcFile.Close()
				return err
			}
			fileRows = 0
		}
		select {
		case <-ctx.Done():
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			srcFile.Close()
			return ctx.Err()
		default:
		} // 响应 Ctrl+C 打断
		totalRows++
		fileRows++
		row, err := iter.Columns()
		if err != nil {
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			srcFile.Close()
			return err
		}
		if err = writer.Write(row); err != nil {
			writer.Flush()
			bufWriter.Flush()
			tarFile.Close()
			srcFile.Close()
			return err
		}
		if totalRows%10000 == 0 {
			if tarPathIdx > 1 {
				fmt.Printf("数据文件%d：已写入%d行；累计拆分%d行，耗时%s\n", tarPathIdx, fileRows, totalRows, util.Cost(start))
			} else {
				fmt.Printf("数据文件%d：已写入%d行；累计耗时%s\n", tarPathIdx, fileRows, util.Cost(start))
			}
		}
	}
	if tarPathIdx > 0 {
		writer.Flush()
		bufWriter.Flush()
		tarFile.Close()
		fmt.Printf("数据文件%d：写入完成，共%s\n", tarPathIdx, color.HiYellowString("%d行", fileRows))
	}
	srcFile.Close()
	fmt.Printf("拆分完成，%d个文件，每个%s，耗时%s\n",
		tarPathIdx, color.HiYellowString("%d行", lineCount), util.Cost(start))
	fmt.Printf("拆分文件夹：%s%s\n", strings.TrimSuffix(tarDir, filepath.Base(tarDir)),
		color.HiYellowString(filepath.Base(tarDir)))
	return nil
}
