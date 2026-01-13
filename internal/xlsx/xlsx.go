package xlsx

import (
	"archive/zip"
	"bytes"
	"context"
	_ "embed"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitee.com/nguaduot/split-xlsx-go/pkg/util"
	"github.com/fatih/color"
	"github.com/xuri/excelize/v2"
)

//go:embed template.xlsx
var templateXlsx []byte

type CellMeta struct {
	StyleId int
	TypeIdx excelize.CellType
	TypeRaw string
}

func cellTypeRaw2Idx(t string) excelize.CellType {
	switch t {
	case "s":
		return excelize.CellTypeSharedString // 7
	case "n":
		return excelize.CellTypeNumber // 6
	case "inlineStr":
		return excelize.CellTypeInlineString // 5
	case "str":
		return excelize.CellTypeFormula // 4
	case "e":
		return excelize.CellTypeError // 3
	case "d":
		return excelize.CellTypeDate // 2
	case "b":
		return excelize.CellTypeBool // 1
	default:
		return excelize.CellTypeUnset // 0
	}
}

func cellTypeIdx2Raw(t excelize.CellType) string {
	switch t {
	case excelize.CellTypeSharedString:
		return "s"
	case excelize.CellTypeNumber:
		return "n"
	case excelize.CellTypeInlineString:
		return "inlineStr"
	case excelize.CellTypeFormula:
		return "str"
	case excelize.CellTypeError:
		return "e"
	case excelize.CellTypeDate:
		return "d"
	case excelize.CellTypeBool:
		return "b"
	default: // excelize.CellTypeUnset
		return ""
	}
}

// readXlsxStyleAndType
// 关于 excelize file.GetCellStyle() file.GetCellType()
// 均需加载完整样式数据，大表内存爆炸
func readXlsxStyleAndType(file string) (map[int]CellMeta, error) {
	// f, err := excelize.OpenFile(file, excelize.Options{
	// 	UnzipSizeLimit:    8 << 30, // 8GB
	// 	UnzipXMLSizeLimit: 4 << 30, // 4GB
	// })
	// if err != nil {
	// 	return nil, nil, err
	// }
	// sheetName := f.GetSheetName(0)
	// iter, err := f.Rows(sheetName)
	// iter.Next()
	// row, err := iter.Columns()
	// if err != nil {
	// 	f.Close()
	// 	return nil, nil, err
	// }
	// f.Close()
	// cols := len(row) // 列数

	r, err := zip.OpenReader(file)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var sheetReader io.ReadCloser
	sheetPath := "xl/worksheets/sheet1.xml"
	for _, f := range r.File {
		if f.Name == sheetPath {
			sheetReader, err = f.Open()
			if err != nil {
				return nil, err
			}
			break
		}
	}
	if sheetReader == nil {
		return nil, fmt.Errorf("文件不存在：%s", sheetPath)
	}
	defer sheetReader.Close()
	decoder := xml.NewDecoder(sheetReader)
	res := make(map[int]CellMeta)
	inTargetRow := false
	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch se := tok.(type) {
		case xml.StartElement:
			// <row r="2">
			if se.Name.Local == "row" {
				for _, a := range se.Attr {
					if a.Name.Local == "r" {
						r, _ := strconv.Atoi(a.Value)
						if r == 2 {
							inTargetRow = true
						}
					}
				}
			}
			// <c r="B2" s="5" t="s">
			if inTargetRow && se.Name.Local == "c" {
				var (
					axis    string
					styleId string
					typeRaw string
				)
				for _, a := range se.Attr {
					switch a.Name.Local {
					case "r":
						axis = a.Value
					case "s":
						styleId = a.Value
					case "t":
						typeRaw = a.Value
					}
				}
				if axis != "" {
					col := util.XlsxColIndexFromAxis(axis)
					styleIdFix := 0
					if styleId != "" {
						styleIdFix, err = strconv.Atoi(styleId)
						if err != nil {
							return nil, err
						}
					}
					res[col] = CellMeta{
						StyleId: styleIdFix,
						TypeIdx: cellTypeRaw2Idx(typeRaw),
						TypeRaw: typeRaw,
					}
				}
			}
		case xml.EndElement:
			// </row>，目标行结束，立刻退出
			if inTargetRow && se.Name.Local == "row" {
				return res, nil
			}
		}
	}
	return res, nil
}

// getRows()
// 不要读取 dimension 信息来获取行数，通过程序生成的表格文件可能并不包含该信息
func getRows(file string) (int, error) {
	// f, err := excelize.OpenFile(file)
	// if err != nil {
	// 	return 0, err
	// }
	// defer f.Close()
	// dimension, err := f.GetSheetDimension(f.GetSheetName(0))
	// if err != nil {
	// 	return 0, err
	// }
	// fmt.Println(dimension) // A1:B100
	// count, err := strconv.Atoi(regexp.MustCompile(`(\d+)$`).FindString(dimension))
	// if err != nil {
	// 	return 0, err
	// }
	// return count, nil

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

func CalcRows(files []string) (int, error) {
	// totalRows := 0
	// for i, file := range files {
	// 	start := time.Now()
	// 	rows, err := getRows(file)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	totalRows += rows
	// 	fmt.Printf("%s：%d行，耗时%s\n", filepath.Base(f), rows, util.Cost(start))
	// }
	// return totalRows, nil

	fmt.Println("正在统计行数…")
	var (
		totalRows int
		mu        sync.Mutex
		wg        sync.WaitGroup
		errors    []error
		errMu     sync.Mutex
	)
	for _, file := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			start := time.Now()
			rows, err := getRows(file)
			if err != nil {
				errMu.Lock()
				errors = append(errors, err)
				fmt.Printf("%s：异常\n", filepath.Base(f))
				errMu.Unlock()
				return
			}
			mu.Lock()
			totalRows += rows
			fmt.Printf("%s：%d行，耗时%s\n", filepath.Base(f), rows, util.Cost(start))
			mu.Unlock()
		}(file)
	}
	wg.Wait()
	if len(errors) > 0 {
		return 0, errors[0]
	}
	return totalRows, nil
}

// MergeXlsx2xlsxV1
// 适用于小文件
func MergeXlsx2xlsxV1(srcPaths []string, tarPath string, ctx context.Context) error {
	start := time.Now()
	fmt.Println("正在解析…")

	sort.Slice(srcPaths, func(i, j int) bool {
		fi, _ := os.Stat(srcPaths[i])
		fj, _ := os.Stat(srcPaths[j])
		return fi.Size() > fj.Size()
	}) // 大小降序
	for i, file := range srcPaths {
		f, err := os.Stat(file)
		if err != nil {
			return err
		}
		fmt.Printf("数据文件%d：%s，%s\n", i+1, color.HiYellowString(filepath.Base(file)), util.SizeReadable(f.Size()))
	}

	// 选取最大文件作为基础文件
	if err := util.CopyFile(srcPaths[0], tarPath); err != nil {
		return err
	}
	tarFile, err := excelize.OpenFile(tarPath, excelize.Options{
		UnzipSizeLimit:    8 << 30, // 8GB
		UnzipXMLSizeLimit: 4 << 30, // 4GB
	})
	if err != nil {
		return err
	}

	tarSheet := tarFile.GetSheetName(0)
	iter, err := tarFile.Rows(tarSheet)
	totalRows := 0
	for iter.Next() {
		totalRows++
		if totalRows-1 > 0 && (totalRows-1)%10000 == 0 {
			fmt.Printf("数据文件1：已读取%d行；累计耗时%s\n", totalRows-1, util.Cost(start))
		}
	}
	fmt.Printf("数据文件1：读取完成，共%s\n", color.HiYellowString("%d行", totalRows-1))

	for i, file := range srcPaths {
		select {
		case <-ctx.Done():
			tarFile.Close()
			return ctx.Err()
		default:
		} // 响应 Ctrl+C 打断
		if i == 0 { // 跳过基础文件
			continue
		}
		f, err := excelize.OpenFile(file, excelize.Options{
			UnzipSizeLimit:    8 << 30, // 8GB
			UnzipXMLSizeLimit: 4 << 30, // 4GB
		})
		if err != nil {
			tarFile.Close()
			return err
		}
		// defer f.Close() // 循环中不使用该方法
		sheet := f.GetSheetName(0) // 只读第一张表
		iter, err := f.Rows(sheet) // 流式读取（不会一次性加载整表）
		if err != nil {
			f.Close()
			tarFile.Close()
			return err
		}
		fileRows := 0
		for iter.Next() {
			select {
			case <-ctx.Done():
				f.Close()
				tarFile.Close()
				return ctx.Err()
			default:
			} // 响应 Ctrl+C 打断
			fileRows++
			totalRows++
			if fileRows == 1 { // 跳过行首
				continue
			}
			row, err := iter.Columns()
			if err != nil {
				f.Close()
				tarFile.Close()
				return err
			}
			for j := range row {
				colName, err := excelize.ColumnNumberToName(j + 1)
				if err != nil {
					f.Close()
					tarFile.Close()
					return err
				}
				srcAxis := fmt.Sprintf("%s%d", colName, fileRows)
				tarAxis := fmt.Sprintf("%s%d", colName, totalRows-i)
				styleId, err := tarFile.GetCellStyle(tarSheet, srcAxis)
				if err != nil {
					f.Close()
					tarFile.Close()
					return err
				}
				tarFile.SetCellStyle(tarSheet, tarAxis, tarAxis, styleId)
				val, err := f.GetCellValue(sheet, srcAxis, excelize.Options{
					RawCellValue: true,
				})
				if err != nil {
					f.Close()
					tarFile.Close()
					return err
				}
				if val == "" { // 无值
					continue
				}
				cellType, err := tarFile.GetCellType(tarSheet, srcAxis)
				if err != nil {
					f.Close()
					tarFile.Close()
					return err
				}
				switch cellType {
				case excelize.CellTypeNumber, excelize.CellTypeUnset:
					valFix, err := strconv.ParseFloat(val, 64)
					if err != nil {
						f.Close()
						tarFile.Close()
						return err
					}
					tarFile.SetCellValue(tarSheet, tarAxis, valFix)
				case excelize.CellTypeInlineString, excelize.CellTypeSharedString:
					tarFile.SetCellValue(tarSheet, tarAxis, val)
				default:
					f.Close()
					tarFile.Close()
					return fmt.Errorf("%s：位置 %s，值 %s，未支持的数据类型 %s", filepath.Base(file), srcAxis, val, cellTypeIdx2Raw(cellType))
				}
			}
			if (totalRows-i-1)%10000 == 0 {
				fmt.Printf("数据文件%d：已读取%d行；累计合并%d行，耗时%s\n", i+1, fileRows-1, totalRows-i-1, util.Cost(start))
			}
		}
		f.Close()
		fmt.Printf("数据文件%d：读取完成，共%s\n", i+1, color.HiYellowString("%d行", fileRows-1))
	}
	if err := tarFile.Save(); err != nil {
		tarFile.Close()
		return err
	}
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

// MergeXlsx2xlsxV2
// 适用于大文件
func MergeXlsx2xlsxV2(srcPaths []string, tarPath string, ctx context.Context) error {
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
	}

	// 解析数据格式
	var meta map[int]CellMeta
	for i, file := range srcPaths {
		m, err := readXlsxStyleAndType(file)
		if err != nil {
			return err
		}
		var msg strings.Builder
		for j := range len(m) {
			col, err := excelize.ColumnNumberToName(j + 1)
			if err != nil {
				return err
			}
			fmt.Fprintf(&msg, "%s列 样式 %d 类型 %s，", col, m[j].StyleId, m[j].TypeRaw)
		}
		log.Printf("%s：数据格式 %s", filepath.Base(file), strings.TrimSuffix(msg.String(), "，"))
		fmt.Printf("数据文件%d：%s，%s，%d列\n",
			i+1, color.HiYellowString(filepath.Base(file)), util.SizeReadable(srcSizes[i]), len(m))
		if i == 0 {
			meta = m
			continue
		}
		if len(m) != len(meta) {
			return fmt.Errorf("列数不一致：%s（%d列），%s（%d）列",
				filepath.Base(srcPaths[0]), len(meta), filepath.Base(file), len(m))
		}
		for k, v := range m {
			if v.StyleId != meta[k].StyleId || v.TypeIdx != meta[k].TypeIdx {
				col, err := excelize.ColumnNumberToName(k)
				if err != nil {
					return err
				}
				return fmt.Errorf("%s列数据格式不一致：%s（样式 %d 类型 %s），%s（样式 %d 类型 %s）",
					col, filepath.Base(srcPaths[0]), meta[k].StyleId, meta[k].TypeRaw,
					filepath.Base(file), v.StyleId, v.TypeRaw)
			}
		}
	}
	for k, v := range meta { // 日志输出不支持的 CellTyle
		if v.TypeIdx != excelize.CellTypeNumber && v.TypeIdx != excelize.CellTypeUnset &&
			v.TypeIdx != excelize.CellTypeInlineString && v.TypeIdx != excelize.CellTypeSharedString {
			col, err := excelize.ColumnNumberToName(k)
			if err != nil {
				return err
			}
			log.Printf("%s：%s列数据类型 %s 暂不支持", filepath.Base(srcPaths[0]), col, v.TypeRaw)
		}
	}

	// // 新建文件，使用 excelize 默认模板（字体为 Calibri）
	// tarFile := excelize.NewFile()
	// defer tarFile.Close()
	// tarSheet := "data"
	// tarFile.SetSheetName(tarFile.GetSheetName(0), tarSheet) // Sheet1 > data
	// sw, err := tarFile.NewStreamWriter(tarSheet)
	// if err != nil {
	// 	return err
	// }

	// 使用模板文件（来自 Excel 2016+ 创建的空文件）
	tarFile, err := excelize.OpenReader(bytes.NewReader(templateXlsx))
	if err != nil {
		return err
	}
	tarSheet := "data"
	sw, err := tarFile.NewStreamWriter(tarSheet) // 流式写入（不爆内存，注意始终从首行开始）
	if err != nil {
		return err
	}

	fmt.Printf("正在合并… %s\n", color.HiBlackString("(停止：Ctrl+C)"))
	wroteHeader := false
	totalRows := 0
	for i, file := range srcPaths {
		select {
		case <-ctx.Done():
			tarFile.Close()
			return ctx.Err()
		default:
		} // 响应 Ctrl+C 打断
		f, err := excelize.OpenFile(file, excelize.Options{
			UnzipSizeLimit:    8 << 30, // 8GB
			UnzipXMLSizeLimit: 4 << 30, // 4GB
		})
		if err != nil {
			tarFile.Close()
			return err
		}
		// defer f.Close() // 循环中不使用该方法
		sheet := f.GetSheetName(0) // 只读第一张表
		iter, err := f.Rows(sheet) // 流式读取（不会一次性加载整表）
		if err != nil {
			f.Close()
			tarFile.Close()
			return err
		}
		fileRows := 0
		for iter.Next() {
			select {
			case <-ctx.Done():
				f.Close()
				tarFile.Close()
				return ctx.Err()
			default:
			} // 响应 Ctrl+C 打断
			fileRows++
			totalRows++
			row, err := iter.Columns()
			if err != nil {
				f.Close()
				tarFile.Close()
				return err
			}
			rowNew := make([]any, len(row))
			if fileRows == 1 { // 控制只写一次行首
				if wroteHeader {
					continue
				}
				wroteHeader = true
				for c := range row { // 行首不检查 CellType
					rowNew[c] = excelize.Cell{
						StyleID: meta[c+1].StyleId,
						Value:   row[c],
					}
				}
			} else {
				for c := range row {
					cell := excelize.Cell{
						StyleID: meta[c+1].StyleId,
					}
					if row[c] == "" {
						cell.Value = nil
					} else if meta[c+1].TypeIdx == excelize.CellTypeNumber ||
						meta[c+1].TypeIdx == excelize.CellTypeUnset {
						valFix, err := strconv.ParseFloat(row[c], 64)
						if err == nil {
							cell.Value = valFix
						} else {
							cell.Value = row[c]
							col, err := excelize.ColumnNumberToName(c + 1)
							if err != nil {
								f.Close()
								tarFile.Close()
								return err
							}
							log.Printf("%s：位置 %s%d，数据类型 %s，异常数据类型值 %s",
								filepath.Base(file), col, fileRows, meta[c+1].TypeRaw, row[c])
						}
					} else { // excelize.CellTypeInlineString, excelize.CellTypeSharedString
						cell.Value = row[c]
					}
					rowNew[c] = cell
				}
			}
			axis := fmt.Sprintf("A%d", totalRows-i)
			if err := sw.SetRow(axis, rowNew); err != nil {
				f.Close()
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
	fmt.Println("正在生成…")
	sw.Flush()
	if err := tarFile.SaveAs(tarPath); err != nil {
		tarFile.Close()
		return err
	}
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

func SplitXlsx2xlsxByLine(srcPath string, tarDir string, lineCount int, ctx context.Context) error {
	start := time.Now()
	fmt.Println("正在解析…")

	// 解析数据格式
	meta, err := readXlsxStyleAndType(srcPath)
	if err != nil {
		return err
	}
	var msg strings.Builder
	for j := range len(meta) {
		col, err := excelize.ColumnNumberToName(j + 1)
		if err != nil {
			return err
		}
		fmt.Fprintf(&msg, "%s列 样式 %d 类型 %s，", col, meta[j].StyleId, meta[j].TypeRaw)
	}
	log.Printf("%s：数据格式 %s", filepath.Base(srcPath), strings.TrimSuffix(msg.String(), "，"))
	for k, v := range meta { // 日志输出不支持的 CellTyle
		if v.TypeIdx != excelize.CellTypeNumber && v.TypeIdx != excelize.CellTypeUnset &&
			v.TypeIdx != excelize.CellTypeInlineString && v.TypeIdx != excelize.CellTypeSharedString {
			col, err := excelize.ColumnNumberToName(k)
			if err != nil {
				return err
			}
			log.Printf("%s：%s列数据类型 %s 暂不支持", filepath.Base(srcPath), col, v.TypeRaw)
		}
	}
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	fmt.Printf("数据文件：%s，%s，%d列\n",
		color.HiYellowString(filepath.Base(srcPath)), util.SizeReadable(info.Size()), len(meta))

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
		tarFile    *excelize.File
		sw         *excelize.StreamWriter
		tarPath    string
		tarPathIdx int
		totalRows  int
		fileRows   int
	)
	for iter.Next() {
		if totalRows%lineCount == 0 {
			if tarPathIdx > 0 {
				sw.Flush()
				if err := tarFile.SaveAs(tarPath); err != nil {
					tarFile.Close()
					return err
				}
				tarFile.Close()
				fmt.Printf("数据文件%d：写入完成，共%d行\n", tarPathIdx, fileRows)
			}
			tarPathIdx++
			tarPath = filepath.Join(tarDir, fmt.Sprintf("%s-%d.xlsx", filepath.Base(tarDir), tarPathIdx))
			// 使用模板文件（来自 Excel 2016+ 创建的空文件）
			tarFile, err = excelize.OpenReader(bytes.NewReader(templateXlsx))
			if err != nil {
				srcFile.Close()
				return err
			}
			sw, err = tarFile.NewStreamWriter("data") // 流式写入（不爆内存，注意始终从首行开始）
			if err != nil {
				tarFile.Close()
				srcFile.Close()
				return err
			}
			rowNew := make([]any, len(rowHeader))
			for c := range rowHeader { // 行首不检查 CellType
				rowNew[c] = excelize.Cell{
					StyleID: meta[c+1].StyleId,
					Value:   rowHeader[c],
				}
			}
			if err := sw.SetRow("A1", rowNew); err != nil {
				tarFile.Close()
				srcFile.Close()
				return err
			}
			fileRows = 0
		}
		select {
		case <-ctx.Done():
			tarFile.Close()
			srcFile.Close()
			return ctx.Err()
		default:
		} // 响应 Ctrl+C 打断
		totalRows++
		fileRows++
		row, err := iter.Columns()
		if err != nil {
			tarFile.Close()
			srcFile.Close()
			return err
		}
		rowNew := make([]any, len(row))
		for c := range row {
			cell := excelize.Cell{
				StyleID: meta[c+1].StyleId,
			}
			if row[c] == "" {
				cell.Value = nil
			} else if meta[c+1].TypeIdx == excelize.CellTypeNumber ||
				meta[c+1].TypeIdx == excelize.CellTypeUnset {
				valFix, err := strconv.ParseFloat(row[c], 64)
				if err == nil {
					cell.Value = valFix
				} else {
					cell.Value = row[c]
					col, err := excelize.ColumnNumberToName(c + 1)
					if err != nil {
						tarFile.Close()
						srcFile.Close()
						return err
					}
					log.Printf("%s：位置 %s%d，数据类型 %s，异常数据类型值 %s",
						filepath.Base(srcPath), col, fileRows, meta[c+1].TypeRaw, row[c])
				}
			} else { // excelize.CellTypeInlineString, excelize.CellTypeSharedString
				cell.Value = row[c]
			}
			rowNew[c] = cell
		}
		axis := fmt.Sprintf("A%d", fileRows+1)
		if err := sw.SetRow(axis, rowNew); err != nil {
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
		sw.Flush()
		if err := tarFile.SaveAs(tarPath); err != nil {
			tarFile.Close()
			return err
		}
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

func SplitXlsx2xlsxByFile(srcPath string, tarDir string, fileCount int, ctx context.Context) error {
	start := time.Now()
	fmt.Println("正在解析…")

	// 解析数据格式
	meta, err := readXlsxStyleAndType(srcPath)
	if err != nil {
		return err
	}
	var msg strings.Builder
	for j := range len(meta) {
		col, err := excelize.ColumnNumberToName(j + 1)
		if err != nil {
			return err
		}
		fmt.Fprintf(&msg, "%s列 样式 %d 类型 %s，", col, meta[j].StyleId, meta[j].TypeRaw)
	}
	log.Printf("%s：数据格式 %s", filepath.Base(srcPath), strings.TrimSuffix(msg.String(), "，"))
	for k, v := range meta { // 日志输出不支持的 CellTyle
		if v.TypeIdx != excelize.CellTypeNumber && v.TypeIdx != excelize.CellTypeUnset &&
			v.TypeIdx != excelize.CellTypeInlineString && v.TypeIdx != excelize.CellTypeSharedString {
			col, err := excelize.ColumnNumberToName(k)
			if err != nil {
				return err
			}
			log.Printf("%s：%s列数据类型 %s 暂不支持", filepath.Base(srcPath), col, v.TypeRaw)
		}
	}

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
	fmt.Printf("数据文件：%s，%s，%d列，%d行\n",
		color.HiYellowString(filepath.Base(srcPath)), util.SizeReadable(info.Size()), len(meta), srcRows)

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
		tarFile    *excelize.File
		sw         *excelize.StreamWriter
		tarPath    string
		tarPathIdx int
		totalRows  int
		fileRows   int
		startFile  time.Time
	)
	for iter.Next() {
		if totalRows%lineCount == 0 {
			if tarPathIdx > 0 {
				sw.Flush()
				if err := tarFile.SaveAs(tarPath); err != nil {
					tarFile.Close()
					return err
				}
				tarFile.Close()
				fmt.Printf("数据文件%d：写入完成，共%s；预计剩余%s\n", tarPathIdx, color.HiYellowString("%d行", fileRows),
					util.CostReadable(time.Since(startFile).Seconds()*float64(fileCount-tarPathIdx)))
			}
			startFile = time.Now()
			tarPathIdx++
			tarPath = filepath.Join(tarDir, fmt.Sprintf("%s-%d.xlsx", filepath.Base(tarDir), tarPathIdx))
			// 使用模板文件（来自 Excel 2016+ 创建的空文件）
			tarFile, err = excelize.OpenReader(bytes.NewReader(templateXlsx))
			if err != nil {
				srcFile.Close()
				return err
			}
			sw, err = tarFile.NewStreamWriter("data") // 流式写入（不爆内存，注意始终从首行开始）
			if err != nil {
				tarFile.Close()
				srcFile.Close()
				return err
			}
			rowNew := make([]any, len(rowHeader))
			for c := range rowHeader { // 行首不检查 CellType
				rowNew[c] = excelize.Cell{
					StyleID: meta[c+1].StyleId,
					Value:   rowHeader[c],
				}
			}
			if err := sw.SetRow("A1", rowNew); err != nil {
				tarFile.Close()
				srcFile.Close()
				return err
			}
			fileRows = 0
		}
		select {
		case <-ctx.Done():
			tarFile.Close()
			srcFile.Close()
			return ctx.Err()
		default:
		} // 响应 Ctrl+C 打断
		totalRows++
		fileRows++
		row, err := iter.Columns()
		if err != nil {
			tarFile.Close()
			srcFile.Close()
			return err
		}
		rowNew := make([]any, len(row))
		for c := range row {
			cell := excelize.Cell{
				StyleID: meta[c+1].StyleId,
			}
			if row[c] == "" {
				cell.Value = nil
			} else if meta[c+1].TypeIdx == excelize.CellTypeNumber ||
				meta[c+1].TypeIdx == excelize.CellTypeUnset {
				valFix, err := strconv.ParseFloat(row[c], 64)
				if err == nil {
					cell.Value = valFix
				} else {
					cell.Value = row[c]
					col, err := excelize.ColumnNumberToName(c + 1)
					if err != nil {
						tarFile.Close()
						srcFile.Close()
						return err
					}
					log.Printf("%s：位置 %s%d，数据类型 %s，异常数据类型值 %s",
						filepath.Base(srcPath), col, fileRows, meta[c+1].TypeRaw, row[c])
				}
			} else { // excelize.CellTypeInlineString, excelize.CellTypeSharedString
				cell.Value = row[c]
			}
			rowNew[c] = cell
		}
		axis := fmt.Sprintf("A%d", fileRows+1)
		if err := sw.SetRow(axis, rowNew); err != nil {
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
		sw.Flush()
		if err := tarFile.SaveAs(tarPath); err != nil {
			tarFile.Close()
			return err
		}
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
