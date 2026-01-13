//go:generate goversioninfo
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"gitee.com/nguaduot/split-xlsx-go/internal/csv"
	"gitee.com/nguaduot/split-xlsx-go/internal/xlsx"
	"gitee.com/nguaduot/split-xlsx-go/pkg/util"
	"github.com/fatih/color"
)

var (
	reader      = bufio.NewReader(os.Stdin)
	defMergeExt = ".xlsx"
)

func getSrcPaths() ([]string, error) {
	files := []string{}
	flag.Parse()
	args := flag.Args() // 所有非 flag 参数
	for _, arg := range args {
		if !util.IsExcelFile(arg) {
			fmt.Printf("该文件不存在或非 Excel 文件：%s\n", arg)
			continue
		}
		if !slices.Contains(files, arg) {
			files = append(files, arg)
			fmt.Printf("数据文件%d：%s\n", len(files), filepath.Base(arg))
		}
	}
	if len(files) < 2 {
		for {
			fmt.Printf("数据文件%d %s：", len(files)+1, color.HiBlackString("(直接回车结束选择)"))
			input, err := reader.ReadString('\n')
			if err != nil {
				return []string{}, err
			}
			input = strings.Trim(strings.TrimSpace(input), "\"'")
			if input == "" {
				break
			}
			if !util.IsExcelFile(input) {
				fmt.Printf("该文件不存在或非 Excel 文件：%s\n", input)
				break
			}
			if !slices.Contains(files, input) {
				files = append(files, input)
			}
		}
	}
	return files, nil
}

// getTargetPath
// 1. 输入含路径则按完整路径导出
// 2. 输入不含路径则按源文件路径导出
// 3. 不输入则优先使用共同前缀，若无则按源文件1生成文件名 -merge
// 4. 输入含后缀则取为格式
// 5. 输入不含后缀则继续引导选择格式
func getTargetPath(srcPaths []string) (string, error) {
	fmt.Printf("导出文件名 %s：", color.HiBlackString("(直接回车自动生成)"))
	input, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	name, ext := strings.Trim(strings.TrimSpace(input), "\"'"), ""
	if name != "" { // 尝试从输入提取文件名和格式
		if !filepath.IsAbs(input) {
			name, err = util.RelativePath2Abs(name)
			if err != nil {
				return "", err
			}
		}
		ext = filepath.Ext(name)
		name = strings.TrimSuffix(name, ext)
		ext = strings.ToLower(ext)
		if ext != "" && ext != ".xlsx" && ext != ".csv" {
			return "", fmt.Errorf("不支持合并为该格式：%s", ext)
		}
	}
	if ext == "" { // 未输入文件名，或文件名未包含后缀，使用默认格式
		// fmt.Printf("导出格式：%s. xlsx %s %s. csv %s\n", color.HiYellowString("1"), color.HiBlackString("(较慢)"),
		// 	color.HiYellowString("2"), color.HiBlackString("(较大)"))
		// fmt.Printf("选择导出格式 %s：", color.HiBlackString("(直接回车使用 xlsx)"))
		// input, err := reader.ReadString('\n')
		// if err != nil {
		// 	return "", err
		// }
		// input = strings.ToLower(strings.TrimSpace(input))
		// if input == "2" || input == ".csv" || input == "csv" {
		// 	ext = ".csv"
		// } else {
		// 	ext = ".xlsx"
		// }
		ext = defMergeExt
	}
	if name == "" { // 未输入文件名，根据源文件生成文件名
		oneName, err := util.ParseApollo14633Name(srcPaths)
		if err != nil {
			return "", err
		}
		if oneName != "" {
			name = oneName
		} else if len(srcPaths) > 0 {
			name1 := strings.TrimSuffix(filepath.Base(srcPaths[0]), filepath.Ext(srcPaths[0]))
			for i := 1; i <= len(name1); i++ {
				prefix := name1[:i]
				match := true
				for _, file := range srcPaths {
					if !strings.HasPrefix(filepath.Base(file), prefix) {
						match = false
						break
					}
				}
				if !match {
					break
				}
				name = filepath.Join(filepath.Dir(srcPaths[0]), prefix)
			}
			name = strings.TrimRight(name, " -_&(（.") + "-merge" // 移除末尾无用字符
		} else {
			name, err = util.RelativePath2Abs(time.Now().Format("20060102150405"))
			if err != nil {
				return "", err
			}
			name += "-merge"
		}
	}
	return name + ext, nil
}

func merge(srcPaths []string, tarPath string) error {
	// 用于响应用户 Ctrl+C 打断
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if strings.ToLower(filepath.Ext(tarPath)) == ".csv" {
		return csv.MergeXlsx2csv(srcPaths, tarPath, ctx)
	}
	// return xlsx.MergeXlsx2xlsxV1(srcPaths, tarPath, ctx)
	return xlsx.MergeXlsx2xlsxV2(srcPaths, tarPath, ctx)
}

func welcome() {
	fmt.Println("====", color.HiCyanString("Excel Merge"), "=====================================")
	fmt.Println("Version :", color.HiGreenString("v1.2.260113"))
	fmt.Println("Author  :", color.HiGreenString("nguaduot"))
	fmt.Println("Repo    :", color.HiGreenString("https://github.com/nguaduot/xlsx-merge-split"))
	fmt.Println("======================================================")

	fmt.Printf("提示1：%s\n", color.HiRedString("请选择格式规整、不含公式的纯数据 Excel 文件，多表首行保持一致，避免合并失败。"))
	fmt.Printf("提示2：%s\n", color.HiRedString("流式读写，内存占用稳定，支持超大数据文件，但请注意 Excel 最大仅支持 1048576 行。"))
}

func main() {
	welcome()

	srcPaths, err := getSrcPaths()
	if err != nil {
		fmt.Println(err)
		util.WaitForExit()
		return
	}
	if len(srcPaths) < 2 {
		fmt.Println("未选择2个及以上 Excel 文件，不进行合并")
		util.WaitForExit()
		return
	}

	tarPath, err := getTargetPath(srcPaths)
	if err != nil {
		fmt.Println(err)
		util.WaitForExit()
		return
	}

	cleanLog, err := util.InitLog(tarPath)
	if err != nil {
		fmt.Println(err)
		util.WaitForExit()
		return
	}

	err = merge(srcPaths, tarPath)
	if err != nil {
		fmt.Println(err)
		if errors.Is(err, context.Canceled) {
			fmt.Println("注意：你已强行停止，合并可能并未成功")
		}
		cleanLog()
		util.WaitForExit()
		return
	}

	cleanLog()
	util.WaitForExit()
}
