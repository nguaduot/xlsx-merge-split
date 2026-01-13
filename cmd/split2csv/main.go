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
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"gitee.com/nguaduot/split-xlsx-go/internal/csv"
	"gitee.com/nguaduot/split-xlsx-go/internal/xlsx"
	"gitee.com/nguaduot/split-xlsx-go/pkg/util"
	"github.com/fatih/color"
)

var (
	reader       = bufio.NewReader(os.Stdin)
	defSplitLine = 20000
	defSplitFile = 2
	defSplitExt  = ".csv"
)

func getSrcPath() (string, error) {
	var file string
	flag.Parse()
	args := flag.Args() // 所有非 flag 参数
	for _, arg := range args {
		if !util.IsExcelFile(arg) {
			fmt.Printf("该文件不存在或非 Excel 文件：%s\n", arg)
			continue
		}
		file = arg
		fmt.Printf("数据文件：%s\n", filepath.Base(file))
		break
	}
	if file == "" {
		fmt.Print("数据文件：")
		input, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		file = strings.Trim(strings.TrimSpace(input), "\"'")
		if file != "" && !util.IsExcelFile(file) {
			return "", fmt.Errorf("该文件不存在或非 Excel 文件：%s\n", file)
		}
	}
	return file, nil
}

func getTarget(srcPath string) (int, int, string, string, error) {
	if srcPath == "" {
		return 0, 0, "", "", errors.New("拆分文件不存在")
	}
	dirTarget := strings.TrimSuffix(srcPath, filepath.Ext(srcPath))
	res, err := util.IsDir(dirTarget)
	if err != nil {
		return 0, 0, "", "", err
	}
	if res {
		entries, err := os.ReadDir(dirTarget)
		if err != nil {
			return 0, 0, "", "", err
		}
		if len(entries) > 0 {
			allSplited := true
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasPrefix(entry.Name(), filepath.Base(dirTarget)) {
					allSplited = false
					break
				}
			}
			if allSplited {
				fmt.Printf("该文件曾拆分为%d份，是否重新拆分？%s ", len(entries), color.HiBlackString("(回车以继续)"))
				_, err = reader.ReadString('\n')
				if err != nil {
					return 0, 0, "", "", err
				}
				for _, entry := range entries {
					err = os.Remove(filepath.Join(dirTarget, entry.Name()))
					if err != nil {
						return 0, 0, "", "", err
					}
				}
			} else { // 拆分文件夹存在其他文件，交由用户处理，避免误删
				return 0, 0, "", "", fmt.Errorf("拆分文件夹包含其他资料，无法拆分：%s", filepath.Base(dirTarget))
			}
		}
	} else {
		err := os.Mkdir(dirTarget, 0755)
		if err != nil { // 不会出现 os.IsExist(err)
			return 0, 0, "", "", err
		}
	}
	var (
		splitLine int
		splitFile int
	)
	fmt.Printf("数据拆分方式：%s. 按行数 %s. 按文件数 %s\n",
		color.HiYellowString("1"), color.HiYellowString("2"), color.HiBlackString("(较慢)"))
	fmt.Printf("选择数据拆分方式 %s：", color.HiBlackString("(可一并设定行数或文件数，直接回车按行数)"))
	input, err := reader.ReadString('\n')
	if err != nil {
		return 0, 0, "", "", err
	}
	args := regexp.MustCompile(`\d+`).FindAllString(input, 2)
	if len(args) > 0 && args[0] == "2" {
		if len(args) > 1 {
			splitFile, err = strconv.Atoi(args[1])
			if err != nil {
				return 0, 0, "", "", err
			}
			if splitFile < 2 {
				return 0, 0, "", "", fmt.Errorf("目标文件数异常：%d", splitFile)
			}
		} else {
			fmt.Printf("拆分文件数 %s：", color.HiBlackString("(直接回车设为%d)", defSplitFile))
			input, err = reader.ReadString('\n')
			if err != nil {
				return 0, 0, "", "", err
			}
			input = strings.TrimSpace(input)
			if input != "" {
				splitFile, err = strconv.Atoi(input)
				if err != nil {
					return 0, 0, "", "", err
				}
				if splitFile < 2 {
					return 0, 0, "", "", fmt.Errorf("目标文件数异常：%d", splitFile)
				}
			} else {
				splitFile = defSplitFile
			}
		}
	} else {
		if len(args) > 1 && args[0] == "1" {
			splitLine, err = strconv.Atoi(args[1])
			if err != nil {
				return 0, 0, "", "", err
			}
			if splitLine < 1 {
				return 0, 0, "", "", fmt.Errorf("目标行数异常：%d", splitLine)
			}
		} else {
			fmt.Printf("拆分行数 %s：", color.HiBlackString("(直接回车设为%d)", defSplitLine))
			input, err = reader.ReadString('\n')
			if err != nil {
				return 0, 0, "", "", err
			}
			input = strings.TrimSpace(input)
			if input != "" {
				splitLine, err = strconv.Atoi(input)
				if err != nil {
					return 0, 0, "", "", err
				}
				if splitLine < 1 {
					return 0, 0, "", "", fmt.Errorf("目标行数异常：%d", splitLine)
				}
			} else {
				splitLine = defSplitLine
			}
		}
	}
	// fmt.Printf("导出格式：%s. xlsx %s %s. csv %s\n", color.HiYellowString("1"), color.HiBlackString("(较慢)"),
	// 	color.HiYellowString("2"), color.HiBlackString("(较大)"))
	// fmt.Printf("选择导出格式 %s：", color.HiBlackString("(直接回车使用 xlsx)"))
	// input, err = reader.ReadString('\n')
	// if err != nil {
	// 	return 0, 0, "", "", err
	// }
	// input = strings.ToLower(strings.TrimSpace(input))
	// if input == "2" || input == ".csv" || input == "csv" {
	// 	splitExt = ".csv"
	// } else {
	// 	splitExt = ".xlsx"
	// }
	return splitLine, splitFile, dirTarget, defSplitExt, nil
}

func split(srcPath string, splitLine int, splitFile int, splitDir string, splitExt string) error {
	// 用于响应用户 Ctrl+C 打断
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if splitExt == ".csv" {
		if splitFile > 0 {
			return csv.SplitXlsx2csvByFile(srcPath, splitDir, splitFile, ctx)
		} else {
			return csv.SplitXlsx2csvByLine(srcPath, splitDir, splitLine, ctx)
		}
	}
	if splitFile > 0 {
		return xlsx.SplitXlsx2xlsxByFile(srcPath, splitDir, splitFile, ctx)
	} else {
		return xlsx.SplitXlsx2xlsxByLine(srcPath, splitDir, splitLine, ctx)
	}
}

func welcome() {
	fmt.Println("====", color.HiCyanString("Excel Split"), "=====================================")
	fmt.Println("Version :", color.HiGreenString("v1.2.260113"))
	fmt.Println("Author  :", color.HiGreenString("nguaduot"))
	fmt.Println("Repo    :", color.HiGreenString("https://github.com/nguaduot/xlsx-merge-split"))
	fmt.Println("======================================================")

	fmt.Printf("提示1：%s\n", color.HiRedString("请选择格式规整、不含公式的纯数据 Excel 文件，避免拆分失败。"))
	fmt.Printf("提示2：%s\n", color.HiRedString("流式读写，内存占用稳定，支持超大数据文件，但请注意 Excel 最大仅支持 1048576 行。"))
}

func main() {
	welcome()

	srcPath, err := getSrcPath()
	if err != nil {
		fmt.Println(err)
		util.WaitForExit()
		return
	}
	if srcPath == "" {
		fmt.Println("未选择 Excel 文件，无可拆分")
		util.WaitForExit()
		return
	}

	splitLine, splitFile, splitDir, splitExt, err := getTarget(srcPath)
	if err != nil {
		fmt.Println(err)
		util.WaitForExit()
		return
	}

	cleanLog, err := util.InitLog(srcPath)
	if err != nil {
		fmt.Println(err)
		util.WaitForExit()
		return
	}

	err = split(srcPath, splitLine, splitFile, splitDir, splitExt)
	if err != nil {
		fmt.Println(err)
		if errors.Is(err, context.Canceled) {
			fmt.Println("注意：你已强行停止，拆分可能并未完成")
		}
		cleanLog()
		util.WaitForExit()
		return
	}

	cleanLog()
	util.WaitForExit()
}
