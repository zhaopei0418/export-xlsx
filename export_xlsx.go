package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/axgle/mahonia"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	_ "github.com/mattn/go-oci8"
)

var xlsxColumns []string

var (
	help           bool
	databaseUrl    string
	sqlFilePath    string = "1.txt"
	xlsxFilePath   string = "1.xlsx"
	sheetPageCount int    = 1000000
)

func usage() {
	fmt.Fprintf(os.Stderr, `export_xlsx version: export_xlsx/1.0
Usage: export_xlsx -l databaseUrl [-s sqlfilepath] [-x xlsxfilepath] [-c sheetpagecount]

Options:
`)
	flag.PrintDefaults()
}

func initFlag() {
	flag.BoolVar(&help, "h", false, "this help")

	flag.StringVar(&databaseUrl, "l", "", "database connection url, username/password@host:port/sid")
	flag.StringVar(&sqlFilePath, "s", "1.txt", "sql script path")
	flag.StringVar(&xlsxFilePath, "x", "1.xlsx", "xlsx file path")
	flag.IntVar(&sheetPageCount, "c", 1000000, "sheet page count")
	flag.Usage = usage
}

func convertToString(src string, srcCode string, tagCode string) string {
	srcCoder := mahonia.NewDecoder(srcCode)
	srcResult := srcCoder.ConvertString(src)
	tagCoder := mahonia.NewDecoder(tagCode)
	_, cdata, _ := tagCoder.Translate([]byte(srcResult), true)
	result := string(cdata)
	return result
}

func readFileToMemory(srcFile string) string {
	content, err := ioutil.ReadFile(srcFile)
	if err != nil {
		log.Fatal(err)
	}
	return string(content)
}

func initXlsxColumns() {
	for i := 0; i < 27; i++ {
		for j := 0; j < 26; j++ {
			byteBuff := bytes.Buffer{}
			if i > 0 {
				byteBuff.WriteString(string(65 + i - 1))
			}
			byteBuff.WriteString(string(65 + j))
			xlsxColumns = append(xlsxColumns, byteBuff.String())
		}
	}
}

func writeXlsxTitle(file *excelize.File, sheet string, columns []string) {
	for i, column := range columns {
		byteBuff := bytes.Buffer{}
		byteBuff.WriteString(xlsxColumns[i])
		byteBuff.WriteString("1")
		file.SetCellValue(sheet, byteBuff.String(), convertToString(column, "gbk", "utf-8"))
	}
}

func main() {
	initFlag()
	flag.Parse()

	if help || len(databaseUrl) == 0 {
		flag.Usage()
		return
	}

	sqlContent := readFileToMemory(sqlFilePath)
	xlsxFile := xlsxFilePath

	initXlsxColumns()

	db, err := sql.Open("oci8", databaseUrl)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	rows, err := db.Query(sqlContent)
	if err != nil {
		log.Fatalln(err)
	}
	defer rows.Close()

	file := excelize.NewFile()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatalln(err)
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i, _ := range values {
		scanArgs[i] = &values[i]
	}

	sheet := "Sheet1"
	sheetCount := 1
	rowCount := 2

	writeXlsxTitle(file, sheet, columns)

	for rows.Next() {
		if rowCount > sheetPageCount {
			sheetCount++
			byteBuff := bytes.Buffer{}
			byteBuff.WriteString("Sheet")
			byteBuff.WriteString(strconv.Itoa(sheetCount))
			sheet = byteBuff.String()
			file.NewSheet(sheet)
			writeXlsxTitle(file, sheet, columns)
			rowCount = 2
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Fatalln(err)
		}

		for i, col := range values {
			byteBuff := bytes.Buffer{}
			byteBuff.WriteString(xlsxColumns[i])
			byteBuff.WriteString(strconv.Itoa(rowCount))
			var value string
			if col != nil {
				value = convertToString(string(col), "gbk", "utf-8")
			}
			file.SetCellValue(sheet, byteBuff.String(), value)
		}
		log.Printf("[WRITE XLSX] Write sheet: %d, row count: %d successful.", sheetCount, rowCount)
		rowCount++
	}

	if err = rows.Err(); err != nil {
		log.Fatalln(err)
	}

	file.SetActiveSheet(1)
	err = file.SaveAs(xlsxFile)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("save file %s successful.", xlsxFile)
}
