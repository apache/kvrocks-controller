package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
)

func PrintTable(fields []string, input interface{}) {
	array, ok := input.([]interface{})
	if !ok {
		fmt.Fprintln(os.Stderr, "table print must be array, but it is", reflect.ValueOf(input).Kind())
		os.Exit(-1)
	}
	fieldWidths := map[string]int{}
	for _, field := range fields {
		fieldWidths[field] = getFieldMaxWidth(field, array, true) + 2
	}
	printTableSeparator(fields, fieldWidths)
	printTableHeader(fields, fieldWidths)
	printTableSeparator(fields, fieldWidths)
	printTableBody(fields, fieldWidths, array, true)
	printTableSeparator(fields, fieldWidths)
}

func getFieldMaxWidth(field string, array []interface{}, include_header bool) int {
	var max_width int
	if include_header {
		max_width = len(field)
	} else {
		max_width = 0
	}
	fields := []string{field}
	for _, item := range array {
		m, err := ConvInterface2StringMap(fields, item)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(-1)
		}
		width := len(m[field])
		if width > max_width {
			max_width = width
		}
	}
	return max_width
}

func ConvInterface2StringMap(fields []string, in interface{}) (map[string]string, error) {
	out := map[string]string{}

	bytes, _ := json.Marshal(in)
	var m map[string]interface{}
	err := json.Unmarshal(bytes, &m)
	if err != nil {
		return nil, errors.New("convert json object to map failed")
	}

	for _, field := range fields {
		out[field] = fmt.Sprint(m[field])
	}
	return out, nil
}

func printTableSeparator(fields []string, fields_width map[string]int) {
	for _, field := range fields {
		width := fields_width[field]
		fmt.Print("+")
		printChar("-", width)
	}
	fmt.Println("+")
}

func printTableHeader(fields []string, fields_width map[string]int) {
	for _, field := range fields {
		width := fields_width[field]
		fmt.Print("| " + field)
		printChar(" ", width-len(field)-1)
	}
	fmt.Println("|")
}

func printTableBody(fields []string, fields_width map[string]int, array []interface{}, show_delimiter bool) {
	for _, item := range array {
		if reflect.ValueOf(item).IsNil() {
			printTableSeparator(fields, fields_width)
			continue
		}
		m, err := ConvInterface2StringMap(fields, item)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(-1)
		}
		for _, field := range fields {
			width := fields_width[field]
			if show_delimiter {
				fmt.Print("| ")
			}
			fmt.Print(m[field])
			printChar(" ", width-len(m[field])-1)
		}
		if show_delimiter {
			fmt.Print("|")
		}
		fmt.Println()
	}
}

func printChar(s string, width int) {
	for i := 0; i < width; i++ {
		fmt.Print(s)
	}
}
