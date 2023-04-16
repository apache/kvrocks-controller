package main

import "fmt"

func PrintError(err error) {
	fmt.Println("Error:", err)
}

func PrintStrings(strings []string) {
	for _, str := range strings {
		fmt.Println(str)
	}
}
