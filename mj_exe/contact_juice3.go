package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	input := bufio.NewScanner(os.Stdin)
	for {
		if !input.Scan() {
			break
		}
		line := input.Text()
		line = strings.TrimSpace(line)
		//str := strings.Split(line, " ")
		//name := str[0]
		//location := str[1]
		//start := str[2]
		//end:=str[3]
		//value:=strings.Join(str[1:],",")

		fmt.Println(line + ",positive")

	}
}
