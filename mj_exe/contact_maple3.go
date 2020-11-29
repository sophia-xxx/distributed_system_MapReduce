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
		str := strings.Split(line, ",")
		key := str[0]
		value := str[1]
		temp := strings.Split(key, " ")
		valueTemp := strings.Split(key, " ")
		if strings.Compare(value, "positive") == 0 {
			fmt.Println(temp[1] + "," + "positive " + temp[2] + " " + temp[3])
		}
		if len(valueTemp) == 3 {
			fmt.Println(valueTemp[0] + "," + "testcase " + valueTemp[1] + " " + valueTemp[2] + " " + key)
		}

	}
}
