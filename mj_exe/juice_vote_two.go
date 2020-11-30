package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	carray := make(map[string]int)
	max := 0
	var maxCan string
	input := bufio.NewScanner(os.Stdin)
	for {
		if !input.Scan() {
			for can, count := range carray {
				if count > max {
					max = count
					maxCan = can
				}
			}
			fmt.Println(maxCan + " win!")
			break
		}

		line := input.Text()
		line = strings.TrimSpace(line)
		// "1 A,B"
		// "1 A,B"
		str := strings.Split(line, " ")
		candidates := strings.Split(str[1], ",")
		winner := candidates[0]
		//canRight:=candidates[1]
		carray[winner]++
	}

}
