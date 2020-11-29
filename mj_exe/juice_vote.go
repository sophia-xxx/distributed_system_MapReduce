package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	input := bufio.NewScanner(os.Stdin)
	var canLeft string
	var canRight string
	var numZero int
	var numOne int
	for {
		if !input.Scan() {
			if numOne > numZero {
				fmt.Println(canLeft + "," + canRight)
			} else {
				fmt.Println(canRight + "," + canLeft)
			}
			break
		}
		line := input.Text()
		line = strings.TrimSpace(line)
		str := strings.Split(line, " ")
		//fmt.Println(line)

		canPair := str[0]
		canTemp := strings.Split(canPair, ",")
		canLeft = canTemp[0]
		canRight = canTemp[1]
		numZero = 0
		numOne = 0
		value := str[1]
		if strings.Compare(value, "0") == 0 {
			numZero++
		}
		if strings.Compare(value, "1") == 0 {
			numOne++
		}
	}

}
