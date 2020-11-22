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
		str := strings.Split(line, " ")
		can1 := str[0]
		can2 := str[1]
		can3 := str[2]
		if strings.Compare(can1, can2) < 0 {
			fmt.Printf("%s,%s 1\n", can1, can2)
		} else {
			fmt.Printf("%s,%s 0\n", can2, can1)
		}

		if strings.Compare(can2, can3) < 0 {
			fmt.Printf("%s,%s 1\n", can2, can3)
		} else {
			fmt.Printf("%s,%s 0\n", can3, can2)
		}

		if strings.Compare(can1, can3) < 0 {
			fmt.Printf("%s,%s 1\n", can1, can3)
		} else {
			fmt.Printf("%s,%s 0\n", can3, can1)
		}
	}
}
