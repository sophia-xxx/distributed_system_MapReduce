package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	// A,B
	input := bufio.NewScanner(os.Stdin)
	for {
		if !input.Scan() {
			break
		}
		line := input.Text()
		line = strings.TrimSpace(line)
		//str := strings.Split(line, ",")
		fmt.Println("1 " + line)
	}

}
