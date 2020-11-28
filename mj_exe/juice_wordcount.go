package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	input := bufio.NewScanner(os.Stdin)
	var wcmap map[string]int
	wcmap = make(map[string]int)
	for {
		if !input.Scan() {
			break
		}
		line := input.Text()
		line = strings.TrimSpace(line)
		str := strings.Split(line, " ")
		wordkey := str[0]
		count, _ := strconv.Atoi(str[1])
		wcmap[wordkey] += count

	}
	for k, v := range wcmap {
		fmt.Printf("%s %d\n", k, v)
	}
}
