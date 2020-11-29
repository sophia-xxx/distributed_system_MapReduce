package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	input := bufio.NewScanner(os.Stdin)
	var positiveList []string
	var testList []string
	for {
		if !input.Scan() {
			names := compare(positiveList, testList)
			for _, name := range names {
				fmt.Println(name)
			}
			break
		}
		line := input.Text()
		line = strings.TrimSpace(line)
		str := strings.Split(line, ",")

		restString := str[1]
		temp := strings.Split(restString, " ")
		test_or_not := temp[0]
		value := strings.Join(temp[1:], ",")
		if strings.Compare(test_or_not, "positive") == 0 {
			positiveList = append(positiveList, value)
		}
		if strings.Compare(test_or_not, "testcase") == 0 {
			testList = append(testList, value)
		}
		//

		fmt.Println(line + ",positive")
	}
}
func compare(positiveList []string, testList []string) []string {
	var names []string
	var start string
	var end string
	for _, time := range positiveList {
		tmp := strings.Split(time, ",")
		start = tmp[0]
		end = tmp[1]
		for _, testTime := range testList {
			tmp := strings.Split(testTime, ",")
			startTest := tmp[0]
			endTest := tmp[1]
			if endTest > start || end > startTest {
				continue
			}
			names = append(names, tmp[2])
		}
	}
	return names
}
