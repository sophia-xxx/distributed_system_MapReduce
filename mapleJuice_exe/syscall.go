package main

import (
	"fmt"
	"os/exec"
)

func main() {
	_, err := exec.Command("/bin/sh", "-c", "maple_vote < maple_vote_test1 >result").Output()
	if err != nil {
		fmt.Printf("%s", err)
	}
}
