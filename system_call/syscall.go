package main

import (
	"fmt"
	"os/exec"
)

// func main() {
// 	//in := bytes.NewBuffer(nil)
// 	//outfile := "syscallout"
// 	infile := "maple_wordcount_test1"
// 	exename := "../mapleJuice_exe/wordcount_maple.exe"
// 	cmd := exename + "<" + infile
// 	// + ">" + outfile
// 	res := exec.Command("/bin/sh", "-c", cmd)
// 	byte, err := res.Output()
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println(string(byte))

// }

func main() {
	//in := bytes.NewBuffer(nil)
	cmd := "echo hello world > test.txt\n"
	result, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(string(result))
}
