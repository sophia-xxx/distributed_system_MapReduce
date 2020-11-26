package main

import (
	"bufio"
	"fmt"
	"mp3/config"
	"os"
	"strconv"
)

func main() {
	fileClips := make(map[int]string)
	// read lines of file
	//execPath, _ := os.Getwd()
	file, err := os.Open("./mj_exe/wc_input")
	if err != nil {
		fmt.Println("Can't open file!")
	}
	defer file.Close()
	fileScanner := bufio.NewScanner(file)
	lineCount := 0
	for fileScanner.Scan() {
		lineCount++
	}
	fmt.Println(file.Name() + " file has " + strconv.Itoa(lineCount) + " lines!!!")
	//fmt.Println(os.Getwd())

	// split file into file clips, then generate list of fileNames
	splitLines := lineCount/3 + 1
	// re-open the file
	file, _ = os.Open("./mj_exe/wc_input")
	fileScanner = bufio.NewScanner(file)
	// determine whether the file is end
	endScan := false
	count := 0
	for fileScanner.Scan() {
		var fileSplit *os.File
		// create new files for different file clips
		fileSplit, _ = os.Create("./" + config.CLIPPREFIX + strconv.Itoa(count))
		defer fileSplit.Close()
		for i := 0; i < splitLines-1; i++ {
			line := fileScanner.Text()
			fileSplit.WriteString(line + "\n")
			if !fileScanner.Scan() {
				endScan = true
				fileClips[count] = "CLIPPREFIX" + strconv.Itoa(count)
				fileInfo, _ := fileSplit.Stat()
				fmt.Println("File clip: ", fileInfo.Size())
				break
			}
		}
		if endScan {
			break
		}
		// last line
		line := fileScanner.Text()
		fileSplit.WriteString(line + "\n")
		// check whether this write successfully
		fileInfo, _ := fileSplit.Stat()
		fileClips[count] = "CLIPPREFIX" + strconv.Itoa(count)
		fmt.Println("File clip: ", fileInfo.Size())
		count++
	}
	//return fileClips
	fmt.Println(fileClips)
}
