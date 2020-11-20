package main

import (
	"fmt"
	"log"
	"mp3/net_node"
	"mp3/run_server"
	"os"
	"strings"
)

func printMainHelp() {
	fmt.Println("Usage: 'go run main.go' or './main'")

}

func main() {
	// If there are not arguments except for main.go
	// we are running the binary in production
	// If we are running in debug mode, we use localhost for the IP
	// Otherwise, the user is not running the function correctly
	args := os.Args[1:]
	switch {
	case len(args) == 0:
		break
	case len(args) == 1 && strings.Compare(args[0], "debug") == 0:
		net_node.INTRO_IP = []byte{0, 0, 0, 0}
	default:
		printMainHelp()
		return
	}

	// Set up Logging
	// Write the logs to a file named logs
	file, err := os.OpenFile("log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	log.SetOutput(file)

	run_server.CLI()
}
