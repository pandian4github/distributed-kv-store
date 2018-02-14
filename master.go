package main

import (
	"fmt"
	"os"
	"log"
	"bufio"
	"strings"
)

var f_map = map[string]func(args []string) error {
	"joinServer": joinServer,
	"killServer": killServer,
	"joinClient": joinClient,
	"breakConnection": breakConnection,
	"createConnection": createConnection,
	"stabilize": stabilize,
	"printStore": printStore,
	"put": put,
	"get": get,
}

func joinServer(args []string) error {
	return nil
}

func killServer(args []string) error {
	return nil
}

func joinClient(args []string) error {
	return nil
}

func breakConnection(args []string) error {
	return nil
}

func createConnection(args []string) error {
	return nil
}

func stabilize(args []string) error {
	return nil
}

func printStore(args []string) error {
	return nil
}

func put(args []string) error {
	return nil
}

func get(args []string) error {
	return nil
}


func main() {
	args := os.Args

	if len(args) != 2 {
		fmt.Println("Program should contain one argument (path to the file containing the commands)")
		os.Exit(1)
	}
	commandsFile := args[1]
	log.Println("Commands file:", commandsFile)
	file, err := os.Open(commandsFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		num_parts := len(parts)
		if num_parts == 0 {
			continue
		}
		if function, ok := f_map[parts[0]]; ok {
			err := function(parts)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Println("Error: unknown command to the key-value store", parts[0])
		}
		log.Println("Execution over for command:", line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
