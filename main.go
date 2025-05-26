package main

import (
	"flag"
	"log"

	"github.com/mikarwacki/TeamworkGoTests/customerimporter"
)

func main() {
	var (
		inputFilePath  = flag.String("input", "", "Input file path")
		outputFilePath = flag.String("output", "", "Output file path (default stdout)")
	)
	flag.Parse()

	if inputFilePath == nil || *inputFilePath == "" {
		log.Fatal("-input flag is required")
	}

	domainsCount, err := customerimporter.ProcessFile(*inputFilePath)
	if err != nil {
		log.Fatalf("Error processing file: %v", err)
	}

	err = customerimporter.WriteOutput(*domainsCount, outputFilePath)
	if err != nil {
		log.Fatalf("Error writing ouput: %v", err)
	}
}
