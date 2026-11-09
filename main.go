package main

import (
	"fmt"
	"os"

	llmr "llm-request-router/src"
)

func main() {
	if err := llmr.Execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
