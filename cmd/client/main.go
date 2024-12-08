package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"flag"

	"github.com/DeltaLaboratory/shard/cmd/client/parser"
	"github.com/DeltaLaboratory/shard/internal/client"
	"github.com/chzyer/readline"

	_ "embed"
)

var (
	leaderAddr = flag.String("addr", "localhost:8000", "Leader address")
)

//go:embed help
var helpString string

func main() {
	flag.Parse()

	client, err := client.NewClient(*leaderAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt: ">> ",
	})
	if err != nil {
		log.Fatalf("Failed to initalize readline: %v", err)
	}
	defer rl.Close()

	fmt.Println("Client (type '.exit' to quit)")
	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == ".help" {
			printHelp()
			continue
		} else if line == ".exit" {
			break
		} else if line == "" {
			continue
		}

		handleQuery(client, line)
	}
}

func printHelp() {
	fmt.Println(helpString)
}

func handleQuery(client *client.Client, query string) {
	parsed, err := parser.Parse(query)
	if err != nil {
		fmt.Println("Parsing Error:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch req := parsed.(type) {
	case parser.SetRequest:
		err := client.Set(ctx, []byte(req.Key), []byte(req.Value))
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		fmt.Printf("SET: key=%s, value=%s\n", req.Key, req.Value)

	case parser.GetRequest:
		value, err := client.Get(ctx, []byte(req.Key))
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		fmt.Printf("GET: key=%s, value=%s\n", req.Key, string(value))

	case parser.DeleteRequest:
		err := client.Delete(ctx, []byte(req.Key))
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		fmt.Printf("DELETE: key=%s\n", req.Key)
	}
}
