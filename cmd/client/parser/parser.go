package parser

import (
	"fmt"
	"strings"
)

type SetRequest struct {
	Key   string
	Value string
}

type GetRequest struct {
	Key string
}

type DeleteRequest struct {
	Key string
}

func parseQuery(query string) []string {
	query = strings.TrimSpace(query)

	tokens := []string{}
	currentToken := ""
	inQuotes := false
	quoteChar := rune(0)
	escape := false

	for _, char := range query {
		switch {
		case (char == '"' || char == '\'') && !escape:
			if !inQuotes {
				inQuotes = true
				quoteChar = char
			} else if char == quoteChar {
				tokens = append(tokens, currentToken)
				currentToken = ""
				inQuotes = false
				quoteChar = rune(0)
			} else {
				currentToken += string(char)
			}
		case char == ' ':
			if inQuotes {
				currentToken += string(char)
			} else if currentToken != "" {
				tokens = append(tokens, currentToken)
				currentToken = ""
			}
		case char == '\\':
			escape = true
		default:
			currentToken += string(char)
		}
	}

	if currentToken != "" {
		tokens = append(tokens, currentToken)
	}

	return tokens
}

func Parse(query string) (interface{}, error) {
	if query == "" {
		return nil, fmt.Errorf("empty query")
	}

	tokens := parseQuery(query)

	if len(tokens) < 1 {
		return nil, fmt.Errorf("invalid query")
	}

	switch tokens[0] {
	case "SET":
		if len(tokens) != 3 {
			return nil, fmt.Errorf("SET query requires exactly 2 arguments (key and value)")
		}
		return SetRequest{
			Key:   tokens[1],
			Value: tokens[2],
		}, nil

	case "GET":
		if len(tokens) != 2 {
			return nil, fmt.Errorf("GET query requires exactly 1 argument (key)")
		}
		return GetRequest{
			Key: tokens[1],
		}, nil

	case "DELETE":
		if len(tokens) != 2 {
			return nil, fmt.Errorf("DELETE query requires exactly 1 argument (key)")
		}
		return DeleteRequest{
			Key: tokens[1],
		}, nil

	default:
		return nil, fmt.Errorf("unknown query type: %s", tokens[0])
	}
}
