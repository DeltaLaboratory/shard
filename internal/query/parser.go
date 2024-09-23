package parser

import (
	"fmt"
	"strings"
	"unicode"
)

type Parser struct {
	tokens []string
	pos    int
}

type ParseResult struct {
	Fields []string
	Key    string
}

func NewParser(input string) *Parser {
	return &Parser{
		tokens: tokenize(input),
		pos:    0,
	}
}

func (p *Parser) Parse() (*ParseResult, error) {
	fields, err := p.parseQuery()
	if err != nil {
		return nil, err
	}

	key, err := p.parseKeyClause()
	if err != nil {
		return nil, err
	}

	if p.pos != len(p.tokens) {
		return nil, fmt.Errorf("unexpected tokens at end of input")
	}

	return &ParseResult{Fields: fields, Key: key}, nil
}

func (p *Parser) parseQuery() ([]string, error) {
	if p.pos >= len(p.tokens) || p.tokens[p.pos] != "QUERY" {
		return nil, fmt.Errorf("expected 'QUERY'")
	}
	p.pos++

	if p.pos >= len(p.tokens) || p.tokens[p.pos] != "(" {
		return nil, fmt.Errorf("expected '('")
	}
	p.pos++

	fields, err := p.parseFieldList()
	if err != nil {
		return nil, err
	}

	if p.pos >= len(p.tokens) || p.tokens[p.pos] != ")" {
		return nil, fmt.Errorf("expected ')'")
	}
	p.pos++

	return fields, nil
}

func (p *Parser) parseFieldList() ([]string, error) {
	var fields []string

	field, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	fields = append(fields, field)

	for p.pos < len(p.tokens) && p.tokens[p.pos] == "," {
		p.pos++
		field, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	return fields, nil
}

func (p *Parser) parseKeyClause() (string, error) {
	if p.pos >= len(p.tokens) || p.tokens[p.pos] != "KEY" {
		return "", fmt.Errorf("expected 'KEY'")
	}
	p.pos++

	return p.parseIdentifier()
}

func (p *Parser) parseIdentifier() (string, error) {
	if p.pos >= len(p.tokens) {
		return "", fmt.Errorf("expected identifier")
	}
	if !isValidIdentifier(p.tokens[p.pos]) {
		return "", fmt.Errorf("invalid identifier: %s", p.tokens[p.pos])
	}
	identifier := p.tokens[p.pos]
	p.pos++
	return identifier, nil
}

func isValidIdentifier(s string) bool {
	if len(s) == 0 || !unicode.IsLetter(rune(s[0])) {
		return false
	}
	for _, ch := range s[1:] {
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			return false
		}
	}
	return true
}

func tokenize(input string) []string {
	input = strings.ReplaceAll(input, "(", " ( ")
	input = strings.ReplaceAll(input, ")", " ) ")
	input = strings.ReplaceAll(input, ",", " , ")
	return strings.Fields(input)
}
