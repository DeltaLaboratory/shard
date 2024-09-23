package parser

import (
	"reflect"
	"testing"
)

func TestParser(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *ParseResult
		wantErr  bool
	}{
		{
			name:     "Valid input with single field",
			input:    "QUERY (field1) KEY mykey",
			expected: &ParseResult{Fields: []string{"field1"}, Key: "mykey"},
			wantErr:  false,
		},
		{
			name:     "Valid input with multiple fields",
			input:    "QUERY (field1, field2, field3) KEY mykey",
			expected: &ParseResult{Fields: []string{"field1", "field2", "field3"}, Key: "mykey"},
			wantErr:  false,
		},
		{
			name:     "Valid input with underscore in identifiers",
			input:    "QUERY (field_1, field_2) KEY my_key",
			expected: &ParseResult{Fields: []string{"field_1", "field_2"}, Key: "my_key"},
			wantErr:  false,
		},
		{
			name:    "Invalid input - missing QUERY",
			input:   "(field1) KEY mykey",
			wantErr: true,
		},
		{
			name:    "Invalid input - missing opening parenthesis",
			input:   "QUERY field1) KEY mykey",
			wantErr: true,
		},
		{
			name:    "Invalid input - missing closing parenthesis",
			input:   "QUERY (field1 KEY mykey",
			wantErr: true,
		},
		{
			name:    "Invalid input - missing KEY",
			input:   "QUERY (field1) mykey",
			wantErr: true,
		},
		{
			name:    "Invalid input - empty field list",
			input:   "QUERY () KEY mykey",
			wantErr: true,
		},
		{
			name:    "Invalid input - invalid identifier",
			input:   "QUERY (1field) KEY mykey",
			wantErr: true,
		},
		{
			name:    "Invalid input - extra tokens at end",
			input:   "QUERY (field1) KEY mykey extratoken",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			result, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if !reflect.DeepEqual(result, tt.expected) {
					t.Errorf("Parse() got = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"Valid identifier", "validIdentifier", true},
		{"Valid identifier with underscore", "valid_identifier", true},
		{"Valid identifier with numbers", "valid123", true},
		{"Invalid identifier starting with number", "1invalid", false},
		{"Invalid identifier with special character", "invalid@", false},
		{"Empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidIdentifier(tt.input); got != tt.want {
				t.Errorf("isValidIdentifier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTokenize(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "Simple tokenization",
			input: "QUERY (field1, field2) KEY mykey",
			want:  []string{"QUERY", "(", "field1", ",", "field2", ")", "KEY", "mykey"},
		},
		{
			name:  "Tokenization with extra spaces",
			input: "QUERY  (  field1 ,  field2  )  KEY  mykey",
			want:  []string{"QUERY", "(", "field1", ",", "field2", ")", "KEY", "mykey"},
		},
		{
			name:  "Empty input",
			input: "",
			want:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tokenize(tt.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tokenize() = %v, want %v", got, tt.want)
			}
		})
	}
}
