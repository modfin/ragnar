package document

import (
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConvertToMarkdown_JSON(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	jsonInput := `{"name": "test", "value": 123}`
	reader := strings.NewReader(jsonInput)

	result, err := ConvertToMarkdown(logger, reader, "application/json", "")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	output, err := io.ReadAll(result)
	if err != nil {
		t.Fatalf("Failed to read result: %v", err)
	}

	expected := "```json\n{\n  \"name\": \"test\",\n  \"value\": 123\n}\n```"
	if string(output) != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, string(output))
	}
}

func TestConvertToMarkdown_PlainText(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	textInput := "This is plain text content"
	reader := strings.NewReader(textInput)

	result, err := ConvertToMarkdown(logger, reader, "text/plain", "")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	output, err := io.ReadAll(result)
	if err != nil {
		t.Fatalf("Failed to read result: %v", err)
	}

	expected := "```text\nThis is plain text content\n```"
	if string(output) != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, string(output))
	}
}

func TestConvertToMarkdown_HTML(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Use simpler HTML to avoid potential issues
	htmlInput := `<h1>Main Heading</h1>
<p>This is a <strong>bold</strong> paragraph with <em>italic</em> text.</p>
<ul>
<li>First item</li>
<li>Second item</li>
</ul>
<h2>Subheading</h2>
<p>Another paragraph with a <a href="https://example.com">link</a>.</p>`

	reader := strings.NewReader(htmlInput)

	result, err := ConvertToMarkdown(logger, reader, "text/html", "")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Use a channel to handle timeout gracefully
	done := make(chan struct{})
	var output []byte
	var readErr error

	go func() {
		defer close(done)
		output, readErr = io.ReadAll(result)
	}()

	select {
	case <-done:
		if readErr != nil {
			t.Fatalf("Failed to read result: %v", readErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out waiting for pandoc conversion")
	}

	outputStr := string(output)
	t.Logf("Pandoc output: %s", outputStr)

	// Check that pandoc converted HTML elements to markdown
	if !strings.Contains(outputStr, "Main Heading") {
		t.Error("Expected heading text to be preserved")
	}
	if !strings.Contains(outputStr, "bold") {
		t.Error("Expected bold text to be preserved")
	}
	if !strings.Contains(outputStr, "italic") {
		t.Error("Expected italic text to be preserved")
	}
	if !strings.Contains(outputStr, "Subheading") {
		t.Error("Expected subheading text to be preserved")
	}
	if !strings.Contains(outputStr, "example.com") {
		t.Error("Expected link URL to be preserved")
	}

	// Check for list items
	if !strings.Contains(outputStr, "First item") || !strings.Contains(outputStr, "Second item") {
		t.Error("Expected list items to be preserved")
	}
}

func TestConvertToMarkdown_HTMLSimple(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Test with simple HTML
	htmlInput := `<h1>Hello World</h1><p>This is a test.</p>`
	reader := strings.NewReader(htmlInput)

	result, err := ConvertToMarkdown(logger, reader, "text/html", "")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Use a channel to handle timeout gracefully
	done := make(chan struct{})
	var output []byte
	var readErr error

	go func() {
		defer close(done)
		output, readErr = io.ReadAll(result)
	}()

	select {
	case <-done:
		if readErr != nil {
			t.Fatalf("Failed to read result: %v", readErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out waiting for pandoc conversion")
	}

	outputStr := string(output)
	t.Logf("Pandoc output: %s", outputStr)

	// Verify basic HTML to markdown conversion
	if !strings.Contains(outputStr, "Hello World") {
		t.Error("Expected heading text to be preserved")
	}
	if !strings.Contains(outputStr, "This is a test") {
		t.Error("Expected paragraph content to be preserved")
	}
}

func TestConvertToMarkdown_FileExtensions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	tests := []struct {
		name               string
		contentType        string
		contentDisposition string
		input              string
		expectedContains   []string
	}{
		{
			name:               "Markdown file",
			contentType:        "application/octet-stream",
			contentDisposition: `attachment; filename="test.md"`,
			input:              "# Test Markdown",
			expectedContains:   []string{"# Test Markdown"},
		},
		{
			name:               "Text file",
			contentType:        "application/octet-stream",
			contentDisposition: `attachment; filename="test.txt"`,
			input:              "Plain text content",
			expectedContains:   []string{"```text", "Plain text content", "```"},
		},
		{
			name:               "JSON file",
			contentType:        "application/octet-stream",
			contentDisposition: `attachment; filename="test.json"`,
			input:              `{"key": "value"}`,
			expectedContains:   []string{"```json", "\"key\": \"value\"", "```"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)

			result, err := ConvertToMarkdown(logger, reader, tt.contentType, tt.contentDisposition)
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}

			output, err := io.ReadAll(result)
			if err != nil {
				t.Fatalf("Failed to read result: %v", err)
			}

			outputStr := string(output)
			for _, expected := range tt.expectedContains {
				if !strings.Contains(outputStr, expected) {
					t.Errorf("Expected output to contain '%s', got: %s", expected, outputStr)
				}
			}
		})
	}
}

func TestConvertToMarkdown_UnsupportedType(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reader := strings.NewReader("some content")

	_, err := ConvertToMarkdown(logger, reader, "application/unknown", "")
	if err == nil {
		t.Error("Expected error for unsupported content type")
	}
}

func TestExtractFromJson(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple JSON",
			input:    `{"name":"test","value":123}`,
			expected: "```json\n{\n  \"name\": \"test\",\n  \"value\": 123\n}\n```",
		},
		{
			name:     "Nested JSON",
			input:    `{"user":{"name":"John","age":30},"active":true}`,
			expected: "```json\n{\n  \"user\": {\n    \"name\": \"John\",\n    \"age\": 30\n  },\n  \"active\": true\n}\n```",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			result, err := extractFromJson(reader)
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}

			output, err := io.ReadAll(result)
			if err != nil {
				t.Fatalf("Failed to read result: %v", err)
			}

			if string(output) != tt.expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", tt.expected, string(output))
			}
		})
	}
}

func TestExtractFromJson_InvalidJSON(t *testing.T) {
	reader := strings.NewReader(`{"invalid": json}`)
	_, err := extractFromJson(reader)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestExtractWithPandoc_HTMLFormats(t *testing.T) {
	tests := []struct {
		name     string
		format   string
		input    string
		expected string
	}{
		{
			name:     "HTML with heading",
			format:   "html",
			input:    "<h1>Test Heading</h1><p>Test paragraph</p>",
			expected: "# Test Heading\n\nTest paragraph\n",
		},
		{
			name:     "HTML with table",
			format:   "html",
			input:    "<table><tr><th>Header</th></tr><tr><td>Data</td></tr></table>",
			expected: "  Header\n  --------\n  Data\n",
		},
		{
			name:     "HTML with code block",
			format:   "html",
			input:    "<pre><code>function test() { return true; }</code></pre>",
			expected: "    function test() { return true; }\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			result, err := extractWithPandoc(reader, tt.format)

			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}

			output, readErr := io.ReadAll(result)

			if readErr != nil {
				t.Fatalf("Failed to read result: %v", readErr)
			}

			assert.Equal(t, tt.expected, string(output))

		})
	}
}

// Test pandoc directly to isolate issues
func TestPandocDirect(t *testing.T) {
	htmlInput := "<h1>Test</h1><p>This is a test.</p>"

	// Test pandoc command directly
	cmd := exec.Command("pandoc", "--from", "html", "--to", "markdown")
	cmd.Stdin = strings.NewReader(htmlInput)

	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Direct pandoc test failed: %v", err)
	}

	outputStr := string(output)
	t.Logf("Direct pandoc output: %s", outputStr)

	if !strings.Contains(outputStr, "Test") {
		t.Error("Expected 'Test' in pandoc output")
	}
}

// Test the extractWithPandoc function with a simpler approach
func TestExtractWithPandoc_Simple(t *testing.T) {
	htmlInput := "<h1>Simple Test</h1>"
	reader := strings.NewReader(htmlInput)

	result, err := extractWithPandoc(reader, "html")
	if err != nil {
		t.Fatalf("extractWithPandoc failed: %v", err)
	}

	// Read with timeout
	done := make(chan []byte, 1)
	errChan := make(chan error, 1)

	go func() {
		output, readErr := io.ReadAll(result)
		if readErr != nil {
			errChan <- readErr
			return
		}
		done <- output
	}()

	select {
	case output := <-done:
		outputStr := string(output)
		t.Logf("extractWithPandoc output: %s", outputStr)
		if !strings.Contains(outputStr, "Simple Test") {
			t.Error("Expected 'Simple Test' in output")
		}
	case err := <-errChan:
		t.Fatalf("Failed to read from extractWithPandoc: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("extractWithPandoc test timed out")
	}
}

// Benchmark test for HTML conversion
func BenchmarkConvertToMarkdown_HTML(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	htmlInput := `<!DOCTYPE html>
<html>
<head><title>Benchmark Test</title></head>
<body>
    <h1>Performance Test</h1>
    <p>This is a performance test with <strong>bold</strong> and <em>italic</em> text.</p>
    <ul><li>Item 1</li><li>Item 2</li><li>Item 3</li></ul>
</body>
</html>`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(htmlInput)
		result, err := ConvertToMarkdown(logger, reader, "text/html", "")
		if err != nil {
			b.Fatalf("Error in benchmark: %v", err)
		}

		// Read the result to ensure full processing
		_, err = io.ReadAll(result)
		if err != nil {
			b.Fatalf("Error reading result: %v", err)
		}
	}
}
