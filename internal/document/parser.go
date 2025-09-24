package document

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func ConvertToMarkdown(log *slog.Logger, reader io.Reader, contentType, contentDisposition string) (io.Reader, error) {
	var err error
	// Dispatch based on the detected content type
	contentType, _, _ = strings.Cut(contentType, ";") // Remove charset if present

	switch contentType {
	case "application/json": // This is probably not something we want to do in general
		log.Debug("content detected as JSON.")
		return extractFromJson(reader)
	case "text/plain":
		log.Debug("content detected as plain text.")
		return io.MultiReader(bytes.NewBufferString("```text\n"), reader, bytes.NewBufferString("\n```")), nil
	case "text/html":
		log.Debug("content detected as html.")
		return extractFromHTML(reader)
	case "application/pdf":
		log.Debug("content detected as PDF.")
		return extractFromPDF(reader)
	case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
		log.Debug("content detected as docx.")
		return extractFromDocx(reader)
	case "application/vnd.oasis.opendocument.text":
		log.Debug("content detected as odt.")
		return extractFromOdt(reader)
	}

	_, params, err := mime.ParseMediaType(contentDisposition)
	if err != nil {
		return nil, fmt.Errorf("error parsing content disposition: %v", err)
	}
	filename, ok := params["filename"]
	if !ok {
		return nil, fmt.Errorf("could not determine file extension from content disposition")
	}
	extension := filepath.Ext(filename)

	switch extension {
	case ".md":
		log.Debug("content detected as markdown from extension.")
		return reader, nil
	case ".txt", ".text":
		log.Debug("content detected as plain text from extension.")
		return io.MultiReader(bytes.NewBufferString("```text\n"), reader, bytes.NewBufferString("\n```")), nil

	case ".json":
		log.Debug("content detected as JSON from extension.")
		return extractFromJson(reader)
	case ".odt":
		log.Debug("content detected as odt from extension.")
		return extractFromOdt(reader)
	case ".docx":
		log.Debug("content detected as docx from extension.")
		return extractFromDocx(reader)
	case ".pdf":
		log.Debug("content detected as pdf from extension.")
		return extractFromPDF(reader)
	}

	return nil, fmt.Errorf("unsupported file: %w", err) // return err
}

func extractFromPDF(in io.Reader) (io.Reader, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // 30 sec time limit

	// The command is `pdftotext [options] [PDF-file] [text-file]`: Using `-` tells it to read from stdin/write to stdout
	cmd := exec.CommandContext(ctx, "pdftotext", "-", "-")

	// Set up buffers to capture the command's output and errors
	var stderr bytes.Buffer

	out, stdout := io.Pipe()

	cmd.Stdout = stdout
	cmd.Stderr = &stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		defer cancel()
		return nil, fmt.Errorf("error creating stdin pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		defer cancel()
		defer stdout.Close()
		return nil, fmt.Errorf("failed to start pdftotext command: %w", err)
	}

	go func() {
		defer stdin.Close()
		_, err := io.Copy(stdin, in)
		if err != nil {
			stdout.CloseWithError(fmt.Errorf("error copying to stdin: %v", err))
			return
		}
	}()

	go func() {
		defer cancel()
		defer stdout.Close()

		err := cmd.Wait()
		if err != nil {
			stdout.CloseWithError(fmt.Errorf("pdftotext execution error: %w, stderr: %s", err, stderr.String()))
			return
		}

	}()

	return out, nil
}

func extractFromJson(reader io.Reader) (io.Reader, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading body: %v", err)
	}
	var buff bytes.Buffer
	err = json.Indent(&buff, content, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("error indenting JSON: %v", err)
	}
	return io.MultiReader(bytes.NewBufferString("```json\n"), &buff, bytes.NewBufferString("\n```")), nil
}

// Separate function to allow easy implementation swapping later
func extractFromDocx(data io.Reader) (io.Reader, error) {
	return extractWithPandoc(data, "docx")
}

// Separate function to allow easy implementation swapping later
func extractFromHTML(data io.Reader) (io.Reader, error) {
	return extractWithPandoc(data, "html")
}

func extractFromOdt(data io.Reader) (io.Reader, error) {
	return extractWithPandoc(data, "odt")
}

func extractWithPandoc(in io.Reader, fromFormat string) (io.Reader, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	args := []string{"--to", "markdown"}
	if fromFormat != "" {
		args = append([]string{"--from", fromFormat}, args...) // If a format is specified, add the --from flag
	}

	// The command is `pandoc --from odt --to markdown`: stdin/stdout is default
	cmd := exec.CommandContext(ctx, "pandoc", args...)

	// Set up pipes for stdin and stdout

	var stderr bytes.Buffer
	out, stdout := io.Pipe()

	cmd.Stdout = stdout
	cmd.Stderr = &stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		defer cancel()
		return nil, fmt.Errorf("error creating stdin pipe: %v", err)
	}

	err = cmd.Start()
	if err != nil {
		defer cancel()
		return nil, fmt.Errorf("pandoc command failed: %w\nstderr: %s", err, stderr.String())
	}

	go func() {
		defer stdin.Close()
		_, err := io.Copy(stdin, in)
		if err != nil {
			stdout.CloseWithError(fmt.Errorf("error copying to stdin: %v", err))
			return
		}
	}()

	// Wait for command completion in another goroutine
	go func() {
		defer cancel()
		defer stdout.Close()

		err := cmd.Wait()
		if err != nil {
			stdout.CloseWithError(fmt.Errorf("pancoc execution error: %w, stderr: %s", err, stderr.String()))
		}
	}()

	return out, nil
}
