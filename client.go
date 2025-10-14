package ragnar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
)

type Client interface {
	GetTubs(ctx context.Context) ([]Tub, error)                                                                                                                                  // Get /tubs
	CreateTub(ctx context.Context, tub Tub) (Tub, error)                                                                                                                         // Post /tubs
	GetTub(ctx context.Context, tub string) (Tub, error)                                                                                                                         // Get /tubs/{tub}
	UpdateTub(ctx context.Context, tub Tub) (Tub, error)                                                                                                                         // Put /tubs/{tub}
	DeleteTub(ctx context.Context, tub string) (Tub, error)                                                                                                                      // Delete /tubs/{tub}
	GetTubDocuments(ctx context.Context, tub string, filter DocumentFilter, limit, offset int) ([]Document, error)                                                               // Get /tubs/{tub}/documents
	GetTubDocument(ctx context.Context, tub, documentId string) (Document, error)                                                                                                // Get /tubs/{tub}/documents/{document_id}
	GetTubDocumentStatus(ctx context.Context, tub, documentId string) (DocumentStatus, error)                                                                                    // Get /tubs/{tub}/documents/{document_id}
	CreateTubDocument(ctx context.Context, tub string, data io.Reader, headers map[string]string) (Document, error)                                                              // Post /tubs/{tub}/documents
	CreateTubDocumentWithOptionals(ctx context.Context, tub string, file io.Reader, markdown io.Reader, chunks []Chunk, headers map[string]string) (Document, error)             // Post /tubs/{tub}/documents (multipart)
	UpdateTubDocument(ctx context.Context, tub, documentId string, data io.Reader, headers map[string]string) (Document, error)                                                  // Put /tubs/{tub}/documents/{document_id}
	UpdateTubDocumentWithOptionals(ctx context.Context, tub, documentId string, file io.Reader, markdown io.Reader, chunks []Chunk, headers map[string]string) (Document, error) // Put /tubs/{tub}/documents/{document_id} (multipart)
	DownloadTubDocument(ctx context.Context, tub, documentId string) (io.ReadCloser, error)                                                                                      // Get /tubs/{tub}/documents/{document_id}/download
	DownloadTubDocumentMarkdown(ctx context.Context, tub, documentId string) (io.ReadCloser, error)                                                                              // Get /tubs/{tub}/documents/{document_id}/download/markdown
	DeleteTubDocument(ctx context.Context, tub, documentId string) error                                                                                                         // Delete /tubs/{tub}/documents/{document_id}
	GetTubDocumentChunks(ctx context.Context, tub, documentId string, limit, offset int) ([]Chunk, error)                                                                        // Get /tubs/{tub}/documents/{document_id}/chunks
	GetTubDocumentChunk(ctx context.Context, tub, documentId string, index int) (Chunk, error)                                                                                   // Get /tubs/{tub}/document/{document_id}/chunks/{index}
	SearchTubDocumentChunks(ctx context.Context, tub, query string, documentFilter DocumentFilter, limit, offset int) ([]Chunk, error)                                           // Get /search/xnn/{tub}
}

type httpClient struct {
	client    *http.Client
	baseURL   string
	accessKey string
}

type ClientConfig struct {
	BaseURL    string
	AccessKey  string
	HTTPClient *http.Client
}

func NewClient(config ClientConfig) Client {
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{}
	}
	return &httpClient{
		client:    config.HTTPClient,
		baseURL:   config.BaseURL,
		accessKey: config.AccessKey,
	}
}

func (c *httpClient) doRequest(ctx context.Context, method, path string, body io.Reader, qp, headers map[string]string) (*http.Response, error) {
	u, err := url.JoinPath(c.baseURL, path)
	if err != nil {
		return nil, fmt.Errorf("failed to join URL path: %w", err)
	}
	if len(qp) > 0 {
		q := url.Values{}
		for key, value := range qp {
			q.Set(key, value)
		}
		u += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.accessKey)

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return resp, nil
}

func (c *httpClient) doJSONRequest(ctx context.Context, method, path string, qp map[string]string, reqBody, respBody interface{}) error {
	var body io.Reader
	if reqBody != nil {
		jsonData, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		body = bytes.NewReader(jsonData)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}

	resp, err := c.doRequest(ctx, method, path, body, qp, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errRespString string
		if resp.Body != nil {
			data, _ := io.ReadAll(resp.Body)
			if len(data) > 0 {
				errRespString = string(data)
			}
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, errRespString)
	}

	if respBody != nil {
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

func (c *httpClient) GetTubs(ctx context.Context) ([]Tub, error) {
	var tubs []Tub
	err := c.doJSONRequest(ctx, "GET", "/tubs", nil, nil, &tubs)
	return tubs, err
}

func (c *httpClient) CreateTub(ctx context.Context, tub Tub) (Tub, error) {
	var result Tub
	err := c.doJSONRequest(ctx, "POST", "/tubs", nil, tub, &result)
	return result, err
}

func (c *httpClient) GetTub(ctx context.Context, tub string) (Tub, error) {
	var result Tub
	err := c.doJSONRequest(ctx, "GET", fmt.Sprintf("/tubs/%s", url.PathEscape(tub)), nil, nil, &result)
	return result, err
}

func (c *httpClient) UpdateTub(ctx context.Context, tub Tub) (Tub, error) {
	var result Tub
	err := c.doJSONRequest(ctx, "PUT", fmt.Sprintf("/tubs/%s", url.PathEscape(tub.TubName)), nil, tub, &result)
	return result, err
}

func (c *httpClient) DeleteTub(ctx context.Context, tub string) (Tub, error) {
	var result Tub
	err := c.doJSONRequest(ctx, "DELETE", fmt.Sprintf("/tubs/%s", url.PathEscape(tub)), nil, nil, &result)
	return result, err
}

func (c *httpClient) GetTubDocuments(ctx context.Context, tub string, filter DocumentFilter, limit, offset int) ([]Document, error) {
	path := fmt.Sprintf("/tubs/%s/documents", url.PathEscape(tub))

	params := map[string]string{}
	if filter != nil && len(filter) > 0 {
		filterData, err := json.Marshal(filter)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal filter: %w", err)
		}
		params["filter"] = string(filterData)
	}
	if limit > 0 {
		params["limit"] = strconv.Itoa(limit)
	}
	if offset > 0 {
		params["offset"] = strconv.Itoa(offset)
	}

	var documents []Document
	err := c.doJSONRequest(ctx, "GET", path, params, nil, &documents)
	return documents, err
}

func (c *httpClient) GetTubDocument(ctx context.Context, tub, documentId string) (Document, error) {
	var document Document
	err := c.doJSONRequest(ctx, "GET", fmt.Sprintf("/tubs/%s/documents/%s", url.PathEscape(tub), url.PathEscape(documentId)), nil, nil, &document)
	return document, err
}

func (c *httpClient) GetTubDocumentStatus(ctx context.Context, tub, documentId string) (DocumentStatus, error) {
	var documentStatus DocumentStatus
	err := c.doJSONRequest(ctx, "GET", fmt.Sprintf("/tubs/%s/documents/%s/status", url.PathEscape(tub), url.PathEscape(documentId)), nil, nil, &documentStatus)
	return documentStatus, err
}

func (c *httpClient) CreateTubDocument(ctx context.Context, tub string, data io.Reader, headers map[string]string) (Document, error) {
	if headers == nil {
		headers = make(map[string]string)
	}

	resp, err := c.doRequest(ctx, "POST", fmt.Sprintf("/tubs/%s/documents", url.PathEscape(tub)), data, nil, headers)
	if err != nil {
		return Document{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return Document{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	var document Document
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		return Document{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return document, nil
}

func (c *httpClient) UpdateTubDocument(ctx context.Context, tub, documentId string, data io.Reader, headers map[string]string) (Document, error) {
	if headers == nil {
		headers = make(map[string]string)
	}

	resp, err := c.doRequest(ctx, "PUT", fmt.Sprintf("/tubs/%s/documents/%s", url.PathEscape(tub), url.PathEscape(documentId)), data, nil, headers)
	if err != nil {
		return Document{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return Document{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	var document Document
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		return Document{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return document, nil
}

func (c *httpClient) DownloadTubDocument(ctx context.Context, tub, documentId string) (io.ReadCloser, error) {
	resp, err := c.doRequest(ctx, "GET", fmt.Sprintf("/tubs/%s/documents/%s/download", url.PathEscape(tub), url.PathEscape(documentId)), nil, nil, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	return resp.Body, nil
}

func (c *httpClient) DownloadTubDocumentMarkdown(ctx context.Context, tub, documentId string) (io.ReadCloser, error) {
	resp, err := c.doRequest(ctx, "GET", fmt.Sprintf("/tubs/%s/documents/%s/download/markdown", url.PathEscape(tub), url.PathEscape(documentId)), nil, nil, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	return resp.Body, nil
}

func (c *httpClient) DeleteTubDocument(ctx context.Context, tub, documentId string) error {
	resp, err := c.doRequest(ctx, "DELETE", fmt.Sprintf("/tubs/%s/documents/%s", url.PathEscape(tub), url.PathEscape(documentId)), nil, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	return nil
}

func (c *httpClient) GetTubDocumentChunks(ctx context.Context, tub, documentId string, limit, offset int) ([]Chunk, error) {
	path := fmt.Sprintf("/tubs/%s/documents/%s/chunks", url.PathEscape(tub), url.PathEscape(documentId))

	params := map[string]string{}
	if limit > 0 {
		params["limit"] = strconv.Itoa(limit)
	}
	if offset > 0 {
		params["offset"] = strconv.Itoa(offset)
	}

	var chunks []Chunk
	err := c.doJSONRequest(ctx, "GET", path, params, nil, &chunks)
	return chunks, err
}

func (c *httpClient) GetTubDocumentChunk(ctx context.Context, tub, documentId string, index int) (Chunk, error) {
	var chunk Chunk
	err := c.doJSONRequest(ctx, "GET", fmt.Sprintf("/tubs/%s/document/%s/chunks/%d", url.PathEscape(tub), url.PathEscape(documentId), index), nil, nil, &chunk)
	return chunk, err
}

func (c *httpClient) SearchTubDocumentChunks(ctx context.Context, tub, query string, documentFilter DocumentFilter, limit, offset int) ([]Chunk, error) {
	path := fmt.Sprintf("/search/xnn/%s", url.PathEscape(tub))

	params := map[string]string{}
	params["q"] = query
	if limit > 0 {
		params["limit"] = strconv.Itoa(limit)
	}
	if offset > 0 {
		params["offset"] = strconv.Itoa(offset)
	}
	if documentFilter != nil && len(documentFilter) > 0 {
		filterData, err := json.Marshal(documentFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal filter: %w", err)
		}
		params["filter"] = string(filterData)
	}

	var chunks []Chunk
	err := c.doJSONRequest(ctx, "GET", path, params, nil, &chunks)
	return chunks, err
}

// CreateTubDocumentWithOptionals creates a document with optional markdown and chunks using multipart form data
func (c *httpClient) CreateTubDocumentWithOptionals(ctx context.Context, tub string, file io.Reader, markdown io.Reader, chunks []Chunk, headers map[string]string) (Document, error) {
	return c.upsertTubDocumentWithOptionals(ctx, "POST", fmt.Sprintf("/tubs/%s/documents", url.PathEscape(tub)), file, markdown, chunks, headers)
}

// UpdateTubDocumentWithOptionals updates a document with optional markdown and chunks using multipart form data
func (c *httpClient) UpdateTubDocumentWithOptionals(ctx context.Context, tub, documentId string, file io.Reader, markdown io.Reader, chunks []Chunk, headers map[string]string) (Document, error) {
	return c.upsertTubDocumentWithOptionals(ctx, "PUT", fmt.Sprintf("/tubs/%s/documents/%s", url.PathEscape(tub), url.PathEscape(documentId)), file, markdown, chunks, headers)
}

// upsertTubDocumentWithOptionals is the common implementation for create/update with optionals
func (c *httpClient) upsertTubDocumentWithOptionals(ctx context.Context, method, path string, file io.Reader, markdown io.Reader, chunks []Chunk, headers map[string]string) (Document, error) {
	var bodyBuffer bytes.Buffer
	writer := multipart.NewWriter(&bodyBuffer)

	// Add file part (required)
	if file == nil {
		return Document{}, fmt.Errorf("file is required")
	}

	filePart, err := writer.CreateFormFile("file", "document")
	if err != nil {
		return Document{}, fmt.Errorf("failed to create file part: %w", err)
	}
	_, err = io.Copy(filePart, file)
	if err != nil {
		return Document{}, fmt.Errorf("failed to copy file data: %w", err)
	}

	// Add markdown part (optional)
	if markdown != nil {
		markdownPart, err := writer.CreateFormField("markdown")
		if err != nil {
			return Document{}, fmt.Errorf("failed to create markdown part: %w", err)
		}
		_, err = io.Copy(markdownPart, markdown)
		if err != nil {
			return Document{}, fmt.Errorf("failed to copy markdown data: %w", err)
		}
	}

	// Add chunks part (optional)
	if len(chunks) > 0 {
		chunksData, err := json.Marshal(chunks)
		if err != nil {
			return Document{}, fmt.Errorf("failed to marshal chunks: %w", err)
		}

		chunksPart, err := writer.CreateFormField("chunks")
		if err != nil {
			return Document{}, fmt.Errorf("failed to create chunks part: %w", err)
		}
		_, err = chunksPart.Write(chunksData)
		if err != nil {
			return Document{}, fmt.Errorf("failed to write chunks data: %w", err)
		}
	}

	err = writer.Close()
	if err != nil {
		return Document{}, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	// Prepare headers
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["Content-Type"] = writer.FormDataContentType()

	// Make the request
	resp, err := c.doRequest(ctx, method, path, &bodyBuffer, nil, headers)
	if err != nil {
		return Document{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errRespString string
		if resp.Body != nil {
			data, _ := io.ReadAll(resp.Body)
			if len(data) > 0 {
				errRespString = string(data)
			}
		}
		return Document{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, errRespString)
	}

	var document Document
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		return Document{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return document, nil
}
