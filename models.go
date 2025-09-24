package ragnar

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"strings"
	"time"
)

type AccessToken struct {
	AccessKeyId     string     `db:"access_key_id" json:"access_key_id"`
	AccessKey       string     `db:"access_key" json:"access_key"`
	TokenName       string     `db:"token_name" json:"token_name"`
	AllowCreateTubs bool       `db:"allow_create_tubs" json:"allow_create_tubs"`
	AllowReadTubs   bool       `db:"allow_read_tubs" json:"allow_read_tubs"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at" json:"updated_at"`
	DeletedAt       *time.Time `db:"deleted_at" json:"-"`
}

type Tub struct {
	TubId     string        `db:"tub_id" json:"tub_id"`
	TubName   string        `db:"tub_name" json:"tub_name"`
	Settings  pgtype.Hstore `db:"settings" json:"settings"`
	CreatedAt time.Time     `db:"created_at" json:"created_at"`
	UpdatedAt time.Time     `db:"updated_at" json:"updated_at"`
	DeletedAt *time.Time    `db:"deleted_at" json:"-"`
}

func (t Tub) WithRequiredDocumentHeaders(headers ...string) Tub {
	if t.Settings == nil {
		t.Settings = make(map[string]*string)
	}
	requiredHeaders := strings.Join(headers, ",")
	t.Settings["required_document_headers"] = &requiredHeaders
	return t
}

func (t Tub) GetRequiredDocumentHeaders() []string {
	if t.Settings == nil {
		return nil
	}
	val, ok := t.Settings["required_document_headers"]
	if !ok || val == nil {
		return nil
	}
	return strings.Split(*val, ",")
}

type Document struct {
	DocumentId string `db:"document_id" json:"document_id" json-description:"Document uuid"`
	TubId      string `db:"tub_id" json:"tub_id" json-description:"Tub id"`
	TubName    string `db:"tub_name" json:"tub_name" json-description:"Tub name"`

	Headers pgtype.Hstore `db:"headers" json:"headers" json-description:"Document header"`

	CreatedAt time.Time `db:"created_at" json:"created_at" json-description:"Created at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at" json-description:"Updated at"`
}

type DocumentStatus struct {
	Status string `db:"status" json:"status" json-description:"Document status" json-enum:"pending,processing,completed,failed"`
}

type Chunk struct {
	TubId      string `db:"tub_id" json:"tub_id" json-description:"Tub id"`
	TubName    string `db:"tub_name" json:"tub_name" json-description:"Tub name"`
	DocumentId string `db:"document_id" json:"document_id" json-description:"Document uuid"`
	ChunkId    int    `db:"chunk_id" json:"chunk_id" json-description:"Chunk identifier"`

	Context string `db:"context" json:"context" json-description:"The context of the chunk, if any"`
	Content string `db:"content" json:"content" json-description:"Fetched chunk content"`

	CreatedAt time.Time `db:"created_at" json:"created_at" json-description:"Created at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at" json-description:"Updated at"`
}

type ChunkReference struct {
	TubId      string `db:"tub_id" json:"tub_id" json-description:"Tub id"`
	TubName    string `db:"tub_name" json:"tub_name" json-description:"Tub name"`
	DocumentId string `db:"document_id" json:"document_id" json-description:"Document uuid"`
	ChunkId    int    `db:"chunk_id" json:"chunk_id" json-description:"Chunk identifier"`
}

type HStore map[string]any

func (j *HStore) Scan(value any) error {
	if value == nil {
		*j = nil
		return nil
	}
	hstoreString, ok := value.(string)
	if !ok {
		return nil
	}
	if hstoreString == "" {
		*j = map[string]any{}
		return nil
	}
	result := make(map[string]any)
	err := json.Unmarshal([]byte(fmt.Sprintf(`{%s}`, strings.ReplaceAll(hstoreString, "=>", ":"))), &result)
	if err != nil {
		return fmt.Errorf("error unmarshalling hstore: %w", err)
	}
	*j = result
	return nil
}

func (j HStore) Value() (driver.Value, error) {
	if j == nil {
		return "", nil
	}
	// create hstore string
	var parts []string
	for key, val := range j {
		quotedKey := fmt.Sprintf(`"%s"`, strings.ReplaceAll(key, `"`, `\"`))

		if val != nil {
			quotedValue := fmt.Sprintf(`"%s"`, strings.ReplaceAll(fmt.Sprintf("%v", val), `"`, `\"`))
			parts = append(parts, fmt.Sprintf("%s => %s", quotedKey, quotedValue))
		} else {
			parts = append(parts, fmt.Sprintf("%s => NULL", quotedKey))
		}
	}

	return strings.Join(parts, ", "), nil
}
