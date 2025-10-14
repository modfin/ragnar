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

// FilterOperator represents a comparison operator for document filtering
type FilterOperator string

const (
	OpEqual              FilterOperator = "$eq"  // Equal to
	OpGreaterThan        FilterOperator = "$gt"  // Greater than
	OpGreaterThanOrEqual FilterOperator = "$gte" // Greater than or equal
	OpLessThan           FilterOperator = "$lt"  // Less than
	OpLessThanOrEqual    FilterOperator = "$lte" // Less than or equal
	OpIn                 FilterOperator = "$in"  // In array (contains)
)

// ValueType represents how the value should be compared in the database
type ValueType string

const (
	ValueTypeText    ValueType = "text"    // Compare as text (default)
	ValueTypeInteger ValueType = "integer" // Compare as integer
	ValueTypeNumeric ValueType = "numeric" // Compare as decimal/float
)

// FilterCondition represents a single filter condition with an operator and value
type FilterCondition struct {
	Operator  FilterOperator `json:"operator"`
	Value     string         `json:"value"`
	ValueType ValueType      `json:"type,omitempty"` // Optional type hint for comparison, defaults to text
}

// FilterValue can be either a simple string (for equality), an array of strings (for $in),
// or a FilterCondition with an operator
type FilterValue struct {
	// Simple equality value
	Simple *string `json:"simple,omitempty"`
	// Array of values for $in operator
	Array []string `json:"array,omitempty"`
	// Condition with operator
	Condition *FilterCondition `json:"condition,omitempty"`
}

// DocumentFilter represents filters for document queries based on headers
type DocumentFilter map[string]FilterValue

func NewDocumentFilter() DocumentFilter {
	return make(DocumentFilter)
}

// UnmarshalJSON implements custom JSON unmarshaling for DocumentFilter
func (df *DocumentFilter) UnmarshalJSON(data []byte) error {
	// First unmarshal into a generic map
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	result := make(DocumentFilter)
	for key, val := range raw {
		fv := FilterValue{}

		switch v := val.(type) {
		case string:
			// Simple string value for equality
			fv.Simple = &v
		case []interface{}:
			// Array for $in operator
			arr := make([]string, len(v))
			for i, item := range v {
				if str, ok := item.(string); ok {
					arr[i] = str
				} else {
					return fmt.Errorf("array values must be strings for field %s", key)
				}
			}
			fv.Array = arr
		case map[string]interface{}:
			// Operator-based condition
			var operator FilterOperator
			var value string
			var valueType ValueType = ValueTypeText // default to text

			for op, val := range v {
				if op == "type" {
					// Handle type hint
					if typeStr, ok := val.(string); ok {
						valueType = ValueType(typeStr)
					}
					continue
				}
				strVal, ok := val.(string)
				if !ok {
					return fmt.Errorf("operator value must be a string for field %s", key)
				}
				operator = FilterOperator(op)
				value = strVal
			}

			if operator == "" {
				return fmt.Errorf("filter condition must have an operator for field %s", key)
			}

			fv.Condition = &FilterCondition{
				Operator:  operator,
				Value:     value,
				ValueType: valueType,
			}
		default:
			return fmt.Errorf("unsupported filter value type for field %s", key)
		}

		result[key] = fv
	}

	*df = result
	return nil
}

// MarshalJSON implements custom JSON marshaling for DocumentFilter
func (df DocumentFilter) MarshalJSON() ([]byte, error) {
	result := make(map[string]interface{})

	for key, fv := range df {
		if fv.Simple != nil {
			result[key] = *fv.Simple
		} else if fv.Array != nil {
			result[key] = fv.Array
		} else if fv.Condition != nil {
			conditionMap := map[string]string{
				string(fv.Condition.Operator): fv.Condition.Value,
			}
			// Only include type if it's not the default (text)
			if fv.Condition.ValueType != "" && fv.Condition.ValueType != ValueTypeText {
				conditionMap["type"] = string(fv.Condition.ValueType)
			}
			result[key] = conditionMap
		}
	}

	return json.Marshal(result)
}

func (df DocumentFilter) WithEqual(field, value string) DocumentFilter {
	if df == nil {
		df = NewDocumentFilter()
	}
	df[field] = FilterValue{Simple: &value}
	return df
}

func (df DocumentFilter) WithIn(field string, values []string) DocumentFilter {
	if df == nil {
		df = NewDocumentFilter()
	}
	df[field] = FilterValue{Array: values}
	return df
}

func (df DocumentFilter) WithCondition(field string, operator FilterOperator, value string, valueType ValueType) DocumentFilter {
	if df == nil {
		df = NewDocumentFilter()
	}
	df[field] = FilterValue{Condition: &FilterCondition{
		Operator:  operator,
		Value:     value,
		ValueType: valueType,
	}}
	return df
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
