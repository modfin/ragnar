package dao

import (
	"fmt"
	"strings"

	"github.com/modfin/ragnar"
)

// documentFilterSQL turns a filter into " AND ..." conditions on the
// "document" table, numbering its placeholders from argIndex. document_id is
// matched against the column; every other key against the document headers.
func documentFilterSQL(filter ragnar.DocumentFilter, argIndex int) (string, []any, error) {
	var q strings.Builder
	var args []any
	i := argIndex

	for _, filterValue := range filter["document_id"] {
		switch {
		case filterValue.Simple != nil:
			fmt.Fprintf(&q, " AND document.document_id = $%d \n", i)
			args = append(args, *filterValue.Simple)
		case filterValue.Array != nil:
			fmt.Fprintf(&q, " AND document.document_id = ANY($%d) \n", i)
			args = append(args, filterValue.Array)
		case filterValue.Condition != nil:
			op, err := filterOperatorSQL(filterValue.Condition.Operator)
			if err != nil {
				return "", nil, err
			}
			fmt.Fprintf(&q, " AND document.document_id %s $%d \n", op, i)
			args = append(args, filterValue.Condition.Value)
		default:
			continue
		}
		i++
	}

	for fieldName, filterValues := range filter {
		if fieldName == "document_id" {
			continue
		}
		fieldName = strings.ToLower(fieldName)

		// Multiple conditions on one field are AND-ed together.
		for _, filterValue := range filterValues {
			switch {
			case filterValue.Simple != nil:
				fmt.Fprintf(&q, " AND document.headers -> $%d = $%d \n", i, i+1)
				args = append(args, fieldName, *filterValue.Simple)
			case filterValue.Array != nil:
				fmt.Fprintf(&q, " AND document.headers -> $%d = ANY($%d) \n", i, i+1)
				args = append(args, fieldName, filterValue.Array)
			case filterValue.Condition != nil:
				leftSide := fmt.Sprintf("document.headers -> $%d", i)
				rightSide := fmt.Sprintf("$%d", i+1)
				switch filterValue.Condition.ValueType {
				case ragnar.ValueTypeInteger:
					leftSide = fmt.Sprintf("CAST(document.headers -> $%d AS INTEGER)", i)
					rightSide = fmt.Sprintf("CAST($%d AS INTEGER)", i+1)
				case ragnar.ValueTypeNumeric:
					leftSide = fmt.Sprintf("CAST(document.headers -> $%d AS NUMERIC)", i)
					rightSide = fmt.Sprintf("CAST($%d AS NUMERIC)", i+1)
				}
				op, err := filterOperatorSQL(filterValue.Condition.Operator)
				if err != nil {
					return "", nil, err
				}
				fmt.Fprintf(&q, " AND %s %s %s \n", leftSide, op, rightSide)
				args = append(args, fieldName, filterValue.Condition.Value)
			default:
				continue
			}
			i += 2
		}
	}
	return q.String(), args, nil
}

func filterOperatorSQL(op ragnar.FilterOperator) (string, error) {
	switch op {
	case ragnar.OpEqual, ragnar.OpIn: // $in in a condition holds a single value
		return "=", nil
	case ragnar.OpGreaterThan:
		return ">", nil
	case ragnar.OpGreaterThanOrEqual:
		return ">=", nil
	case ragnar.OpLessThan:
		return "<", nil
	case ragnar.OpLessThanOrEqual:
		return "<=", nil
	default:
		return "", fmt.Errorf("unsupported operator: %s", op)
	}
}
