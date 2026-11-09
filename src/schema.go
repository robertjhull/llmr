package llmr

import (
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

type CompiledSchema struct {
	schema *jsonschema.Schema
	raw    json.RawMessage
}

func CompileSchema(schema json.RawMessage) (*CompiledSchema, error) {
	sch, err := jsonschema.CompileString("final_response_format.json", string(schema))
	if err != nil {
		return nil, fmt.Errorf("invalid final_response_format: %w", err)
	}
	return &CompiledSchema{schema: sch, raw: schema}, nil
}

func (s *CompiledSchema) ValidateText(text string) error {
	var parsed any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		return fmt.Errorf("response is not valid JSON: %w", err)
	}
	return s.schema.Validate(parsed)
}

func (s *CompiledSchema) Raw() json.RawMessage {
	return s.raw
}
