package convert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

func formatJSON(value []byte, pretty bool) (string, error) {
	if offset := invalidUTF8Offset(value); offset >= 0 {
		return "", offsetError("format JSON", offset, ErrInvalidPayload, "invalid UTF-8 sequence")
	}
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || !json.Valid(trimmed) {
		return "", fmt.Errorf("%w: malformed JSON or trailing non-whitespace content", ErrInvalidPayload)
	}

	var output bytes.Buffer
	var err error
	if pretty {
		err = json.Indent(&output, trimmed, "", "  ")
	} else {
		err = json.Compact(&output, trimmed)
	}
	if err != nil {
		return "", fmt.Errorf("%w: format JSON: %v", ErrInvalidPayload, err)
	}
	if !utf8.Valid(output.Bytes()) {
		return "", fmt.Errorf("%w: formatted JSON is not UTF-8", ErrInvalidPayload)
	}
	return output.String(), nil
}
