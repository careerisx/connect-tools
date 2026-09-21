// Package convert provides byte-authoritative payload parsing and display
// conversions. None of its display helpers mutate the supplied byte slice.
package convert

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

var (
	// ErrInvalidPayload indicates malformed payload contents.
	ErrInvalidPayload = errors.New("invalid payload")
	// ErrInvalidEncoding indicates an unsupported payload encoding.
	ErrInvalidEncoding = errors.New("invalid payload encoding")
	// ErrInvalidCharset indicates an unsupported character set.
	ErrInvalidCharset = errors.New("invalid charset")
	// ErrInvalidFormat indicates an unsupported display format.
	ErrInvalidFormat = errors.New("invalid display format")
)

// OffsetError reports a zero-based byte offset in the caller's original
// string. It unwraps to the associated sentinel error.
type OffsetError struct {
	Operation string
	Offset    int
	Err       error
}

// Error implements error.
func (e *OffsetError) Error() string {
	return fmt.Sprintf("%s at offset %d: %v", e.Operation, e.Offset, e.Err)
}

// Unwrap returns the classifiable cause.
func (e *OffsetError) Unwrap() error {
	return e.Err
}

// Encoding identifies the syntax used for payload input.
type Encoding string

const (
	// EncodingText parses input using PayloadInput.Charset.
	EncodingText Encoding = "text"
	// EncodingHex parses hexadecimal digits with optional Unicode whitespace.
	EncodingHex Encoding = "hex"
	// EncodingBase64 parses standard padded Base64 syntax.
	EncodingBase64 Encoding = "base64"
)

// Charset identifies a supported text character set.
type Charset string

const (
	// CharsetUTF8 is strict UTF-8.
	CharsetUTF8 Charset = "utf-8"
	// CharsetASCII accepts only bytes in the seven-bit ASCII range.
	CharsetASCII Charset = "ascii"
	// CharsetGBK is the GBK simplified Chinese encoding.
	CharsetGBK Charset = "gbk"
)

// PayloadInput describes user-entered payload text and optional raw delimiter
// bytes appended after successful decoding.
type PayloadInput struct {
	Encoding        Encoding
	Data            string
	Charset         Charset
	AppendDelimiter []byte
}

// FormatKind identifies a byte-to-string display conversion.
type FormatKind string

const (
	// FormatText decodes bytes with Format.Charset.
	FormatText FormatKind = "text"
	// FormatHex emits contiguous uppercase hexadecimal digits.
	FormatHex FormatKind = "hex"
	// FormatBase64 emits standard padded Base64.
	FormatBase64 FormatKind = "base64"
	// FormatJSONPretty validates and indents one JSON value.
	FormatJSONPretty FormatKind = "json-pretty"
	// FormatJSONMinify validates and compacts one JSON value.
	FormatJSONMinify FormatKind = "json-minify"
	// FormatEscape emits a C-style byte escape representation.
	FormatEscape FormatKind = "escape"
)

// Format configures a byte-to-string conversion.
type Format struct {
	Kind    FormatKind
	Charset Charset
}

// Decode parses input into newly owned bytes. AppendDelimiter is appended only
// after the main input has been parsed successfully.
func Decode(input PayloadInput) ([]byte, error) {
	var (
		payload []byte
		err     error
	)
	switch input.Encoding {
	case EncodingText:
		payload, err = decodeText(input.Data, normalizeCharset(input.Charset))
	case EncodingHex:
		payload, err = decodeHex(input.Data)
	case EncodingBase64:
		payload, err = decodeBase64(input.Data)
	default:
		return nil, fmt.Errorf("%w: %q", ErrInvalidEncoding, input.Encoding)
	}
	if err != nil {
		return nil, err
	}

	result := make([]byte, 0, len(payload)+len(input.AppendDelimiter))
	result = append(result, payload...)
	result = append(result, input.AppendDelimiter...)
	return result, nil
}

// Encode converts bytes to a display string without modifying bytes.
func Encode(value []byte, format Format) (string, error) {
	switch format.Kind {
	case FormatText:
		return encodeText(value, normalizeCharset(format.Charset))
	case FormatHex:
		return strings.ToUpper(hex.EncodeToString(value)), nil
	case FormatBase64:
		return base64.StdEncoding.EncodeToString(value), nil
	case FormatJSONPretty:
		return formatJSON(value, true)
	case FormatJSONMinify:
		return formatJSON(value, false)
	case FormatEscape:
		return EncodeEscape(value), nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidFormat, format.Kind)
	}
}

func normalizeCharset(charset Charset) Charset {
	if charset == "" {
		return CharsetUTF8
	}
	return charset
}

func decodeText(value string, charset Charset) ([]byte, error) {
	if offset := invalidUTF8Offset([]byte(value)); offset >= 0 {
		return nil, offsetError("decode text", offset, ErrInvalidPayload, "invalid UTF-8 input")
	}
	switch charset {
	case CharsetUTF8:
		return []byte(value), nil
	case CharsetASCII:
		for offset, current := range []byte(value) {
			if current > 0x7f {
				return nil, offsetError("decode ASCII", offset, ErrInvalidPayload, "character is outside ASCII")
			}
		}
		return []byte(value), nil
	case CharsetGBK:
		encoded, consumed, err := transform.String(simplifiedchinese.GBK.NewEncoder(), value)
		if err != nil {
			return nil, offsetError("encode GBK", consumed, ErrInvalidPayload, err.Error())
		}
		return []byte(encoded), nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrInvalidCharset, charset)
	}
}

func encodeText(value []byte, charset Charset) (string, error) {
	switch charset {
	case CharsetUTF8:
		if offset := invalidUTF8Offset(value); offset >= 0 {
			return "", offsetError("decode UTF-8", offset, ErrInvalidPayload, "invalid UTF-8 sequence")
		}
		return string(value), nil
	case CharsetASCII:
		for offset, current := range value {
			if current > 0x7f {
				return "", offsetError("decode ASCII", offset, ErrInvalidPayload, "byte is outside ASCII")
			}
		}
		return string(value), nil
	case CharsetGBK:
		return decodeGBK(value)
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidCharset, charset)
	}
}

func decodeHex(value string) ([]byte, error) {
	digits := make([]byte, 0, len(value))
	offsets := make([]int, 0, len(value))
	for offset := 0; offset < len(value); {
		current, size := utf8.DecodeRuneInString(value[offset:])
		if current == utf8.RuneError && size == 1 {
			return nil, offsetError("decode HEX", offset, ErrInvalidPayload, "invalid UTF-8 input")
		}
		if isUnicodeSpace(current) {
			offset += size
			continue
		}
		if size != 1 || !isHexDigit(value[offset]) {
			return nil, offsetError("decode HEX", offset, ErrInvalidPayload, "invalid hexadecimal character")
		}
		digits = append(digits, value[offset])
		offsets = append(offsets, offset)
		offset += size
	}
	if len(digits)%2 != 0 {
		return nil, offsetError("decode HEX", offsets[len(offsets)-1], ErrInvalidPayload, "odd number of hexadecimal digits")
	}
	decoded := make([]byte, hex.DecodedLen(len(digits)))
	_, _ = hex.Decode(decoded, digits)
	return decoded, nil
}

func decodeBase64(value string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err == nil {
		return decoded, nil
	}
	if corrupt, ok := err.(base64.CorruptInputError); ok {
		return nil, offsetError("decode Base64", int(corrupt), ErrInvalidPayload, err.Error())
	}
	return nil, fmt.Errorf("%w: decode Base64: %v", ErrInvalidPayload, err)
}

func decodeGBK(value []byte) (string, error) {
	for offset := 0; offset < len(value); {
		length := 1
		switch current := value[offset]; {
		case current <= 0x7f || current == 0x80:
		case current >= 0x81 && current <= 0xfe:
			if offset+1 >= len(value) {
				return "", offsetError("decode GBK", offset, ErrInvalidPayload, "truncated multibyte sequence")
			}
			trail := value[offset+1]
			if trail < 0x40 || trail > 0xfe || trail == 0x7f {
				return "", offsetError("decode GBK", offset+1, ErrInvalidPayload, "invalid trailing byte")
			}
			length = 2
		default:
			return "", offsetError("decode GBK", offset, ErrInvalidPayload, "invalid leading byte")
		}

		decoded, _, err := transform.Bytes(simplifiedchinese.GBK.NewDecoder(), value[offset:offset+length])
		if err != nil || bytes.Contains(decoded, []byte(string(utf8.RuneError))) {
			detail := "unmapped byte sequence"
			if err != nil {
				detail = err.Error()
			}
			return "", offsetError("decode GBK", offset, ErrInvalidPayload, detail)
		}
		offset += length
	}

	decoded, consumed, err := transform.Bytes(simplifiedchinese.GBK.NewDecoder(), value)
	if err != nil {
		return "", offsetError("decode GBK", consumed, ErrInvalidPayload, err.Error())
	}
	return string(decoded), nil
}

func invalidUTF8Offset(value []byte) int {
	for offset := 0; offset < len(value); {
		_, size := utf8.DecodeRune(value[offset:])
		if size == 1 && value[offset] >= utf8.RuneSelf {
			return offset
		}
		offset += size
	}
	return -1
}

func offsetError(operation string, offset int, sentinel error, detail string) error {
	return &OffsetError{
		Operation: operation,
		Offset:    offset,
		Err:       fmt.Errorf("%w: %s", sentinel, detail),
	}
}

func isHexDigit(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}

func isUnicodeSpace(value rune) bool {
	return unicode.IsSpace(value)
}
