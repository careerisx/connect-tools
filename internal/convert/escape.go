package convert

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// DecodeEscape decodes C-style escapes and UTF-8 literal text into newly
// owned bytes. Escape failures report a byte offset in input.
func DecodeEscape(input string) ([]byte, error) {
	result := make([]byte, 0, len(input))
	for offset := 0; offset < len(input); {
		if input[offset] != '\\' {
			_, size := utf8.DecodeRuneInString(input[offset:])
			if size == 1 && input[offset] >= utf8.RuneSelf {
				return nil, offsetError("decode escape", offset, ErrInvalidPayload, "invalid UTF-8 input")
			}
			result = append(result, input[offset:offset+size]...)
			offset += size
			continue
		}

		escapeOffset := offset
		offset++
		if offset >= len(input) {
			return nil, offsetError("decode escape", escapeOffset, ErrInvalidPayload, "trailing backslash")
		}
		switch input[offset] {
		case 'n':
			result = append(result, '\n')
			offset++
		case 'r':
			result = append(result, '\r')
			offset++
		case 't':
			result = append(result, '\t')
			offset++
		case '0':
			result = append(result, 0)
			offset++
		case '\\':
			result = append(result, '\\')
			offset++
		case '"':
			result = append(result, '"')
			offset++
		case 'x':
			value, next, err := parseEscapeHex(input, offset+1, 2, escapeOffset)
			if err != nil {
				return nil, err
			}
			result = append(result, byte(value))
			offset = next
		case 'u':
			value, next, err := parseEscapeHex(input, offset+1, 4, escapeOffset)
			if err != nil {
				return nil, err
			}
			offset = next
			if value >= 0xd800 && value <= 0xdbff {
				if offset+6 > len(input) || input[offset:offset+2] != "\\u" {
					return nil, offsetError("decode escape", escapeOffset, ErrInvalidPayload, "high surrogate requires a low surrogate")
				}
				low, lowNext, lowErr := parseEscapeHex(input, offset+2, 4, offset)
				if lowErr != nil {
					return nil, lowErr
				}
				if low < 0xdc00 || low > 0xdfff {
					return nil, offsetError("decode escape", offset, ErrInvalidPayload, "invalid low surrogate")
				}
				value = uint64(utf16.DecodeRune(rune(value), rune(low)))
				offset = lowNext
			} else if value >= 0xdc00 && value <= 0xdfff {
				return nil, offsetError("decode escape", escapeOffset, ErrInvalidPayload, "unexpected low surrogate")
			}
			result = utf8.AppendRune(result, rune(value))
		case 'U':
			value, next, err := parseEscapeHex(input, offset+1, 8, escapeOffset)
			if err != nil {
				return nil, err
			}
			if value > utf8.MaxRune || value >= 0xd800 && value <= 0xdfff {
				return nil, offsetError("decode escape", escapeOffset, ErrInvalidPayload, "invalid Unicode code point")
			}
			result = utf8.AppendRune(result, rune(value))
			offset = next
		default:
			return nil, offsetError("decode escape", escapeOffset, ErrInvalidPayload, fmt.Sprintf("unknown escape \\%c", input[offset]))
		}
	}
	return result, nil
}

// EncodeEscape returns a reversible C-style representation of bytes. Printable
// ASCII is emitted literally; all other bytes use named or hexadecimal escapes.
func EncodeEscape(value []byte) string {
	var result strings.Builder
	result.Grow(len(value))
	for _, current := range value {
		switch current {
		case '\n':
			result.WriteString("\\n")
		case '\r':
			result.WriteString("\\r")
		case '\t':
			result.WriteString("\\t")
		case 0:
			result.WriteString("\\0")
		case '\\':
			result.WriteString("\\\\")
		case '"':
			result.WriteString("\\\"")
		default:
			if current >= 0x20 && current <= 0x7e {
				result.WriteByte(current)
			} else {
				fmt.Fprintf(&result, "\\x%02X", current)
			}
		}
	}
	return result.String()
}

func parseEscapeHex(input string, start, digits, escapeOffset int) (uint64, int, error) {
	end := start + digits
	if end > len(input) {
		return 0, start, offsetError("decode escape", escapeOffset, ErrInvalidPayload, "truncated hexadecimal escape")
	}
	for offset := start; offset < end; offset++ {
		if !isHexDigit(input[offset]) {
			return 0, offset, offsetError("decode escape", offset, ErrInvalidPayload, "invalid hexadecimal escape digit")
		}
	}
	value, err := strconv.ParseUint(input[start:end], 16, digits*4)
	if err != nil {
		return 0, start, offsetError("decode escape", start, ErrInvalidPayload, err.Error())
	}
	return value, end, nil
}
