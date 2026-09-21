package convert

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var (
	// ErrInvalidNumeric indicates an unsupported numeric representation.
	ErrInvalidNumeric = errors.New("invalid numeric value")
	// ErrNumericOverflow indicates that a value does not fit its requested width.
	ErrNumericOverflow = errors.New("numeric value overflows requested width")
)

// ByteOrder selects explicit multibyte numeric byte order.
type ByteOrder string

const (
	// ByteOrderBig stores the most significant byte first.
	ByteOrderBig ByteOrder = "big"
	// ByteOrderLittle stores the least significant byte first.
	ByteOrderLittle ByteOrder = "little"
)

// IntegerToBytes parses an integer and returns its exact-width representation.
// Decimal syntax is the default; 0x/0X prefixed values are hexadecimal.
func IntegerToBytes(text string, signed bool, bits int, order ByteOrder) ([]byte, error) {
	byteOrder, err := binaryOrder(order)
	if err != nil {
		return nil, err
	}
	if err := validateIntegerBits(bits); err != nil {
		return nil, err
	}

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty integer", ErrInvalidNumeric)
	}
	base := numericBase(trimmed)
	var raw uint64
	if signed {
		value, parseErr := strconv.ParseInt(trimmed, base, bits)
		if parseErr != nil {
			return nil, classifyNumericParseError(parseErr)
		}
		raw = uint64(value)
	} else {
		value, parseErr := strconv.ParseUint(trimmed, base, bits)
		if parseErr != nil {
			return nil, classifyNumericParseError(parseErr)
		}
		raw = value
	}

	result := make([]byte, bits/8)
	putUnsigned(result, raw, byteOrder)
	return result, nil
}

// IntegerFromBytes formats an exact-width integer as a decimal string.
func IntegerFromBytes(value []byte, signed bool, bits int, order ByteOrder) (string, error) {
	byteOrder, err := binaryOrder(order)
	if err != nil {
		return "", err
	}
	if err := validateIntegerBits(bits); err != nil {
		return "", err
	}
	if len(value) != bits/8 {
		return "", fmt.Errorf("%w: %d-bit integer requires %d bytes, got %d", ErrInvalidNumeric, bits, bits/8, len(value))
	}

	raw := readUnsigned(value, byteOrder)
	if !signed {
		return strconv.FormatUint(raw, 10), nil
	}
	switch bits {
	case 8:
		return strconv.FormatInt(int64(int8(raw)), 10), nil
	case 16:
		return strconv.FormatInt(int64(int16(raw)), 10), nil
	case 32:
		return strconv.FormatInt(int64(int32(raw)), 10), nil
	case 64:
		return strconv.FormatInt(int64(raw), 10), nil
	default:
		return "", fmt.Errorf("%w: unsupported integer width %d", ErrInvalidNumeric, bits)
	}
}

// FloatToBytes parses a finite IEEE 754 float32 or float64 value.
func FloatToBytes(text string, bits int, order ByteOrder) ([]byte, error) {
	byteOrder, err := binaryOrder(order)
	if err != nil {
		return nil, err
	}
	if bits != 32 && bits != 64 {
		return nil, fmt.Errorf("%w: float width must be 32 or 64, got %d", ErrInvalidNumeric, bits)
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(text), bits)
	if err != nil {
		return nil, classifyNumericParseError(err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("%w: NaN and infinity are not accepted", ErrInvalidNumeric)
	}

	result := make([]byte, bits/8)
	if bits == 32 {
		byteOrder.PutUint32(result, math.Float32bits(float32(value)))
	} else {
		byteOrder.PutUint64(result, math.Float64bits(value))
	}
	return result, nil
}

// FloatFromBytes decodes a finite IEEE 754 value and formats it losslessly.
func FloatFromBytes(value []byte, bits int, order ByteOrder) (string, error) {
	byteOrder, err := binaryOrder(order)
	if err != nil {
		return "", err
	}
	if bits != 32 && bits != 64 {
		return "", fmt.Errorf("%w: float width must be 32 or 64, got %d", ErrInvalidNumeric, bits)
	}
	if len(value) != bits/8 {
		return "", fmt.Errorf("%w: %d-bit float requires %d bytes, got %d", ErrInvalidNumeric, bits, bits/8, len(value))
	}

	var decoded float64
	if bits == 32 {
		decoded = float64(math.Float32frombits(byteOrder.Uint32(value)))
	} else {
		decoded = math.Float64frombits(byteOrder.Uint64(value))
	}
	if math.IsNaN(decoded) || math.IsInf(decoded, 0) {
		return "", fmt.Errorf("%w: decoded value is NaN or infinity", ErrInvalidNumeric)
	}
	return strconv.FormatFloat(decoded, 'g', -1, bits), nil
}

func validateIntegerBits(bits int) error {
	switch bits {
	case 8, 16, 32, 64:
		return nil
	default:
		return fmt.Errorf("%w: integer width must be 8, 16, 32, or 64, got %d", ErrInvalidNumeric, bits)
	}
}

func binaryOrder(order ByteOrder) (binary.ByteOrder, error) {
	switch order {
	case ByteOrderBig:
		return binary.BigEndian, nil
	case ByteOrderLittle:
		return binary.LittleEndian, nil
	default:
		return nil, fmt.Errorf("%w: unsupported byte order %q", ErrInvalidNumeric, order)
	}
}

func numericBase(value string) int {
	unsigned := strings.TrimPrefix(strings.TrimPrefix(value, "+"), "-")
	if strings.HasPrefix(unsigned, "0x") || strings.HasPrefix(unsigned, "0X") {
		return 0
	}
	return 10
}

func classifyNumericParseError(err error) error {
	var numberError *strconv.NumError
	if errors.As(err, &numberError) && errors.Is(numberError.Err, strconv.ErrRange) {
		return fmt.Errorf("%w: %v", ErrNumericOverflow, err)
	}
	return fmt.Errorf("%w: %v", ErrInvalidNumeric, err)
}

func putUnsigned(destination []byte, value uint64, order binary.ByteOrder) {
	switch len(destination) {
	case 1:
		destination[0] = byte(value)
	case 2:
		order.PutUint16(destination, uint16(value))
	case 4:
		order.PutUint32(destination, uint32(value))
	case 8:
		order.PutUint64(destination, value)
	}
}

func readUnsigned(value []byte, order binary.ByteOrder) uint64 {
	switch len(value) {
	case 1:
		return uint64(value[0])
	case 2:
		return uint64(order.Uint16(value))
	case 4:
		return uint64(order.Uint32(value))
	case 8:
		return order.Uint64(value)
	default:
		return 0
	}
}
