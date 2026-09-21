package convert

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
)

var (
	// ErrInvalidChecksum indicates unsupported checksum parameters.
	ErrInvalidChecksum = errors.New("invalid checksum parameters")
)

// AdditiveChecksum returns the sum of all bytes modulo 2^bits. Supported
// widths are 8, 16, and 32 bits.
func AdditiveChecksum(value []byte, width int) (uint32, error) {
	var mask uint64
	switch width {
	case 8:
		mask = 1<<8 - 1
	case 16:
		mask = 1<<16 - 1
	case 32:
		mask = 1<<32 - 1
	default:
		return 0, fmt.Errorf("%w: additive width must be 8, 16, or 32, got %d", ErrInvalidChecksum, width)
	}
	var sum uint64
	for _, current := range value {
		sum += uint64(current)
	}
	return uint32(sum & mask), nil
}

// CRC16Params fully describes a direct, width-16 CRC. Polynomial uses its
// conventional non-reflected representation, such as 0x1021 or 0x8005.
type CRC16Params struct {
	Polynomial uint16
	Initial    uint16
	XorOut     uint16
	ReflectIn  bool
	ReflectOut bool
}

var (
	// CRC16ARCParams describes CRC-16/ARC (also called IBM).
	CRC16ARCParams = CRC16Params{Polynomial: 0x8005, Initial: 0x0000, XorOut: 0x0000, ReflectIn: true, ReflectOut: true}
	// CRC16CCITTFalseParams describes CRC-16/CCITT-FALSE.
	CRC16CCITTFalseParams = CRC16Params{Polynomial: 0x1021, Initial: 0xffff, XorOut: 0x0000, ReflectIn: false, ReflectOut: false}
	// CRC16X25Params describes CRC-16/X-25.
	CRC16X25Params = CRC16Params{Polynomial: 0x1021, Initial: 0xffff, XorOut: 0xffff, ReflectIn: true, ReflectOut: true}
	// CRC16ModbusParams describes CRC-16/MODBUS.
	CRC16ModbusParams = CRC16Params{Polynomial: 0x8005, Initial: 0xffff, XorOut: 0x0000, ReflectIn: true, ReflectOut: true}
)

// CRC16 calculates a CRC using explicit algorithm parameters.
func CRC16(value []byte, params CRC16Params) (uint16, error) {
	if params.Polynomial == 0 {
		return 0, fmt.Errorf("%w: CRC16 polynomial must be non-zero", ErrInvalidChecksum)
	}
	crc := params.Initial
	for _, current := range value {
		if params.ReflectIn {
			current = bits.Reverse8(current)
		}
		crc ^= uint16(current) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ params.Polynomial
			} else {
				crc <<= 1
			}
		}
	}
	if params.ReflectOut {
		crc = bits.Reverse16(crc)
	}
	return crc ^ params.XorOut, nil
}

// CRC32Polynomial is a reversed CRC32 polynomial in the representation
// required by hash/crc32.MakeTable.
type CRC32Polynomial uint32

const (
	// CRC32IEEEPolynomial is the reversed IEEE polynomial.
	CRC32IEEEPolynomial CRC32Polynomial = crc32.IEEE
	// CRC32CastagnoliPolynomial is the reversed Castagnoli polynomial.
	CRC32CastagnoliPolynomial CRC32Polynomial = crc32.Castagnoli
	// CRC32KoopmanPolynomial is the reversed Koopman polynomial.
	CRC32KoopmanPolynomial CRC32Polynomial = crc32.Koopman
)

// CRC32 calculates a standard-library CRC32 using a reversed polynomial.
func CRC32(value []byte, polynomial CRC32Polynomial) (uint32, error) {
	if polynomial == 0 {
		return 0, fmt.Errorf("%w: CRC32 reversed polynomial must be non-zero", ErrInvalidChecksum)
	}
	return crc32.Checksum(value, crc32.MakeTable(uint32(polynomial))), nil
}
