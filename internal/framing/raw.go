package framing

import "fmt"

type rawDecoder struct {
	maximum int
}

func newRaw(maximum int) *rawDecoder {
	return &rawDecoder{maximum: maximum}
}

func (d *rawDecoder) Push(chunk []byte) ([][]byte, error) {
	if len(chunk) == 0 {
		return nil, nil
	}
	if len(chunk) > d.maximum {
		return nil, fmt.Errorf("%w: got %d bytes, maximum is %d", ErrFrameTooLarge, len(chunk), d.maximum)
	}
	frame := append([]byte(nil), chunk...)
	return [][]byte{frame}, nil
}

func (d *rawDecoder) Reset() {}
