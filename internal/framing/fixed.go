package framing

import "fmt"

type fixedDecoder struct {
	length int
	buffer []byte
}

func newFixed(length, maximum int) (*fixedDecoder, error) {
	if length <= 0 {
		return nil, fmt.Errorf("%w: fixed length must be positive", ErrInvalidConfig)
	}
	if length > maximum {
		return nil, fmt.Errorf("%w: fixed length %d exceeds maximum %d", ErrInvalidConfig, length, maximum)
	}
	return &fixedDecoder{length: length}, nil
}

func (d *fixedDecoder) Push(chunk []byte) ([][]byte, error) {
	if len(chunk) == 0 {
		return nil, nil
	}

	frameCount := (len(d.buffer) + len(chunk)) / d.length
	frames := make([][]byte, 0, frameCount)
	for len(chunk) > 0 {
		needed := d.length - len(d.buffer)
		if needed > len(chunk) {
			d.buffer = append(d.buffer, chunk...)
			break
		}

		frame := make([]byte, d.length)
		copied := copy(frame, d.buffer)
		copy(frame[copied:], chunk[:needed])
		frames = append(frames, frame)
		d.buffer = d.buffer[:0]
		chunk = chunk[needed:]
	}
	return frames, nil
}

func (d *fixedDecoder) Reset() {
	d.buffer = nil
}
