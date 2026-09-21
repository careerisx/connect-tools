package framing

import "fmt"

type delimiterDecoder struct {
	delimiter        []byte
	includeDelimiter bool
	maximum          int
	prefix           []int
	matched          int
	buffer           []byte
}

func newDelimiter(delimiter []byte, includeDelimiter bool, maximum int) (*delimiterDecoder, error) {
	if len(delimiter) == 0 {
		return nil, fmt.Errorf("%w: delimiter must not be empty", ErrInvalidConfig)
	}
	if len(delimiter) > maximum {
		return nil, fmt.Errorf("%w: delimiter length %d exceeds maximum %d", ErrInvalidConfig, len(delimiter), maximum)
	}
	decoder := &delimiterDecoder{
		delimiter:        append([]byte(nil), delimiter...),
		includeDelimiter: includeDelimiter,
		maximum:          maximum,
		prefix:           make([]int, len(delimiter)),
	}
	for index, matched := 1, 0; index < len(decoder.delimiter); index++ {
		for matched > 0 && decoder.delimiter[index] != decoder.delimiter[matched] {
			matched = decoder.prefix[matched-1]
		}
		if decoder.delimiter[index] == decoder.delimiter[matched] {
			matched++
		}
		decoder.prefix[index] = matched
	}
	return decoder, nil
}

func (d *delimiterDecoder) Push(chunk []byte) ([][]byte, error) {
	if len(chunk) == 0 {
		return nil, nil
	}
	var frames [][]byte
	for _, current := range chunk {
		d.buffer = append(d.buffer, current)
		for d.matched > 0 && current != d.delimiter[d.matched] {
			d.matched = d.prefix[d.matched-1]
		}
		if current == d.delimiter[d.matched] {
			d.matched++
		}
		if d.matched == len(d.delimiter) {
			payloadEnd := len(d.buffer) - len(d.delimiter)
			frameEnd := payloadEnd
			if d.includeDelimiter {
				frameEnd = len(d.buffer)
			}
			if frameEnd > d.maximum {
				d.Reset()
				return frames, fmt.Errorf("%w: delimiter frame has %d bytes, maximum is %d", ErrFrameTooLarge, frameEnd, d.maximum)
			}
			frames = append(frames, append([]byte(nil), d.buffer[:frameEnd]...))
			d.buffer = nil
			d.matched = 0
			continue
		}

		if d.confirmedPayloadLength() > d.maximumPayloadLength() {
			d.Reset()
			return frames, fmt.Errorf("%w: delimiter frame exceeds %d bytes", ErrFrameTooLarge, d.maximum)
		}
	}
	return frames, nil
}

func (d *delimiterDecoder) Reset() {
	d.buffer = nil
	d.matched = 0
}

func (d *delimiterDecoder) maximumPayloadLength() int {
	if d.includeDelimiter {
		return d.maximum - len(d.delimiter)
	}
	return d.maximum
}

func (d *delimiterDecoder) confirmedPayloadLength() int {
	return len(d.buffer) - d.matched
}
