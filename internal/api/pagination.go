package api

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
)

const (
	defaultListPageSize = 50
	maximumListPageSize = 500
	pageTokenVersion    = byte(1)
)

var errInvalidPageToken = errors.New("invalid page token")

func pageSize(value uint32) (int, error) {
	if value == 0 {
		return defaultListPageSize, nil
	}
	if value > maximumListPageSize {
		return 0, errors.New("page size must not exceed 500")
	}
	return int(value), nil
}

func decodePageToken(token string) (uint64, error) {
	if token == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) != 9 || raw[0] != pageTokenVersion {
		return 0, errInvalidPageToken
	}
	return binary.BigEndian.Uint64(raw[1:]), nil
}

func encodePageToken(offset uint64) string {
	var raw [9]byte
	raw[0] = pageTokenVersion
	binary.BigEndian.PutUint64(raw[1:], offset)
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

func pageBounds(total int, requestedSize uint32, token string) (start, end int, next string, err error) {
	size, err := pageSize(requestedSize)
	if err != nil {
		return 0, 0, "", err
	}
	offset, err := decodePageToken(token)
	if err != nil || offset > uint64(total) {
		return 0, 0, "", errInvalidPageToken
	}
	start = int(offset)
	end = start + size
	if end > total {
		end = total
	}
	if end < total {
		next = encodePageToken(uint64(end))
	}
	return start, end, next, nil
}
