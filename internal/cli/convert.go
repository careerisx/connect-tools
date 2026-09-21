package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	ctconvert "connect-tools/internal/convert"
	"github.com/spf13/cobra"
)

func newConvertCommand(opts *runtimeOptions) *cobra.Command {
	var input, from, to, charset, order string
	var signed bool
	var bits int
	command := &cobra.Command{Use: "convert", Short: "Convert payload, number, or timestamp formats", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if from == "unix-s" || from == "unix-ms" || from == "unix-us" || from == "unix-ns" {
			value, err := strconv.ParseInt(input, 10, 64)
			if err != nil {
				return usageError(err)
			}
			parsed, err := ctconvert.UnixToTime(value, timestampUnit(from))
			if err != nil {
				return usageError(err)
			}
			_, err = fmt.Fprintln(opts.Stdout, parsed.UTC().Format(time.RFC3339Nano))
			return err
		}
		if from == "rfc3339" && strings.HasPrefix(to, "unix-") {
			parsed, err := time.Parse(time.RFC3339Nano, input)
			if err != nil {
				return usageError(err)
			}
			value, err := ctconvert.TimeToUnix(parsed, timestampUnit(to))
			if err != nil {
				return usageError(err)
			}
			_, err = fmt.Fprintln(opts.Stdout, value)
			return err
		}
		byteOrder := ctconvert.ByteOrder(order)
		if from == "number" {
			var data []byte
			var err error
			if strings.HasPrefix(to, "float") {
				data, err = ctconvert.FloatToBytes(input, bits, byteOrder)
			} else {
				data, err = ctconvert.IntegerToBytes(input, signed, bits, byteOrder)
			}
			if err != nil {
				return usageError(err)
			}
			encoded, err := ctconvert.Encode(data, ctconvert.Format{Kind: ctconvert.FormatHex})
			if err != nil {
				return operationError(err)
			}
			_, err = fmt.Fprintln(opts.Stdout, encoded)
			return err
		}
		var decoded []byte
		var err error
		if from == "escape" {
			decoded, err = ctconvert.DecodeEscape(input)
		} else {
			decoded, err = ctconvert.Decode(ctconvert.PayloadInput{Encoding: ctconvert.Encoding(from), Data: input, Charset: ctconvert.Charset(charset)})
		}
		if err != nil {
			return usageError(err)
		}
		if to == "integer" {
			value, err := ctconvert.IntegerFromBytes(decoded, signed, bits, byteOrder)
			if err != nil {
				return usageError(err)
			}
			_, err = fmt.Fprintln(opts.Stdout, value)
			return err
		}
		if to == "float" {
			value, err := ctconvert.FloatFromBytes(decoded, bits, byteOrder)
			if err != nil {
				return usageError(err)
			}
			_, err = fmt.Fprintln(opts.Stdout, value)
			return err
		}
		result, err := ctconvert.Encode(decoded, ctconvert.Format{Kind: ctconvert.FormatKind(to), Charset: ctconvert.Charset(charset)})
		if err != nil {
			return usageError(err)
		}
		_, err = fmt.Fprintln(opts.Stdout, result)
		return err
	}}
	command.Flags().StringVar(&input, "data", "", "input value")
	command.Flags().StringVar(&from, "from", "text", "input format")
	command.Flags().StringVar(&to, "to", "hex", "output format")
	command.Flags().StringVar(&charset, "charset", "utf-8", "text charset")
	command.Flags().StringVar(&order, "byte-order", "big", "big or little")
	command.Flags().BoolVar(&signed, "signed", false, "signed integer")
	command.Flags().IntVar(&bits, "bits", 32, "numeric bit width")
	return command
}

func newChecksumCommand(opts *runtimeOptions) *cobra.Command {
	var algorithm, textValue, hexValue, base64Value, file string
	command := &cobra.Command{Use: "checksum", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		flags := payloadFlags{text: textValue, hex: hexValue, base64: base64Value, file: file, charset: "utf-8"}
		input, err := payloadInputFromFlags(cmd.Context(), cmd.Flags(), opts, flags)
		if err != nil {
			return err
		}
		data, err := ctconvert.Decode(ctconvert.PayloadInput{Encoding: payloadEncoding(input.Encoding), Data: input.Data, Charset: ctconvert.CharsetUTF8})
		if err != nil {
			return usageError(err)
		}
		var value uint32
		switch strings.ToLower(algorithm) {
		case "sum8":
			value, err = ctconvert.AdditiveChecksum(data, 8)
		case "sum16":
			value, err = ctconvert.AdditiveChecksum(data, 16)
		case "sum32":
			value, err = ctconvert.AdditiveChecksum(data, 32)
		case "crc16-arc":
			var v uint16
			v, err = ctconvert.CRC16(data, ctconvert.CRC16ARCParams)
			value = uint32(v)
		case "crc16-modbus":
			var v uint16
			v, err = ctconvert.CRC16(data, ctconvert.CRC16ModbusParams)
			value = uint32(v)
		case "crc16-ccitt":
			var v uint16
			v, err = ctconvert.CRC16(data, ctconvert.CRC16CCITTFalseParams)
			value = uint32(v)
		case "crc16-x25":
			var v uint16
			v, err = ctconvert.CRC16(data, ctconvert.CRC16X25Params)
			value = uint32(v)
		case "crc32-ieee":
			value, err = ctconvert.CRC32(data, ctconvert.CRC32IEEEPolynomial)
		case "crc32-castagnoli":
			value, err = ctconvert.CRC32(data, ctconvert.CRC32CastagnoliPolynomial)
		case "crc32-koopman":
			value, err = ctconvert.CRC32(data, ctconvert.CRC32KoopmanPolynomial)
		default:
			return usageErrorf("unsupported checksum algorithm")
		}
		if err != nil {
			return usageError(err)
		}
		_, err = fmt.Fprintf(opts.Stdout, "{\"decimal\":%d,\"hex\":\"0x%X\"}\n", value, value)
		return err
	}}
	command.Flags().StringVar(&algorithm, "algorithm", "crc32-ieee", "checksum algorithm")
	command.Flags().StringVar(&textValue, "text", "", "text input")
	command.Flags().StringVar(&hexValue, "hex", "", "HEX input")
	command.Flags().StringVar(&base64Value, "base64", "", "Base64 input")
	command.Flags().StringVar(&file, "file", "", "file input")
	return command
}

func payloadEncoding(value connecttoolsv1.PayloadEncoding) ctconvert.Encoding {
	switch value {
	case connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_TEXT:
		return ctconvert.EncodingText
	case connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_HEX:
		return ctconvert.EncodingHex
	default:
		return ctconvert.EncodingBase64
	}
}

func timestampUnit(value string) ctconvert.TimestampUnit {
	switch value {
	case "unix-ms":
		return ctconvert.TimestampMilliseconds
	case "unix-us":
		return ctconvert.TimestampMicroseconds
	case "unix-ns":
		return ctconvert.TimestampNanoseconds
	default:
		return ctconvert.TimestampSeconds
	}
}
