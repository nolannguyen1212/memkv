package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/william1nguyen/valkeydb/internal/resp"
)

const (
	recordVersion    = 2
	recordHeaderSize = 14
	maxRecordPayload = 16 << 20
	flagBatch        = 1
)

var (
	recordMagic         = [4]byte{'V', 'K', 'W', 'L'}
	recordChecksumTable = crc32.MakeTable(crc32.Castagnoli)
)

type Command struct {
	Name string
	Args []string
}

type truncatedRecordError struct {
	message string
	offset  int64
	err     error
}

func (err *truncatedRecordError) Error() string {
	return fmt.Sprintf("%s at offset %d: %v", err.message, err.offset, err.err)
}

func (err *truncatedRecordError) Unwrap() error {
	return err.err
}

func encodeRecord(values []resp.Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("WAL record cannot be empty")
	}
	flags := byte(0)
	if len(values) > 1 {
		flags = flagBatch
	}
	commands := make([]Command, len(values))
	for index, value := range values {
		command, err := commandFromValue(value)
		if err != nil {
			return nil, fmt.Errorf("WAL command %d: %w", index, err)
		}
		commands[index] = command
	}
	payload, err := encodeCommands(commands)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxRecordPayload {
		return nil, fmt.Errorf("WAL payload exceeds %d bytes", maxRecordPayload)
	}

	record := make([]byte, recordHeaderSize+len(payload))
	copy(record[:4], recordMagic[:])
	record[4] = recordVersion
	record[5] = flags
	binary.BigEndian.PutUint32(record[6:10], uint32(len(payload)))
	binary.BigEndian.PutUint32(record[10:14], crc32.Checksum(payload, recordChecksumTable))
	copy(record[recordHeaderSize:], payload)
	return record, nil
}

func commandFromValue(value resp.Value) (Command, error) {
	if value.Type != resp.TypeArray || len(value.Array) == 0 {
		return Command{}, fmt.Errorf("command must be a non-empty array")
	}
	name := value.Array[0]
	if name.Type != resp.TypeBulkString && name.Type != resp.TypeSimpleString {
		return Command{}, fmt.Errorf("command name must be a string")
	}
	command := Command{Name: name.String, Args: make([]string, len(value.Array)-1)}
	for index, argument := range value.Array[1:] {
		if argument.Type != resp.TypeBulkString && argument.Type != resp.TypeSimpleString {
			return Command{}, fmt.Errorf("argument %d must be a string", index)
		}
		command.Args[index] = argument.String
	}
	return command, nil
}

func encodeCommands(commands []Command) ([]byte, error) {
	var payload bytes.Buffer
	if err := binary.Write(&payload, binary.BigEndian, uint32(len(commands))); err != nil {
		return nil, err
	}
	for _, command := range commands {
		if command.Name == "" {
			return nil, fmt.Errorf("WAL command name cannot be empty")
		}
		if err := writeString(&payload, command.Name); err != nil {
			return nil, err
		}
		if err := binary.Write(&payload, binary.BigEndian, uint32(len(command.Args))); err != nil {
			return nil, err
		}
		for _, argument := range command.Args {
			if err := writeString(&payload, argument); err != nil {
				return nil, err
			}
		}
		if payload.Len() > maxRecordPayload {
			return nil, fmt.Errorf("WAL payload exceeds %d bytes", maxRecordPayload)
		}
	}
	return payload.Bytes(), nil
}

func writeString(writer io.Writer, value string) error {
	if uint64(len(value)) > uint64(maxRecordPayload) {
		return fmt.Errorf("WAL string exceeds %d bytes", maxRecordPayload)
	}
	if err := binary.Write(writer, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := io.WriteString(writer, value)
	return err
}

func decodeRecord(reader io.Reader, offset int64) ([]Command, int64, error) {
	header := make([]byte, recordHeaderSize)
	written, err := io.ReadFull(reader, header)
	if err == io.EOF && written == 0 {
		return nil, 0, io.EOF
	}
	if err != nil {
		return nil, 0, &truncatedRecordError{message: "truncated WAL header", offset: offset, err: err}
	}
	if !bytes.Equal(header[:4], recordMagic[:]) {
		return nil, 0, fmt.Errorf("invalid WAL magic at offset %d", offset)
	}
	if header[4] != recordVersion {
		return nil, 0, fmt.Errorf("unsupported WAL version %d at offset %d", header[4], offset)
	}
	flags := header[5]
	if flags != 0 && flags != flagBatch {
		return nil, 0, fmt.Errorf("unsupported WAL flags %d at offset %d", flags, offset)
	}
	length := binary.BigEndian.Uint32(header[6:10])
	if length > maxRecordPayload {
		return nil, 0, fmt.Errorf("WAL payload length %d exceeds limit at offset %d", length, offset)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, 0, &truncatedRecordError{message: "truncated WAL payload", offset: offset, err: err}
	}
	expectedChecksum := binary.BigEndian.Uint32(header[10:14])
	if checksum := crc32.Checksum(payload, recordChecksumTable); checksum != expectedChecksum {
		return nil, 0, fmt.Errorf("WAL checksum mismatch at offset %d", offset)
	}

	commands, err := decodeCommands(payload)
	if err != nil {
		return nil, 0, fmt.Errorf("decode WAL payload at offset %d: %w", offset, err)
	}
	if flags == 0 && len(commands) != 1 {
		return nil, 0, fmt.Errorf("WAL single record contains %d commands at offset %d", len(commands), offset)
	}
	if flags == flagBatch && len(commands) < 2 {
		return nil, 0, fmt.Errorf("WAL batch contains %d commands at offset %d", len(commands), offset)
	}
	return commands, int64(recordHeaderSize) + int64(length), nil
}

func decodeCommands(payload []byte) ([]Command, error) {
	reader := bytes.NewReader(payload)
	count, err := readUint32(reader)
	if err != nil || count == 0 || uint64(count) > uint64(len(payload)) {
		return nil, fmt.Errorf("invalid command count")
	}
	commands := make([]Command, int(count))
	for index := range commands {
		name, err := readString(reader)
		if err != nil || name == "" {
			return nil, fmt.Errorf("command %d has invalid name", index)
		}
		argumentCount, err := readUint32(reader)
		if err != nil || uint64(argumentCount) > uint64(reader.Len()/4) {
			return nil, fmt.Errorf("command %d has invalid argument count", index)
		}
		arguments := make([]string, int(argumentCount))
		for argumentIndex := range arguments {
			arguments[argumentIndex], err = readString(reader)
			if err != nil {
				return nil, fmt.Errorf("command %d argument %d is invalid", index, argumentIndex)
			}
		}
		commands[index] = Command{Name: name, Args: arguments}
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("payload has trailing data")
	}
	return commands, nil
}

func readUint32(reader io.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(reader, binary.BigEndian, &value)
	return value, err
}

func readString(reader *bytes.Reader) (string, error) {
	length, err := readUint32(reader)
	if err != nil || uint64(length) > uint64(reader.Len()) {
		return "", io.ErrUnexpectedEOF
	}
	value := make([]byte, int(length))
	if _, err := io.ReadFull(reader, value); err != nil {
		return "", err
	}
	return string(value), nil
}
