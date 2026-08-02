package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/william1nguyen/memkv/internal/store"
)

const (
	formatVersion  = 1
	headerSize     = 30
	MaxPayloadSize = 256 << 20
	tempSuffix     = ".tmp"
	filePerm       = 0644
)

var (
	magic         = [4]byte{'V', 'K', 'S', 'P'}
	checksumTable = crc32.MakeTable(crc32.Castagnoli)
)

type Data struct {
	State              store.LogicalSnapshot
	CreatedAtUnixMilli int64
	IncludedOffset     int64
}

type File struct {
	mutex   sync.Mutex
	enabled bool
	files   fileSystem
}

type syncedFile interface {
	io.Writer
	Sync() error
	Close() error
}

type fileSystem interface {
	Open(string) (syncedFile, error)
	Rename(string, string) error
	Remove(string) error
	SyncDirectory(string) error
}

type osFileSystem struct{}

func (osFileSystem) Open(path string) (syncedFile, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, filePerm)
}

func (osFileSystem) Rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func (osFileSystem) Remove(path string) error {
	return os.Remove(path)
}

func (osFileSystem) SyncDirectory(path string) error {
	return syncDirectory(path)
}

func New(enabled bool) *File {
	return &File{enabled: enabled, files: osFileSystem{}}
}

func Encode(data Data) ([]byte, error) {
	var payload bytes.Buffer

	if err := gob.NewEncoder(&payload).Encode(data.State); err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}

	if payload.Len() > MaxPayloadSize {
		return nil, fmt.Errorf("snapshot payload exceeds %d bytes", MaxPayloadSize)
	}

	encoded := make([]byte, headerSize+payload.Len())
	copy(encoded[:4], magic[:])
	binary.BigEndian.PutUint16(encoded[4:6], formatVersion)
	createdAt := data.CreatedAtUnixMilli

	if createdAt == 0 {
		createdAt = time.Now().UnixMilli()
	}

	binary.BigEndian.PutUint64(encoded[6:14], uint64(createdAt))
	binary.BigEndian.PutUint64(encoded[14:22], uint64(data.IncludedOffset))
	binary.BigEndian.PutUint32(encoded[22:26], uint32(payload.Len()))
	binary.BigEndian.PutUint32(encoded[26:30], crc32.Checksum(payload.Bytes(), checksumTable))
	copy(encoded[headerSize:], payload.Bytes())
	return encoded, nil
}

func Decode(reader io.Reader) (*Data, error) {
	header := make([]byte, headerSize)

	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	if !bytes.Equal(header[:4], magic[:]) {
		return nil, fmt.Errorf("invalid snapshot magic")
	}

	version := binary.BigEndian.Uint16(header[4:6])

	if version != formatVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	offset := int64(binary.BigEndian.Uint64(header[14:22]))
	createdAt := int64(binary.BigEndian.Uint64(header[6:14]))
	length := binary.BigEndian.Uint32(header[22:26])

	if length > MaxPayloadSize {
		return nil, fmt.Errorf("snapshot payload length %d exceeds limit", length)
	}

	expectedChecksum := binary.BigEndian.Uint32(header[26:30])
	payload := make([]byte, length)

	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	if checksum := crc32.Checksum(payload, checksumTable); checksum != expectedChecksum {
		return nil, fmt.Errorf("snapshot checksum mismatch")
	}

	trailing, err := io.ReadAll(io.LimitReader(reader, 1))

	if err != nil {
		return nil, fmt.Errorf("read trailing data: %w", err)
	}

	if len(trailing) != 0 {
		return nil, fmt.Errorf("snapshot has trailing data")
	}

	var state store.LogicalSnapshot

	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&state); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}

	return &Data{State: state, CreatedAtUnixMilli: createdAt, IncludedOffset: offset}, nil
}

func (file *File) Save(data Data, path string) error {
	if !file.enabled {
		return nil
	}

	encoded, err := Encode(data)

	if err != nil {
		return err
	}

	file.mutex.Lock()
	defer file.mutex.Unlock()

	temp := path + tempSuffix
	defer removeTemporaryFile(file.files, temp)
	handle, err := file.files.Open(temp)

	if err != nil {
		return err
	}

	if err := writeAndSync(handle, encoded); err != nil {
		return errors.Join(err, handle.Close())
	}

	if err := handle.Close(); err != nil {
		return err
	}

	if err := file.files.Rename(temp, path); err != nil {
		return err
	}

	return file.files.SyncDirectory(filepath.Dir(path))
}

func removeTemporaryFile(files fileSystem, path string) {
	_ = files.Remove(path)
}

func (file *File) Load(path string) (*Data, error) {
	if !file.enabled {
		return nil, nil
	}

	file.mutex.Lock()
	defer file.mutex.Unlock()

	handle, err := os.Open(path)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	data, decodeErr := Decode(handle)
	return data, errors.Join(decodeErr, handle.Close())
}

func (file *File) Close() error {
	return nil
}

func writeAndSync(file syncedFile, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)

		if err != nil {
			return err
		}

		if written == 0 {
			return io.ErrShortWrite
		}

		data = data[written:]
	}

	return file.Sync()
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)

	if err != nil {
		return err
	}

	if err := directory.Sync(); err != nil {
		return errors.Join(err, directory.Close())
	}

	return directory.Close()
}
