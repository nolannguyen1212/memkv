package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/william1nguyen/valkeydb/internal/store"
)

type faultFile struct {
	writeErr error
	syncErr  error
	closeErr error
}

func (file *faultFile) Write(data []byte) (int, error) {
	if file.writeErr != nil {
		return 0, file.writeErr
	}

	return len(data), nil
}

func (file *faultFile) Sync() error {
	return file.syncErr
}

func (file *faultFile) Close() error {
	return file.closeErr
}

type faultFileSystem struct {
	file             *faultFile
	openErr          error
	renameErr        error
	syncDirectoryErr error
}

func (files faultFileSystem) Open(string) (syncedFile, error) {
	return files.file, files.openErr
}

func (files faultFileSystem) Rename(string, string) error {
	return files.renameErr
}

func (files faultFileSystem) Remove(string) error {
	return nil
}

func (files faultFileSystem) SyncDirectory(string) error {
	return files.syncDirectoryErr
}

func TestCodecRoundTrip(t *testing.T) {
	data := Data{
		State: store.LogicalSnapshot{
			Keyspace: map[string]store.KeyMetadata{
				"key": {Type: store.KeyTypeString, Version: 7},
			},
			Strings: map[string]store.StringEntry{"key": {Value: "value"}},
		},
		CreatedAtUnixMilli: 1234,
		IncludedOffset:     42,
	}

	encoded, err := Encode(data)

	if err != nil {
		t.Fatal(err)
	}

	decoded, err := Decode(bytes.NewReader(encoded))

	if err != nil {
		t.Fatal(err)
	}

	if decoded.IncludedOffset != 42 {
		t.Fatalf("IncludedOffset = %d, want 42", decoded.IncludedOffset)
	}

	if decoded.CreatedAtUnixMilli != 1234 {
		t.Fatalf("CreatedAtUnixMilli = %d, want 1234", decoded.CreatedAtUnixMilli)
	}

	if decoded.State.Strings["key"].Value != "value" {
		t.Fatalf("value = %q, want value", decoded.State.Strings["key"].Value)
	}

	if decoded.State.Keyspace["key"].Version != 7 {
		t.Fatalf("version = %d, want 7", decoded.State.Keyspace["key"].Version)
	}
}

func TestDecodeRejectsChecksumMismatch(t *testing.T) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{}})

	if err != nil {
		t.Fatal(err)
	}

	encoded[len(encoded)-1] ^= 0xff

	if _, err := Decode(bytes.NewReader(encoded)); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Decode() error = %v, want checksum error", err)
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{}})

	if err != nil {
		t.Fatal(err)
	}

	encoded[5]++

	if _, err := Decode(bytes.NewReader(encoded)); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("Decode() error = %v, want version error", err)
	}
}

func TestDecodeRejectsTruncatedPayload(t *testing.T) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{}})

	if err != nil {
		t.Fatal(err)
	}

	if _, err := Decode(bytes.NewReader(encoded[:len(encoded)-1])); err == nil {
		t.Fatal("Decode() accepted truncated payload")
	}
}

func TestDecodeRejectsTrailingData(t *testing.T) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{}})

	if err != nil {
		t.Fatal(err)
	}

	encoded = append(encoded, 0)

	if _, err := Decode(bytes.NewReader(encoded)); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("Decode() error = %v, want trailing data error", err)
	}
}

func TestSaveFailurePreservesExistingSnapshot(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "dump.vksp")
	file := New(true)
	initial := Data{State: store.LogicalSnapshot{Strings: map[string]store.StringEntry{
		"key": {Value: "value"},
	}}}

	if err := file.Save(initial, path); err != nil {
		t.Fatal(err)
	}

	if err := os.Mkdir(path+tempSuffix, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := file.Save(Data{State: store.LogicalSnapshot{}}, path); err == nil {
		t.Fatal("Save() succeeded with an unusable temporary path")
	}

	loaded, err := file.Load(path)

	if err != nil {
		t.Fatal(err)
	}

	if loaded.State.Strings["key"].Value != "value" {
		t.Fatalf("snapshot value = %q, want value", loaded.State.Strings["key"].Value)
	}
}

func TestSaveReportsEveryReplacementStageFailure(t *testing.T) {
	stages := []struct {
		name  string
		files faultFileSystem
	}{
		{name: "open", files: faultFileSystem{file: &faultFile{}, openErr: errors.New("open")}},
		{name: "write", files: faultFileSystem{file: &faultFile{writeErr: errors.New("write")}}},
		{name: "sync", files: faultFileSystem{file: &faultFile{syncErr: errors.New("sync")}}},
		{name: "close", files: faultFileSystem{file: &faultFile{closeErr: errors.New("close")}}},
		{name: "rename", files: faultFileSystem{file: &faultFile{}, renameErr: errors.New("rename")}},
		{name: "directory sync", files: faultFileSystem{file: &faultFile{}, syncDirectoryErr: errors.New("directory sync")}},
	}

	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			file := &File{enabled: true, files: stage.files}
			err := file.Save(Data{State: store.LogicalSnapshot{}}, "dump.vksp")

			if err == nil || err.Error() != stage.name {
				t.Fatalf("Save() error = %v, want %q", err, stage.name)
			}
		})
	}
}

func FuzzDecode(f *testing.F) {
	encoded, err := Encode(Data{State: store.LogicalSnapshot{Strings: map[string]store.StringEntry{
		"key": {Value: "value"},
	}}})

	if err != nil {
		f.Fatal(err)
	}

	f.Add(encoded)
	f.Add([]byte("VKSP"))
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = Decode(bytes.NewReader(input))
	})
}

func BenchmarkEncode(b *testing.B) {
	stringsByKey := make(map[string]store.StringEntry, 10_000)
	keyspace := make(map[string]store.KeyMetadata, 10_000)

	for index := range 10_000 {
		key := fmt.Sprintf("key:%d", index)
		stringsByKey[key] = store.StringEntry{Value: "value"}
		keyspace[key] = store.KeyMetadata{Type: store.KeyTypeString}
	}

	data := Data{State: store.LogicalSnapshot{Keyspace: keyspace, Strings: stringsByKey}}
	b.ResetTimer()

	for b.Loop() {
		if _, err := Encode(data); err != nil {
			b.Fatal(err)
		}
	}
}
