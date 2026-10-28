package connections

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

const restoredStateMarker = ".restore-inspection-only"
const maxArchiveFileBytes int64 = 512 << 20

type ArchiveFile struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type StateArchive struct {
	Format           int                    `json:"format"`
	CreatedAt        time.Time              `json:"created_at"`
	Files            map[string]ArchiveFile `json:"files"`
	Inspection       *Inspection            `json:"inspection"`
	ResumeAuthorized bool                   `json:"resume_authorized"`
}

func syncArchiveDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// ArchiveState copies a stopped worker's exact state pair under both read-only
// locks. The private archive includes row data; no network or recovery occurs.
func ArchiveState(directory, destination string) (*StateArchive, error) {
	var manifest *StateArchive
	_, err := inspectState(directory, func(inspection *Inspection) error {
		file, err := os.CreateTemp(filepath.Dir(destination), ".connection-backup-*")
		if err != nil {
			return errors.New("private archive destination unavailable")
		}
		defer os.Remove(file.Name())
		defer file.Close()
		if err = file.Chmod(0600); err != nil {
			return err
		}
		gzipWriter := gzip.NewWriter(file)
		writer := tar.NewWriter(gzipWriter)
		manifest = &StateArchive{Format: 1, CreatedAt: time.Now().UTC(), Files: map[string]ArchiveFile{}, Inspection: inspection}
		for _, name := range []string{"connection.db", "source.db"} {
			path := filepath.Join(directory, name)
			info, err := os.Lstat(path)
			if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxArchiveFileBytes {
				return errors.New("state archive requires bounded regular files")
			}
			input, err := os.Open(path)
			if err != nil {
				return errors.New("state archive input unavailable")
			}
			actual, err := input.Stat()
			if err != nil || !os.SameFile(info, actual) {
				input.Close()
				return errors.New("state archive input changed")
			}
			if err = writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: info.Size(), Typeflag: tar.TypeReg}); err != nil {
				input.Close()
				return err
			}
			digest := sha256.New()
			size, err := io.CopyN(io.MultiWriter(writer, digest), input, info.Size())
			input.Close()
			if err != nil {
				return errors.New("state archive copy incomplete")
			}
			manifest.Files[name] = ArchiveFile{Size: size, SHA256: hex.EncodeToString(digest.Sum(nil))}
		}
		raw, _ := json.Marshal(manifest)
		if err = writer.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0600, Size: int64(len(raw)), Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		if _, err = writer.Write(raw); err != nil {
			return err
		}
		if err = writer.Close(); err != nil {
			return err
		}
		if err = gzipWriter.Close(); err != nil {
			return err
		}
		if err = file.Sync(); err != nil {
			return err
		}
		if err = file.Close(); err != nil {
			return err
		}
		// Link publishes only a complete archive and cannot overwrite any
		// existing path, even when another caller races this publication.
		if err = os.Link(file.Name(), destination); err != nil {
			return errors.New("archive publication failed; existing destinations are never overwritten")
		}
		if err = syncArchiveDirectory(filepath.Dir(destination)); err != nil {
			return errors.New("archive publication durability unconfirmed")
		}
		return nil
	})
	return manifest, err
}

// RestoreState extracts only into a new inspection directory. The durable marker
// forbids using even an intact, apparently clean older pair to restart a worker.
// Any failed extraction stays quarantined for investigation; no original changes.
func RestoreState(archive, directory string) (*StateArchive, error) {
	input, err := os.Open(archive)
	if err != nil {
		return nil, errors.New("state archive unavailable")
	}
	defer input.Close()
	if err = os.Mkdir(directory, 0700); err != nil {
		return nil, errors.New("restore requires a new inspection directory")
	}
	marker, err := os.OpenFile(filepath.Join(directory, restoredStateMarker), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0400)
	if err != nil {
		return nil, errors.New("restore quarantine unavailable")
	}
	_, err = marker.WriteString("Inspection only. No worker resume or initialization is authorized.\n")
	if err == nil {
		err = marker.Sync()
	}
	if closeErr := marker.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = syncArchiveDirectory(directory)
	}
	if err == nil {
		err = syncArchiveDirectory(filepath.Dir(directory))
	}
	if err != nil {
		return nil, errors.New("restore quarantine durability unconfirmed")
	}
	gz, err := gzip.NewReader(input)
	if err != nil {
		return nil, errors.New("invalid state archive compression")
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	files := map[string]ArchiveFile{}
	seen := map[string]bool{}
	var manifest StateArchive
	for {
		header, e := reader.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, errors.New("invalid state archive")
		}
		name := header.Name
		if seen[name] || header.Typeflag != tar.TypeReg || (name != "connection.db" && name != "source.db" && name != "manifest.json") || header.Size <= 0 || header.Size > maxArchiveFileBytes {
			return nil, errors.New("unsupported state archive entry")
		}
		seen[name] = true
		if name == "manifest.json" {
			if header.Size > 65536 {
				return nil, errors.New("invalid state archive manifest")
			}
			raw, e := io.ReadAll(io.LimitReader(reader, 65537))
			if e != nil || json.Unmarshal(raw, &manifest) != nil {
				return nil, errors.New("invalid state archive manifest")
			}
			continue
		}
		output, e := os.OpenFile(filepath.Join(directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return nil, errors.New("restore file creation failed")
		}
		hash := sha256.New()
		size, e := io.CopyN(io.MultiWriter(output, hash), reader, header.Size)
		if e == nil {
			e = output.Sync()
		}
		if closeErr := output.Close(); e == nil {
			e = closeErr
		}
		if e != nil {
			return nil, errors.New("restore file incomplete")
		}
		files[name] = ArchiveFile{Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}
	}
	// Consume the gzip footer as well: tar EOF alone does not verify its CRC.
	trailing, e := io.Copy(io.Discard, io.LimitReader(gz, 1))
	if e != nil || trailing != 0 || len(seen) != 3 || len(files) != 2 || manifest.Format != 1 || manifest.CreatedAt.IsZero() || manifest.ResumeAuthorized || manifest.Inspection == nil || manifest.Inspection.ResumeAuthorized || !reflect.DeepEqual(files, manifest.Files) {
		return nil, errors.New("state archive integrity verification failed")
	}
	inspection, e := InspectState(directory)
	if e != nil || !reflect.DeepEqual(inspection, manifest.Inspection) {
		return nil, errors.New("restored state does not match its paired manifest")
	}
	if err = syncArchiveDirectory(directory); err != nil {
		return nil, errors.New("restore durability unconfirmed")
	}
	return &manifest, nil
}
