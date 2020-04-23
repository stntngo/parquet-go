package inmemory

import (
	"github.com/spf13/afero"

	"github.com/stntngo/parquet-go/source"
)

var os = afero.NewMemMapFs()

func Remove(path string) error {
	return os.Remove(path)
}

func RemoveAll(path string) error {
	return os.RemoveAll(path)
}

type InMemoryFile struct {
	FilePath string
	File     afero.File
}

func NewInMemoryFileWriter(name string) (source.ParquetFile, error) {
	return (&InMemoryFile{}).Create(name)
}

func NewInMemoryFileReader(name string) (source.ParquetFile, error) {
	return (&InMemoryFile{}).Open(name)
}

func (self *InMemoryFile) Create(name string) (source.ParquetFile, error) {
	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	var out InMemoryFile
	out.FilePath = name
	out.File = file
	return &out, err
}

func (self *InMemoryFile) Open(name string) (source.ParquetFile, error) {
	if name == "" {
		name = self.FilePath
	}

	var err error
	out := new(InMemoryFile)
	out.FilePath = name
	out.File, err = os.Open(name)
	return out, err
}
func (self *InMemoryFile) Seek(offset int64, pos int) (int64, error) {
	return self.File.Seek(offset, pos)
}

func (self *InMemoryFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = self.File.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (self *InMemoryFile) Write(b []byte) (n int, err error) {
	return self.File.Write(b)
}

func (self *InMemoryFile) Close() error {
	return self.File.Close()
}
