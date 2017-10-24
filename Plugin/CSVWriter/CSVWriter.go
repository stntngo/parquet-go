package CSVWriter

import (
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/parquet"
)

type MetadataType struct {
	Type       string
	Name       string
	TypeLength int32
	Scale      int32
	Precision  int32
}

type CSVWriterHandler struct {
	SchemaHandler *SchemaHandler
	NP            int64
	Footer        *parquet.FileMetaData
	RowGroups     []*RowGroups

	PFile ParquetFile

	PageSize     int64
	RowGroupSize int64
	Offset       int64
	Record       [][]string
	Metadata     []MetadataType
	RecAveSize   int64
	Size         int64
}

func NewSchemaHandlerFromMetadata(mds []MetadataType) *SchemaHandler {
	schemaList := make([]*parquet.SchemaElement)

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = "parquet-go-root"
	rootNumChildren := len(mds)
	rootSchema.NumChildren = &rootNumChildren
	rt := parquet.FieldRepetitionType(-1)
	rootSchema.RepetitionType = &rt
	schemaList = append(schemaList, rootSchema)

	for _, md := range mds {
		schema := parquet.NewSchemaElement()
		schema.Name = md.Name
		numChildren := 0
		schema.NumChildren = &numChildren
		rt := parquet.FieldRepetitionType(1)
		schema.FieldRepetitionType = &rt

		if IsBaseType(md.Type) {
			t := NameToBaseType(md.Type)
			schema.Type = &t
			if md.Type == "FIXED_LEN_BYTE_ARRAY" {
				schema.TypeLength = &schema.TypeLength
			}

		} else {
			if name == "INT_8" || name == "INT_16" || name == "INT_32" ||
				name == "UINT_8" || name == "UINT_16" || name == "UINT_32" ||
				name == "DATE" || name == "TIME_MILLIS" {
				t := parquet.Type_INT32
				ct := ParquetType.NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "INT_64" || name == "UINT_64" ||
				name == "TIME_MICROS" || name == "TIMESTAMP_MICROS" {
				t := parquet.Type_INT64
				ct := ParquetType.NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "UTF8" {
				t := parquet.Type_BYTE_ARRAY
				ct := ParquetType.NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "INTERVAL" {
				t := parquet.Type_FIXED_LEN_BYTE_ARRAY
				ct := ParquetType.NameToConvertedType(name)
				var ln int32 = 12
				schema.Type = &t
				schema.ConvertedType = &ct
				schema.TypeLength = &ln
			} else if name == "DECIMAL" {
				ct := ParquetType.NameToConvertedType(name)
				t := ParquetType.NameToBaseType("BYTE_ARRAY")
				scale := md.Scale
				precision := md.Precision

				schema.Type = &t
				schema.ConvertedType = &ct
				schema.Scale = &scale
				schema.Precision = &precision

			}
		}

		schemaList = append(schemaList, schema)
	}

	return NewSchemaHandlerFromSchemaList(schemaList)

}

func NewCSVWriterHandler() *CSVWriterHandler {
	res := new(CSVWriterHandler)
	res.NP = 1
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	return res
}

func (self *CSVWriterHandler) WriteInit(md []string, pfile ParquetFile, np int64, recordAveSize int64) {
	self.SchemaHandler = NewSchemaHandlerFromMetadata(md)
	self.Metadata = md
	self.PFile = pfile
	self.NP = np
	self.RecAveSize = recordAveSize
	self.Footer = parquet.NewFileMetaData()
	self.Footer.Version = 1
	self.Footer.Schema = append(self.Footer.Schema, self.SchemaHandler.SchemaElements...)
	self.Offset = 4
	self.PFile.Write([]byte("PAR1"))
}

func (self *CSVWriterHandler) Write(rec []string) {
	self.Size += self.RecAveSize
	self.Record = append(self.Record, rec)

	if self.Size > self.RowGroupSize {
		self.Flush()
	}
}

func (self *CSVWriterHandler) Flush() {
	pagesMapList := make([]map[string][]*Page, self.NP)
	for i := 0; i < int(self.NP); i++ {
		pagesMapList[i] = make(map[string][]*Page)
	}

	doneChan := make(chan int)
	l := int64(len(self.Objs))
	var c int64 = 0
	delta := (l + self.NP - 1) / self.NP
	for c = 0; c < self.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > l {
			end = l
		}
		if bgn >= l {
			bgn, end = l, l
		}

		go func(b, e int, index int64) {
			if e <= b {
				doneChan <- 0
				return
			}

			tableMap := MarshalCSV(self.Objs, b, e, md, self.SchemaHandler)
			for name, table := range *tableMap {
				pagesMapList[index][name], _ = TableToDataPages(table, int32(self.PageSize),
					parquet.CompressionCodec_SNAPPY)
			}

			doneChan <- 0
		}(int(bgn), int(end), c)
	}

	for c = 0; c < self.NP; c++ {
		<-doneChan
	}

	totalPagesMap := make(map[string][]*Page)
	for _, pagesMap := range pagesMapList {
		for name, pages := range pagesMap {
			if _, ok := totalPagesMap[name]; !ok {
				totalPagesMap[name] = pages
			} else {
				totalPagesMap[name] = append(totalPagesMap[name], pages...)
			}
		}
	}

	//pages -> chunk
	chunkMap := make(map[string]*Chunk)
	for name, pages := range totalPagesMap {
		chunkMap[name] = PagesToChunk(pages)
	}

	//chunks -> rowGroup
	rowGroup := NewRowGroup()
	rowGroup.RowGroupHeader.Columns = make([]*parquet.ColumnChunk, 0)

	for k := 0; k < len(self.SchemaHandler.SchemaElements); k++ {
		//for _, chunk := range chunkMap {
		schema := self.SchemaHandler.SchemaElements[k]
		if schema.GetNumChildren() > 0 {
			continue
		}
		chunk := chunkMap[self.SchemaHandler.IndexMap[int32(k)]]
		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
		rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalCompressedSize
		rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.ChunkHeader)
	}
	rowGroup.RowGroupHeader.NumRows = int64(len(self.Objs))

	for k := 0; k < len(rowGroup.Chunks); k++ {
		rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = self.Offset
		rowGroup.Chunks[k].ChunkHeader.FileOffset = self.Offset

		for l := 0; l < len(rowGroup.Chunks[k].Pages); l++ {
			data := rowGroup.Chunks[k].Pages[l].RawData
			self.PFile.Write(data)
			self.Offset += int64(len(data))
		}
	}
	self.Footer.NumRows += int64(len(self.Objs))
	self.Footer.RowGroups = append(self.Footer.RowGroups, rowGroup.RowGroupHeader)
	self.Size = 0
	self.Objs = self.Objs[0:0]
}