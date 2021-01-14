package hive_test

import (
	"testing"
	"time"

	"github.com/stntngo/parquet-go/hive"
	"github.com/stntngo/parquet-go/local"
	"github.com/stntngo/parquet-go/parquet"
	"github.com/stntngo/parquet-go/reader"
	"github.com/stntngo/parquet-go/writer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Metric struct {
	Name      string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Value     float64 `parquet:"name=value, type=DOUBLE"`
	Timestamp int64   `parquet:"name=metric_ts, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
}

func TestTimestamp(t *testing.T) {
	f, err := local.NewLocalFileWriter("metrics.parquet")
	require.NoError(t, err)

	start := time.Now().Add(-1 * time.Second)

	records := make([]Metric, 0, 10)
	for i := 0; i < 10; i++ {
		metric := Metric{
			Name:      "metric",
			Value:     1234.56,
			Timestamp: time.Now().UTC().Unix() * int64(time.Microsecond),
		}

		records = append(records, metric)
	}

	pw, err := writer.NewParquetWriter(f, records, 4)
	require.NoError(t, err)

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, metric := range records {
		require.NoError(t, pw.Write(metric))
	}

	require.NoError(t, pw.WriteStop())
	require.NoError(t, f.Close())

	fr, err := local.NewLocalFileReader("metrics.parquet")
	require.NoError(t, err)

	pr, err := reader.NewParquetReader(fr, new(Metric), 4)
	require.NoError(t, err)

	assert.Equal(t, int64(10), pr.GetNumRows())
	stop := time.Now().Add(time.Second)

	m := make([]Metric, pr.GetNumRows())
	require.NoError(t, pr.Read(&m))

	for _, metric := range m {
		assert.Equal(t, "metric", metric.Name)
		assert.Equal(t, float64(1234.56), metric.Value)
		ts := time.Unix(metric.Timestamp/int64(time.Microsecond), 0)
		assert.True(t, start.Before(ts))
		assert.True(t, stop.After(ts))
	}

	for _, element := range pr.SchemaHandler.SchemaElements {
		if element.Name == "Parquet_go_root" {
			continue
		}

		_, err := hive.GetHiveType(element)
		assert.NoError(t, err)
	}

	pr.ReadStop()
	require.NoError(t, fr.Close())
}
