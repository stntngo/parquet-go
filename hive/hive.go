package hive

import (
	"errors"

	"github.com/stntngo/parquet-go/parquet"
)

var (
	errUnsupportedParquetType = errors.New("cannot translate parquet type to hive type. unsupported parquet type")
)

// GetHiveType converts a Parquet Schema Element into its equivalent Hive Type as a string.
func GetHiveType(el *parquet.SchemaElement) (string, error) {
	if el.IsSetLogicalType() {
		return "", errUnsupportedParquetType
	}

	if el.IsSetConvertedType() {
		switch el.GetConvertedType() {
		case parquet.ConvertedType_UTF8:
			return "string", nil
		case parquet.ConvertedType_DECIMAL:
			return "decimal", nil
		case parquet.ConvertedType_DATE:
			return "date", nil
		case parquet.ConvertedType_TIMESTAMP_MILLIS:
			return "timestamp", nil
		case parquet.ConvertedType_INT_8:
			return "tinyint", nil
		case parquet.ConvertedType_INT_16:
			return "smallint", nil
		case parquet.ConvertedType_INT_32:
			return "int", nil
		case parquet.ConvertedType_INT_64:
			return "bigint", nil
		}

		return "", errUnsupportedParquetType
	}

	if el.IsSetType() {
		switch el.GetType() {
		case parquet.Type_BOOLEAN:
			return "boolean", nil
		case parquet.Type_INT32:
			return "int", nil
		case parquet.Type_INT64:
			return "bigint", nil
		case parquet.Type_FLOAT:
			return "float", nil
		case parquet.Type_DOUBLE:
			return "double", nil
		case parquet.Type_BYTE_ARRAY:
			return "binary", nil
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			return "binary", nil
		}

		return "", errUnsupportedParquetType
	}

	return "", errUnsupportedParquetType
}
