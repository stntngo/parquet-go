# parquet-go 

## This Version of `parquet-go` Is Pinned To An Old Version of Apache Thrift. Do Not Use.

## Type
There are two types in Parquet: Primitive Type and Logical Type. Logical types are stored as primitive types. The following list is the currently implemented data types:

|Parquet Type|Primitive Type|Go Type|
|-|-|-|
|BOOLEAN|BOOLEAN|bool|
|INT32|INT32|int32|
|INT64|INT64|int64|
|INT96|INT96|string|
|FLOAT|FLOAT|float32|
|DOUBLE|DOUBLE|float64|
|BYTE_ARRAY|BYTE_ARRAY|string|
|FIXED_LEN_BYTE_ARRAY|FIXED_LEN_BYTE_ARRAY|string|
|UTF8|BYTE_ARRAY|string|
|INT_8|INT32|int8|
|INT_16|INT32|int16|
|INT_32|INT32|int32|
|INT_64|INT64|int64|
|UINT_8|INT32|uint8|
|UINT_16|INT32|uint16|
|UINT_32|INT32|uint32|
|UINT_64|INT64|uint64|
|DATE|INT32|int32|
|TIME_MILLIS|INT32|int32|
|TIME_MICROS|INT64|int64|
|TIMESTAMP_MILLIS|INT64|int64|
|TIMESTAMP_MICROS|INT64|int64|
|INTERVAL|FIXED_LEN_BYTE_ARRAY|string|
|DECIMAL|INT32,INT64,FIXED_LEN_BYTE_ARRAY,BYTE_ARRAY|int32,int64,string,string|
|LIST||slice||
|MAP||map||

### Tips
* Although DECIMAL can be stored as INT32,INT64,FIXED_LEN_BYTE_ARRAY,BYTE_ARRAY, Currently I suggest to use FIXED_LEN_BYTE_ARRAY. 

## Encoding

#### PLAIN:
All types  
#### PLAIN_DICTIONARY:
All types  
#### DELTA_BINARY_PACKED:
INT32, INT64, INT_8, INT_16, INT_32, INT_64, UINT_8, UINT_16, UINT_32, UINT_64, TIME_MILLIS, TIME_MICROS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS  
#### DELTA_BYTE_ARRAY:
BYTE_ARRAY, UTF8  
#### DELTA_LENGTH_BYTE_ARRAY:
BYTE_ARRAY, UTF8

### Tips
* Some platforms don't support all kinds of encodings. If you are not sure, just use PLAIN and PLAIN_DICTIONARY.
* If the fields have many different values, please don't use PLAIN_DICTIONARY encoding. Because it will record all the different values in a map which will use a lot of memory.
