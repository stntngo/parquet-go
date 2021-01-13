package marshal

import (
	"github.com/stntngo/parquet-go/common"
	"github.com/stntngo/parquet-go/layout"
	"github.com/stntngo/parquet-go/parquet"
	"github.com/stntngo/parquet-go/schema"
)

//Marshal function for CSV like data
func MarshalCSV(records []interface{}, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	res := make(map[string]*layout.Table)
	if ln := len(records); ln <= 0 {
		return &res, nil
	}

	for i := 0; i < len(records[0].([]interface{})); i++ {
		pathStr := schemaHandler.GetRootInName() + "." + schemaHandler.Infos[i+1].InName
		table := layout.NewEmptyTable()
		res[pathStr] = table
		table.Path = common.StrToPath(pathStr)
		table.MaxDefinitionLevel = 1
		table.MaxRepetitionLevel = 0
		table.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		table.Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
		table.Info = schemaHandler.Infos[i+1]
		// Pre-allocate these arrays for efficiency
		table.Values = make([]interface{}, 0, len(records))
		table.RepetitionLevels = make([]int32, 0, len(records))
		table.DefinitionLevels = make([]int32, 0, len(records))

		for j := 0; j < len(records); j++ {
			rec := records[j].([]interface{})[i]
			table.Values = append(table.Values, rec)
			table.RepetitionLevels = append(table.RepetitionLevels, 0)

			if rec == nil {
				table.DefinitionLevels = append(table.DefinitionLevels, 0)
			} else {
				table.DefinitionLevels = append(table.DefinitionLevels, 1)
			}
		}
	}
	return &res, nil
}
