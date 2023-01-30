package operators

import (
	"strconv"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/sqlparser"
)

func tableSpecRequiresNewTableNode(oldSpec, newSpec *sqlparser.TableSpec) bool {
	// any change on the number of columns requires a new table node
	if len(oldSpec.Columns) != len(newSpec.Columns) {
		return true
	}

	for col := 0; col < len(oldSpec.Columns); col++ {
		oldColumn := oldSpec.Columns[col]
		newColumn := newSpec.Columns[col]
		// any column reordering requires a new base node
		if !sqlparser.EqualsIdentifierCI(oldColumn.Name, newColumn.Name, nil) {
			return true
		}
		// if the type of the column has had a meaningful change, we need a new table node
		if columnTypeHasChanged(&oldColumn.Type, &newColumn.Type) {
			return true
		}
	}
	return false
}

// ported from https://github.com/planetscale/singularity/blob/main/pkg/handlers/checkbranchmergeability/boost.go#L210-L317
func columnTypeHasChanged(colType, destColType *sqlparser.ColumnType) bool {
	colType = sqlparser.CloneRefOfColumnType(colType)
	destColType = sqlparser.CloneRefOfColumnType(destColType)

	// We don't care about changes to the default value, auto increment or
	// comment since that does not affect any existing values, and we always
	// read from the database through the vstream or with an upquery.
	if colType.Options != nil {
		colType.Options.Default = nil
		colType.Options.Autoincrement = false
		colType.Options.Comment = nil
	}
	if destColType.Options != nil {
		destColType.Options.Default = nil
		destColType.Options.Autoincrement = false
		destColType.Options.Comment = nil
	}

	if sqlparser.EqualsRefOfColumnType(colType, destColType, nil) {
		return false
	}

	// Alright, we've handled the very basic case of no change above,
	// now for anything more detailed.

	// If we move from nullable to not null, we see that as an incompatible type change
	if colType.Options != nil && colType.Options.Null != nil && !*colType.Options.Null &&
		(destColType.Options == nil || destColType.Options.Null == nil || *destColType.Options.Null) {
		return true
	}

	lowerDestColType := strings.ToLower(destColType.Type)
	switch strings.ToLower(colType.Type) {
	case "tinyint":
		return !strings.EqualFold("tinyint", destColType.Type)
	case "smallint":
		return !slices.Contains([]string{"smallint", "tinyint"}, lowerDestColType)
	case "mediumint":
		return !slices.Contains([]string{"mediumint", "smallint", "tinyint"}, lowerDestColType)
	case "int":
		return !slices.Contains([]string{"int", "mediumint", "smallint", "tinyint"}, lowerDestColType)
	case "bigint":
		return !slices.Contains([]string{"bigint", "mediumint", "smallint", "tinyint"}, lowerDestColType)
	case "tinyblob":
		return !strings.EqualFold("tinyblob", destColType.Type)
	case "blob":
		return !slices.Contains([]string{"blob", "tinyblob"}, lowerDestColType)
	case "mediumblob":
		return !slices.Contains([]string{"mediumblob", "blob", "tinyblob"}, lowerDestColType)
	case "longblob":
		return !slices.Contains([]string{"longblob", "mediumblob", "blob", "tinyblob"}, lowerDestColType)
	case "tinytext":
		return !strings.EqualFold("tinytext", destColType.Type)
	case "text":
		return !slices.Contains([]string{"text", "tinytext"}, lowerDestColType)
	case "mediumtext":
		return !slices.Contains([]string{"mediumtext", "text", "tinytext"}, lowerDestColType)
	case "longtext":
		return !slices.Contains([]string{"longtext", "mediumtext", "text", "tinytext"}, lowerDestColType)
	case "char", "nchar", "varchar", "nvarchar", "binary", "varbinary":
		if !strings.EqualFold(colType.Type, destColType.Type) {
			return true
		}
		return !safeLengthIncrease(destColType.Length, colType.Length)
	case "enum":
		if !strings.EqualFold(colType.Type, destColType.Type) {
			return true
		}
		// Only allow expansion of enum values here
		if len(colType.EnumValues) < len(destColType.EnumValues) {
			return true
		}
		// Check if all the first values match from the existing
		// enum
		for i, val := range destColType.EnumValues {
			if val != colType.EnumValues[i] {
				return true
			}
		}
		return false
	}

	// Not known, for safety we assume it's a non-compatible change.
	return true
}

func safeLengthIncrease(from, to *sqlparser.Literal) bool {
	if from == nil && to == nil {
		return true
	}
	if from == nil || to == nil {
		return false
	}
	if from.Type != sqlparser.IntVal || to.Type != sqlparser.IntVal {
		// Should never happen as this is always an int literal but
		// a safety check just in case
		return false
	}

	fromVal, err := strconv.ParseInt(from.Val, 10, 64)
	if err != nil {
		return false
	}
	toVal, err := strconv.ParseInt(to.Val, 10, 64)
	if err != nil {
		return false
	}
	return toVal >= fromVal
}
