package boostplan

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func collationForColumn(table *sqlparser.TableSpec, column *sqlparser.ColumnDefinition) (collations.ID, error) {
	var tableCharsetCollation collations.ID
	var tableCollation collations.ID
	var env = collations.Local()

	for _, option := range table.Options {
		switch strings.ToLower(option.Name) {
		case "charset":
			tableCharsetCollation = env.DefaultCollationForCharset(option.String)
			if tableCharsetCollation == collations.Unknown {
				return collations.Unknown, fmt.Errorf("unsupported table charset: %s", option.String)
			}
		case "collate":
			tableCollation = env.LookupByName(option.String)
			if tableCollation == collations.Unknown {
				return collations.Unknown, fmt.Errorf("unsupported table collation: %s", option.String)
			}
		}
	}

	// If we have no custom collation, use the charset default
	if tableCollation == collations.Unknown {
		tableCollation = tableCharsetCollation
	}

	// If nothing was defined, fallback to our default
	if tableCollation == collations.Unknown {
		tableCollation = collations.ID(env.DefaultConnectionCharset())
	}

	var collationID collations.ID
	sqlType := column.Type.SQLType()
	switch {
	case sqltypes.IsText(sqlType):
		collationID = tableCollation
		collationName := column.Type.Options.Collate
		charset := column.Type.Charset
		if collationName != "" {
			collation := env.LookupByName(collationName)
			if collation == collations.Unknown {
				return collations.Unknown, fmt.Errorf("unsupported column collation: %s", collationName)
			}
			collationID = collation
		} else if charset.Name != "" {
			var collation collations.ID
			if charset.Binary {
				collation = env.BinaryCollationForCharset(charset.Name)
			} else {
				collation = env.DefaultCollationForCharset(charset.Name)
			}

			if collation == collations.Unknown {
				return collations.Unknown, fmt.Errorf("unsupported column character set: %s", charset.Name)
			}
			collationID = collation
		}
	default:
		collationID = collations.CollationBinaryID
	}

	return collationID, nil
}
