package sql

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestSchema(tts ...sqltypes.Type) (schema []Type) {
	for _, t := range tts {
		var st = Type{
			T:         t,
			Collation: collations.CollationBinaryID,
			Nullable:  false,
		}
		if sqltypes.IsText(t) {
			st.Collation = collations.CollationUtf8mb4ID
		}
		schema = append(schema, st)
	}
	return
}

func TypeFromField(field *querypb.Field) Type {
	return Type{
		T:         field.Type,
		Collation: collations.ID(field.Charset),
		Nullable:  (field.Flags & uint32(querypb.MySqlFlag_NOT_NULL_FLAG)) == 0,
	}
}
