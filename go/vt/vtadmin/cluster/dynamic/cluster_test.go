package dynamic

import (
	"context"
	"encoding/base32"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtadmin/cluster"
)

func TestClusterFromString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		s          string
		encoder    func(b []byte) string
		expectedID string
		shouldErr  bool
	}{
		{
			name: "ok",
			s: `{
				"id": "dynamic_cluster",
				"discovery": "dynamic",
				"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"fqdn\": \"localhost:15000\", \"hostname\": \"localhost:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"localhost:15991\" } } ] }"
			}`,
			expectedID: "dynamic_cluster",
		},
		{
			name:      "empty id",
			s:         `{"id": ""}`,
			shouldErr: true,
		},
		{
			name:      "no id",
			s:         `{"vtctlds": []}`,
			shouldErr: true,
		},
		{
			name:      "bad encoding",
			s:         "…∆ø†h¬®çå®øç", // this junk (when base32 hex'd) breaks base64 std decoding
			encoder:   base32.HexEncoding.EncodeToString,
			shouldErr: true,
		},
		{
			name:      "invalid json",
			s:         `{`,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.encoder == nil {
				tt.encoder = base64.StdEncoding.EncodeToString
			}

			enc := tt.encoder([]byte(tt.s))

			c, id, err := ClusterFromString(context.Background(), enc)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Nil(t, c, "when err != nil, cluster must be nil")
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, id, "when err == nil, id must be non-empty")

			assert.Equal(t, tt.expectedID, id)
			assert.NotNil(t, c, "when err == nil, cluster should not be nil")
		})
	}

	t.Run("vtsql credentials", func(t *testing.T) {
		t.Parallel()

		enc := base64.StdEncoding.EncodeToString([]byte(`{
			"id": "dynamic_cluster",
			"vtsql-credentials-username": "vtadmin-username",
			"vtsql-credentials-password": "my-password",
			"discovery": "dynamic",
			"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"fqdn\": \"localhost:15000\", \"hostname\": \"localhost:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"localhost:15991\" } } ] }"
		}`))

		cfg, id, err := cluster.LoadConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(enc)), "json")

		require.NoError(t, err)
		require.NotEmpty(t, id, "when err == nil, id must be non-empty")

		assert.Equal(t, cfg.VtSQLFlags["credentials-username"], "vtadmin-username")
		assert.Equal(t, cfg.VtSQLFlags["credentials-password"], "my-password")
	})

	t.Run("vtsql credentials - empty password string", func(t *testing.T) {
		t.Parallel()

		enc := base64.StdEncoding.EncodeToString([]byte(`{
			"id": "dynamic_cluster",
			"vtsql-credentials-username": "vtadmin-username",
			"vtsql-credentials-password": "",
			"discovery": "dynamic",
			"discovery-dynamic-discovery": "{\"vtctlds\": [ { \"host\": { \"fqdn\": \"localhost:15000\", \"hostname\": \"localhost:15999\" } } ], \"vtgates\": [ { \"host\": {\"hostname\": \"localhost:15991\" } } ] }"
		}`))

		cfg, id, err := cluster.LoadConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(enc)), "json")

		require.NoError(t, err)
		require.NotEmpty(t, id, "when err == nil, id must be non-empty")

		assert.Equal(t, cfg.VtSQLFlags["credentials-username"], "vtadmin-username")
		assert.Equal(t, cfg.VtSQLFlags["credentials-password"], "")
	})
}
