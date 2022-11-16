package dynamic

import (
	"context"
	"encoding/base32"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			name:       "PlanetScale example",
			s:          `{"id":"101-101-db1-kiamr3uh3da0","name":"101-101-db1-kiamr3uh3da0","vtsql-credentials-username":"kiamr3uh3da0","vtsql-credentials-password":"","discovery":"dynamic","discovery-dynamic-discovery":"{\"vtctlds\":[{\"host\":{\"fqdn\":\"default-8487d4c-vtctld-e0fba64e.user-data.svc.cluster.local:15000\",\"hostname\":\"default-8487d4c-vtctld-e0fba64e.user-data.svc.cluster.local:15999\"}}],\"vtgates\":[{\"host\":{\"hostname\":\"primary-kiamr3uh3da0-vtgate.user-data.svc.cluster.local:15999\"}}]}"}`,
			expectedID: "101-101-db1-kiamr3uh3da0",
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
}
func TestClusterDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		s                   string
		encoder             func(b []byte) string
		expectedID          string
		shouldVtctldErr     bool
		expectedVtctldAddrs []string
		shouldVTGateErr     bool
		expectedVTGateAddrs []string
		shouldErr           bool
	}{
		{
			name:                "PlanetScale example",
			s:                   `{"id":"101-101-db1-kiamr3uh3da0","name":"101-101-db1-kiamr3uh3da0","vtsql-credentials-username":"kiamr3uh3da0","vtsql-credentials-password":"","discovery":"dynamic","discovery-dynamic-discovery":"{\"vtctlds\":[{\"host\":{\"fqdn\":\"default-8487d4c-vtctld-e0fba64e.user-data.svc.cluster.local:15000\",\"hostname\":\"default-8487d4c-vtctld-e0fba64e.user-data.svc.cluster.local:15999\"}}],\"vtgates\":[{\"host\":{\"hostname\":\"primary-kiamr3uh3da0-vtgate.user-data.svc.cluster.local:15999\"}}]}"}`,
			expectedID:          "101-101-db1-kiamr3uh3da0",
			expectedVtctldAddrs: []string{"default-8487d4c-vtctld-e0fba64e.user-data.svc.cluster.local:15999"},
			expectedVTGateAddrs: []string{"primary-kiamr3uh3da0-vtgate.user-data.svc.cluster.local:15999"},
		},
	}

	ctx := context.Background()

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

			vtgates, err := c.Discovery.DiscoverVTGateAddrs(ctx, []string{})
			if tt.shouldVTGateErr {
				assert.Error(t, err)
				assert.Nil(t, c, "when err != nil, cluster must be nil")
				return
			}
			assert.Equal(t, tt.expectedVTGateAddrs, vtgates)

			vtctlds, err := c.Discovery.DiscoverVtctldAddrs(ctx, []string{})
			if tt.shouldVtctldErr {
				assert.Error(t, err)
				assert.Nil(t, c, "when err != nil, cluster must be nil")
				return
			}
			assert.Equal(t, tt.expectedVtctldAddrs, vtctlds)
		})
	}
}
