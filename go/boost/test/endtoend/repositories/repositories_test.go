package votes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/boost/test/helpers/booste2e"
)

func TestRepositoriesWithInQuery(t *testing.T) {
	tt := booste2e.Setup(t, booste2e.WithRecipe("repositories"))

	for i := 0; i < 10; i++ {
		tt.ExecuteFetch("insert into repositories (name, url, created_at, updated_at) values ('repo-%d', 'https://repo-%d.com', NOW(), NOW())", i, i)
	}

	for i := 0; i < 10; i++ {
		tt.ExecuteFetch("insert into tags (name, created_at, updated_at) values ('tag-%d', NOW(), NOW())", i)
	}

	for i := 0; i < 10; i++ {
		tt.ExecuteFetch("insert into repository_tags (tag_id, repository_id, created_at, updated_at) values (%d, %d, NOW(), NOW())", i, i)
	}

	for i := 0; i < 10; i++ {
		tt.ExecuteFetch("insert into stars (user_id, repository_id, created_at, updated_at) values (%d, %d, NOW(), NOW())", i, i)
	}

	time.Sleep(1 * time.Second)

	tt.ExecuteFetch("SET @@boost_cached_queries = true")

	const Query = `
select count(*) as count_all, stars.repository_id as stars_repository_id from stars
    join repositories on repositories.id = stars.repository_id
    join repository_tags on repository_tags.repository_id = repositories.id join tags on tags.id = repository_tags.tag_id
    where stars.spammy = false and tags.name IN %s group by stars.repository_id
`

	rs := tt.ExecuteFetch(Query, "('tag-6', 'tag-7', 'tag-8')")
	require.Len(t, rs.Rows, 3)
	require.Equal(t, 1, tt.BoostTestCluster.WorkerReads())

	rs = tt.ExecuteFetch(Query, "('tag-3')")
	require.Len(t, rs.Rows, 1)
	require.Equal(t, 2, tt.BoostTestCluster.WorkerReads())
}
