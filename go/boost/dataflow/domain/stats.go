package domain

import "vitess.io/vitess/go/stats"

var (
	StatUpquery           = stats.NewCountersWithMultiLabels("BoostDomainUpquery", "The total number of upqueries performed", []string{"Worker", "Domain"})
	StatUpqueryResultSize = stats.NewHistogram("BoostDomainUpqueryResultSize", "The number of returned rows for each upquery", []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000})
	StatUpqueryError      = stats.NewCountersWithMultiLabels("BoostDomainUpqueryError", "The total number of failed upqueries", []string{"Worker", "Domain"})
)

type domainStats struct {
	labels []string
}

func (s *domainStats) onUpqueryFinished(err error, rowcount int) {
	StatUpquery.Add(s.labels, 1)
	if err == nil {
		StatUpqueryResultSize.Add(int64(rowcount))
	} else {
		StatUpqueryError.Add(s.labels, 1)
	}
}
