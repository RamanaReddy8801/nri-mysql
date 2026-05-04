package utils

// QuerySet contains SQL queries that may vary by database flavor (MySQL vs MariaDB)
// SlowQueries differs due to CPU timing field availability
// BlockingSessionsQuery differs due to different lock wait tables and DIGEST handling
type QuerySet struct {
	SlowQueries                 string
	CurrentRunningQueriesSearch string
	RecentQueriesSearch         string
	PastQueriesSearch           string
	BlockingSessionsQuery       string
	// NeedsQueryNormalization indicates whether blocking query texts require
	// Go-side normalization. True for MariaDB because its trx_query fallback
	// returns raw SQL; false for MySQL where DIGEST_TEXT is already normalized.
	NeedsQueryNormalization bool
}

// GetQuerySet returns the appropriate query set based on database flavor
// MariaDB uses modified queries for slow queries and blocking sessions
func GetQuerySet(flavor DatabaseFlavor) QuerySet {
	slowQuery := SlowQueries
	blockingQuery := BlockingSessionsQuery
	needsNormalization := false

	if flavor == DatabaseFlavorMariaDB {
		slowQuery = MariaDBSlowQueries
		blockingQuery = MariaDBBlockingSessionsQuery
		needsNormalization = true
	}

	// These queries are compatible with both MySQL and MariaDB
	return QuerySet{
		SlowQueries:                 slowQuery,
		CurrentRunningQueriesSearch: CurrentRunningQueriesSearch,
		RecentQueriesSearch:         RecentQueriesSearch,
		PastQueriesSearch:           PastQueriesSearch,
		BlockingSessionsQuery:       blockingQuery,
		NeedsQueryNormalization:     needsNormalization,
	}
}
