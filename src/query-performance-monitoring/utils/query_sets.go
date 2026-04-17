package utils

// QuerySet contains SQL queries that may vary by database flavor (MySQL vs MariaDB)
// Currently only SlowQueries differs between MySQL and MariaDB due to CPU timing field availability
type QuerySet struct {
	SlowQueries                 string
	CurrentRunningQueriesSearch string
	RecentQueriesSearch         string
	PastQueriesSearch           string
}

// GetQuerySet returns the appropriate query set based on database flavor
// For slow query monitoring, MariaDB uses a modified query without SUM_CPU_TIME
func GetQuerySet(flavor DatabaseFlavor) QuerySet {
	slowQuery := SlowQueries
	if flavor == DatabaseFlavorMariaDB {
		slowQuery = MariaDBSlowQueries
	}

	// These queries are compatible with both MySQL and MariaDB
	return QuerySet{
		SlowQueries:                 slowQuery,
		CurrentRunningQueriesSearch: CurrentRunningQueriesSearch,
		RecentQueriesSearch:         RecentQueriesSearch,
		PastQueriesSearch:           PastQueriesSearch,
	}
}
