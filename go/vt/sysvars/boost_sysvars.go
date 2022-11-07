package sysvars

// PlanetScale Boost specific sysvars

// BoostCachedQueries sets whether Boost caching is enabled
var BoostCachedQueries = SystemVariable{Name: "boost_cached_queries", IsBoolean: true, Default: off}

func init() {
	// Append to VitessAware on init() (the global variable is fully initialized when init is called).
	// Doing this on a separate file will prevent merge conflicts in the future
	VitessAware = append(VitessAware, BoostCachedQueries)
}
