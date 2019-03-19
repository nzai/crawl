package jobs

// Job crawl job
type Job interface {
	Do(*Context) error
}
