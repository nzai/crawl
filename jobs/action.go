package jobs

// SingleContextAction action results single context
type SingleContextAction interface {
	Do(*Context) error
}

// MultipleContextAction action results multiple contexts
type MultipleContextAction interface {
	Do(*Context) ([]*Context, error)
}
