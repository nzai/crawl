package constants

import "time"

const (
	// DefaultRetry default retry times
	DefaultRetry = 3
	// DefaultRetryInterval default retry interval
	DefaultRetryInterval = time.Second * 10
)
