package jobs

import (
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/nzai/crawl/constants"

	"github.com/nzai/netop"
	"go.uber.org/zap"
)

var (
	// ErrKeyCountInvalid match group count different from context key count
	ErrKeyCountInvalid = errors.New("match group count different from context key count")
)

// Fetch http get html and match regexp
type Fetch struct {
	url           string
	retry         int
	retryInterval time.Duration
	regexp        *regexp.Regexp
	sets          []string
	parallel      int
	jobs          []Job
	debug         bool
}

// NewFetch create fetch from config
func NewFetch(c *Config) (Job, error) {
	url, err := c.String("url")
	if err != nil {
		return nil, err
	}

	retry := c.IntDefault("retry", constants.DefaultRetry)
	retryInterval := c.DurationDefault("interval", constants.DefaultRetryInterval)

	expression, err := c.String("regexp")
	if err != nil {
		return nil, err
	}

	regex, err := regexp.Compile(expression)
	if err != nil {
		zap.L().Error("compile regex expression failed",
			zap.Error(err),
			zap.String("expression", expression))
		return nil, err
	}

	sets, err := c.Strings("sets")
	if err != nil {
		return nil, err
	}

	parallel := c.IntDefault("parallel", 0)

	debug := c.BoolDefault("debug", false)

	jobs, err := c.ToJobs()
	if err != nil {
		return nil, err
	}

	return Fetch{
		url:           url,
		retry:         retry,
		retryInterval: retryInterval,
		regexp:        regex,
		sets:          sets,
		parallel:      parallel,
		jobs:          jobs,
		debug:         debug,
	}, nil
}

// Do do job
func (s Fetch) Do(ctx *Context) error {
	html, err := s.getHTML(ctx)
	if err != nil {
		return err
	}

	return s.match(ctx, html)
}

func (s Fetch) getHTML(ctx *Context) (string, error) {
	url := ctx.Expand(s.url)

	html, err := netop.GetString(url, netop.Retry(s.retry, s.retryInterval))
	if err != nil {
		zap.L().Error("get html string failed",
			zap.Error(err),
			zap.String("url", url))
		return "", err
	}

	if s.debug {
		zap.L().Debug("get html success",
			zap.String("url", url),
			zap.String("html", html))
	}

	return html, nil
}

func (s Fetch) match(ctx *Context, html string) error {
	groups := s.regexp.FindAllStringSubmatch(html, -1)
	if s.debug {
		zap.L().Debug("match html success", zap.Int("groups", len(groups)))
	}

	ch := make(chan bool, s.parallel)
	wg := new(sync.WaitGroup)
	wg.Add(len(groups))

	for _, group := range groups {
		if len(s.sets) != len(group)-1 {
			return ErrKeyCountInvalid
		}

		go func(_group []string) {
			cloneCtx := ctx.Clone()
			for index, key := range s.sets {
				cloneCtx.Set(key, _group[index+1])

				if s.debug {
					zap.L().Debug("set match context success",
						zap.String("key", key),
						zap.String("value", _group[index+1]))
				}
			}

			for _, job := range s.jobs {
				err := job.Do(cloneCtx)
				if err != nil {
					zap.L().Error("do job failed", zap.Error(err))
					return
				}
			}

			<-ch
			wg.Done()
		}(group)

		ch <- true
	}
	wg.Wait()

	return nil
}
