package jobs

import (
	"errors"
	"regexp"
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
	headers       map[string]string
	retry         int
	retryInterval time.Duration
	regexp        *regexp.Regexp
	sets          []string
	debug         bool
}

// newFetch create fetch action
func newFetch(c *Config) (interface{}, error) {
	url, err := c.String("url")
	if err != nil {
		return nil, err
	}

	headers := c.MapDefault("headers")

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

	debug := c.BoolDefault("debug", false)

	return &Fetch{
		url:           url,
		headers:       headers,
		retry:         retry,
		retryInterval: retryInterval,
		regexp:        regex,
		sets:          sets,
		debug:         debug,
	}, nil
}

// Do do job
func (s Fetch) Do(ctx *Context) ([]*Context, error) {
	html, err := s.getHTML(ctx)
	if err != nil {
		return nil, err
	}

	return s.match(ctx, html)
}

func (s Fetch) getHTML(ctx *Context) (string, error) {
	url := ctx.Expand(s.url)
	parameters := []netop.RequestParam{netop.Retry(s.retry, s.retryInterval)}
	for key, value := range s.headers {
		parameters = append(parameters, netop.Header(key, ctx.Expand(value)))
	}

	html, err := netop.GetString(url, parameters...)
	if err != nil {
		zap.L().Error("get html string failed",
			zap.Error(err),
			zap.String("url", url),
			zap.Any("headers", s.headers))
		return "", err
	}

	if s.debug {
		zap.L().Debug("get html success",
			zap.String("url", url),
			zap.Any("headers", s.headers))
	}

	return html, nil
}

func (s Fetch) match(ctx *Context, html string) ([]*Context, error) {
	groups := s.regexp.FindAllStringSubmatch(html, -1)
	if s.debug {
		zap.L().Debug("match html success",
			zap.String("expression", s.regexp.String()),
			zap.Int("matches", len(groups)))

		if len(groups) == 0 {
			zap.L().Debug("find 0 match", zap.String("html", html))
		}
	}

	ctxs := make([]*Context, len(groups))
	for index, group := range groups {
		if len(s.sets) != len(group)-1 {
			return nil, ErrKeyCountInvalid
		}

		cloneCtx := ctx.Clone()
		for keyIndex, key := range s.sets {
			cloneCtx.Set(key, group[keyIndex+1])

			if s.debug {
				zap.L().Debug("set match context success",
					zap.String("key", key),
					zap.String("value", group[keyIndex+1]))
			}
		}

		ctxs[index] = cloneCtx
	}

	return ctxs, nil
}
