package main

import (
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/go-errors/errors"
	"github.com/nzai/crawl/config"
	"github.com/nzai/crawl/context"
	"github.com/nzai/go-utility/net"
)

const (
	// defaultRetry 缺省的重试次数
	defaultRetry = 5
	// defaultRetryInterval 缺省的重试间隔
	defaultRetryInterval = time.Second * 5
)

// Crawl 抓取
type Crawl struct {
}

// NewCrawl 新建抓取
func NewCrawl() *Crawl {
	return &Crawl{}
}

// Do 执行抓取
func (s Crawl) Do(config *config.Config) error {

	log.Print("开始 >>>>>>>>>>>>")
	start := time.Now()
	defer log.Printf(">>>>>>>>>>>> 结束 耗时:%s", time.Now().Sub(start).String())

	configs, err := config.Configs("")
	if err != nil {
		return err
	}

	ctx := context.New()
	for _, config := range configs {
		err = s.do(config, ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// do 执行操作
func (s Crawl) do(config *config.Config, ctx *context.Context) error {

	action, err := config.Get("type")
	if err != nil {
		return err
	}

	switch action {
	case "get":
		// 抓取网页
		return s.get(config, ctx)
	case "match":
		// 匹配
		return s.match(config, ctx)
	case "range":
		// 循环
		return s.forrange(config, ctx)
	default:
		return errors.Errorf("unknown action type: %s", action)
	}
}

// actions 执行后续操作
func (s Crawl) actions(config *config.Config, ctx *context.Context) error {

	configs, err := config.Configs("actions")
	if err != nil {
		return err
	}

	for _, config := range configs {
		err = s.do(config, ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// get 抓取网页并解析
func (s Crawl) get(config *config.Config, ctx *context.Context) error {

	parameters, err := config.Config("parameters")
	if err != nil {
		return err
	}

	url, err := parameters.StringParameter("url", ctx)
	if err != nil {
		return err
	}

	retry, err := parameters.Int("retry")
	if err != nil {
		retry = defaultRetry
	}

	interval, err := parameters.Duration("interval")
	if err != nil {
		interval = defaultRetryInterval
	}

	key, err := parameters.String("key")
	if err != nil {
		return err
	}

	html, err := net.DownloadStringRetry(url, retry, interval)
	if err != nil {
		return err
	}

	err = ctx.Set(key, html)
	if err != nil {
		return err
	}

	return s.actions(config, ctx)
}

// match 匹配
func (s Crawl) match(config *config.Config, ctx *context.Context) error {

	parameters, err := config.Config("parameters")
	if err != nil {
		return err
	}

	key, err := parameters.String("key")
	if err != nil {
		return err
	}

	pattern, err := parameters.String("pattern")
	if err != nil {
		return err
	}

	keys, err := parameters.Strings("keys")
	if err != nil {
		return err
	}

	html, err := ctx.Get(key)
	if err != nil {
		return err
	}

	complied, err := regexp.Compile(pattern)
	if err != nil {
		return errors.Errorf("compile regex error: %+v", err)
	}

	matches := complied.FindAllStringSubmatch(html, -1)
	for index, match := range matches {

		if len(keys) != len(match) {
			return errors.Errorf("match keys len %d is not equal matches len %d", len(keys), len(match))
		}

		cloneContext := ctx.Clone()
		err = cloneContext.Set(keys[index], match[index])
		if err != nil {
			return err
		}

		err = s.actions(config, cloneContext)
		if err != nil {
			return err
		}
	}

	return nil
}

// forrange 循环
func (s Crawl) forrange(config *config.Config, ctx *context.Context) error {

	parameters, err := config.Config("parameters")
	if err != nil {
		return err
	}

	start, err := parameters.IntParameter("start", ctx)
	if err != nil {
		return err
	}

	end, err := parameters.IntParameter("end", ctx)
	if err != nil {
		return err
	}

	format, err := parameters.String("format")
	if err != nil {
		return err
	}

	key, err := parameters.String("key")
	if err != nil {
		return err
	}

	parallel, err := parameters.Int("parallel")
	if err != nil {
		return err
	}

	for index := start; index < end; index++ {

		err = ctx.Set(key, fmt.Sprintf(format, index))
		if err != nil {
			return err
		}

		err = s.actions(config, ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
