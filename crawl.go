package main

import (
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"strings"

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
	// defaultParallel 缺省的并发数量(不并发)
	defaultParallel = 1
)

// Crawl 抓取
type Crawl struct {
}

// NewCrawl 新建抓取
func NewCrawl() *Crawl {
	return &Crawl{}
}

// Do 执行抓取
func (s Crawl) Do(configs []*config.Config) error {

	log.Print("开始 >>>>>>>>>>>>")
	start := time.Now()
	defer log.Printf(">>>>>>>>>>>> 结束 耗时:%s", time.Now().Sub(start).String())

	ctx := context.New()
	for _, config := range configs {
		err := s.do(config, ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// do 执行操作
func (s Crawl) do(conf *config.Config, ctx *context.Context) error {

	action, err := conf.Get("type")
	if err != nil {
		return err
	}

	switch action {
	case "get":
		// 抓取网页
		return s.get(conf, ctx)
	case "match":
		// 匹配
		return s.match(conf, ctx)
	case "range":
		// 循环
		return s.forrange(conf, ctx)
	case "download":
		// 下载
		return s.download(conf, ctx)
	case "print":
		// 显示
		return s.print(conf, ctx)
	default:
		return errors.Errorf("unknown action type: %s", action)
	}
}

// actions 执行后续操作
func (s Crawl) actions(conf *config.Config, ctx *context.Context) error {

	configs, err := conf.Configs("actions")
	if err != nil {
		if strings.HasSuffix(err.Error(), "not found") {
			return nil
		}

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
func (s Crawl) get(conf *config.Config, ctx *context.Context) error {

	parameters, err := conf.Config("parameters")
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

	return s.actions(conf, ctx)
}

// match 匹配
func (s Crawl) match(conf *config.Config, ctx *context.Context) error {

	parameters, err := conf.Config("parameters")
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

	groups := complied.FindAllStringSubmatch(html, -1)
	for _, group := range groups {

		if len(keys) != len(group)-1 {
			return errors.Errorf("match keys len %d is not equal matches len %d", len(keys), len(group)-1)
		}

		cloneContext := ctx.Clone()
		for index, key := range keys {
			err = cloneContext.Set(key, group[index+1])
			if err != nil {
				return err
			}
		}

		err = s.actions(conf, cloneContext)
		if err != nil {
			return err
		}
	}

	return nil
}

// forrange 循环
func (s Crawl) forrange(conf *config.Config, ctx *context.Context) error {

	parameters, err := conf.Config("parameters")
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
		parallel = defaultParallel
	}

	ch := make(chan bool, parallel)
	wg := new(sync.WaitGroup)
	wg.Add(end - start + 1)

	for index := start; index < end; index++ {

		go func(idx int) {

			_context := ctx.Clone()

			err = _context.Set(key, fmt.Sprintf(format, index))
			if err != nil {
				err1, success := err.(*errors.Error)
				if success {
					log.Fatal(err1.ErrorStack())
				}
				log.Fatal(err)
			}

			err = s.actions(conf, _context)
			if err != nil {
				err1, success := err.(*errors.Error)
				if success {
					log.Fatal(err1.ErrorStack())
				}
				log.Fatal(err)
			}

			<-ch
			wg.Done()
		}(index)

		ch <- true
	}
	wg.Wait()

	return nil
}

// download 下载
func (s Crawl) download(conf *config.Config, ctx *context.Context) error {

	parameters, err := conf.Config("parameters")
	if err != nil {
		return err
	}

	url, err := parameters.StringParameter("url", ctx)
	if err != nil {
		return err
	}

	path, err := parameters.StringParameter("path", ctx)
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

	err = net.DownloadFileRetry(url, path, retry, interval)
	if err != nil {
		return errors.New(err)
	}

	return nil
}

// print 显示内容
func (s Crawl) print(conf *config.Config, ctx *context.Context) error {

	content, err := conf.StringParameter("parameters", ctx)
	if err != nil {
		return err
	}

	log.Print(content)

	return s.actions(conf, ctx)
}
