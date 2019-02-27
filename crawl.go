package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nzai/crawl/config"
	"github.com/nzai/crawl/context"
	"go.uber.org/zap"

	"github.com/nzai/netop"
)

const (
	// defaultRetry 缺省的重试次数
	defaultRetry = 5
	// defaultRetryInterval 缺省的重试间隔
	defaultRetryInterval = time.Second * 5
	// defaultParallel 缺省的并发数量(不并发)
	defaultParallel = 0
	// defaultDebug 缺省不调试
	defaultDebug = false
	// defaultOverwrite 缺省不覆盖
	defaultOverwrite = false
	// downloadingFileExt 正在下载的文件后缀
	downloadingFileExt = ".downloading"
	// downloadNotFoundFileExt 不存在的文件后缀
	downloadNotFoundFileExt = ".404"
)

// Crawl 抓取
type Crawl struct{}

// NewCrawl 新建抓取
func NewCrawl() *Crawl {
	return &Crawl{}
}

// Do 执行抓取
func (s Crawl) Do(configs []*config.Config) error {
	zap.L().Info("start")
	start := time.Now()

	ctx := context.New()
	for _, config := range configs {
		err := s.do(config, ctx)
		if err != nil {
			return err
		}
	}

	zap.L().Info("[END]", zap.Duration("in", time.Now().Sub(start)))
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
	case "string_replace":
		// 字符串替换
		return s.stringReplace(conf, ctx)
	default:
		return fmt.Errorf("unknown action type: %s", action)
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

	key, err := parameters.String("key")
	if err != nil {
		return err
	}

	retry := parameters.IntDefault("retry", defaultRetry)
	interval := parameters.DurationDefault("interval", defaultRetryInterval)
	debug := parameters.BoolDefault("debug", defaultDebug)

	if debug {
		zap.L().Debug("[DEBUG]get", zap.String("url", url), zap.Int("retry", retry), zap.Duration("interval", interval))
	}

	html, err := netop.GetString(url, netop.Retry(retry, interval))
	if err != nil {
		zap.L().Error("download string failed", zap.Error(err), zap.String("url", url))
		return err
	}

	err = ctx.Set(key, html)
	if err != nil {
		return err
	}

	if debug {
		zap.L().Debug("[DEBUG]get", zap.String("key", key), zap.String("html", html))
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

	input, err := ctx.Get(key)
	if err != nil {
		return err
	}

	parallel := parameters.IntDefault("parallel", defaultParallel)
	debug := parameters.BoolDefault("debug", defaultDebug)

	complied, err := regexp.Compile(pattern)
	if err != nil {
		zap.L().Error("compile regex failed", zap.Error(err), zap.String("pattern", pattern))
		return err
	}

	groups := complied.FindAllStringSubmatch(input, -1)
	if debug {
		zap.L().Debug("[DEBUG]match", zap.String("pattern", pattern), zap.Int("groups", len(groups)))
	}

	ch := make(chan bool, parallel)
	wg := new(sync.WaitGroup)
	wg.Add(len(groups))

	for _, group := range groups {
		if len(keys) != len(group)-1 {
			return fmt.Errorf("match keys len %d is not equal matches len %d", len(keys), len(group)-1)
		}

		go func(_group []string) {
			cloneContext := ctx.Clone()
			for index, key := range keys {
				err = cloneContext.Set(key, _group[index+1])
				if err != nil {
					zap.L().Fatal("set context failed", zap.Error(err), zap.String("key", key), zap.String("value", _group[index+1]))
				}

				if debug {
					zap.L().Debug("[DEBUG]match", zap.String("key", key), zap.String("value", _group[index+1]))
				}
			}

			err = s.actions(conf, cloneContext)
			if err != nil {
				zap.L().Error("do action failed", zap.Error(err))
			}

			<-ch
			wg.Done()

		}(group)

		ch <- true
	}
	wg.Wait()

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

	parallel := parameters.IntDefault("parallel", defaultParallel)
	debug := parameters.BoolDefault("debug", defaultDebug)

	if debug {
		zap.L().Debug("[DEBUG]range", zap.Int("start", start), zap.Int("end", end), zap.Int("parallel", parallel))
	}

	ch := make(chan bool, parallel)
	wg := new(sync.WaitGroup)
	wg.Add(end - start + 1)

	for index := start; index <= end; index++ {
		go func(idx int) {

			if debug {
				zap.L().Debug("[DEBUG]range", zap.String("key", key), zap.Int("index", idx))
			}

			_context := ctx.Clone()
			err = _context.Set(key, fmt.Sprintf(format, idx))
			if err != nil {
				zap.L().Fatal("set context failed", zap.Error(err), zap.String("key", key), zap.String("value", fmt.Sprintf(format, idx)))
			}

			err = s.actions(conf, _context)
			if err != nil {
				zap.L().Error("do action failed", zap.Error(err))
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

	referer, err := parameters.StringParameter("referer", ctx)
	if err != nil {
		referer = ""
	}

	path, err := parameters.StringParameter("path", ctx)
	if err != nil {
		return err
	}
	path = filepath.Join(*rootPath, path)

	retry := parameters.IntDefault("retry", defaultRetry)
	interval := parameters.DurationDefault("interval", defaultRetryInterval)
	overwrite := parameters.BoolDefault("overwrite", defaultOverwrite)
	debug := parameters.BoolDefault("debug", defaultDebug)

	if debug {
		zap.L().Debug("[DEBUG]download",
			zap.String("url", url),
			zap.String("referer", referer),
			zap.String("path", path),
			zap.Int("retry", retry),
			zap.Duration("interval", interval),
			zap.Bool("overwrite", overwrite))
	}

	err = s.downloadFile(url, referer, path, retry, interval, overwrite)
	if err != nil {
		return err
	}

	return nil
}

// downloadFile 下载文件
func (s Crawl) downloadFile(url, referer, path string, retry int, interval time.Duration, overwrite bool) error {
	notFoundPath := path + downloadNotFoundFileExt
	if (s.isExists(path) || s.isExists(notFoundPath)) && !overwrite {
		return nil
	}

	err := s.ensureDir(filepath.Dir(path))
	if err != nil {
		return err
	}

	downloadingPath := path + downloadingFileExt

	for times := retry - 1; times >= 0; times-- {
		err = s.tryDownloadFile(url, referer, downloadingPath, retry, interval)
		if err == nil {
			zap.L().Info("[Download]", zap.String("url", url), zap.String("path", path))
			return os.Rename(downloadingPath, path)
		}

		if err == netop.ErrNotFound {
			zap.L().Info("[Download]", zap.String("url", url), zap.String("path", notFoundPath))
			return os.Rename(downloadingPath, notFoundPath)
		}

		if times == 0 {
			zap.L().Error("download file failed", zap.Error(err), zap.String("url", url), zap.Int("retry", retry))
			return err
		}

		// 延时重试
		zap.L().Warn("try download file failed",
			zap.Error(err),
			zap.String("url", url),
			zap.Int("retry", retry),
			zap.Float64("interval", interval.Seconds()))
		time.Sleep(interval)
	}

	return nil
}

// tryDownloadFile 尝试下载文件
func (s Crawl) tryDownloadFile(url, referer, path string, retry int, interval time.Duration) error {
	progressChannel := make(chan *netop.Progress, 64)
	go func(ch <-chan *netop.Progress) {
		for progress := range ch {
			zap.L().Debug("download progress",
				zap.String("total", s.parseSizeText(progress.Total)),
				zap.String("completed", fmt.Sprintf("%s(%.1f%%)", s.parseSizeText(progress.Completed), float32(progress.Completed)*100/float32(progress.Total))),
				zap.String("speed", s.parseSizeText(progress.Speed)+"/s"),
				zap.Duration("elapsed", progress.Elapsed),
				zap.Duration("remain", progress.Remain))
		}
	}(progressChannel)

	buffer, err := netop.GetBuffer(url,
		netop.Refer(referer),
		netop.Retry(retry, interval),
		netop.OnProgress(progressChannel, time.Second*10))
	if err != nil {
		zap.L().Error("download file failed",
			zap.Error(err),
			zap.String("url", url),
			zap.String("referer", referer),
			zap.String("path", path),
			zap.Int("retry", retry),
			zap.Duration("interval", interval))
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		zap.L().Error("open download file failed", zap.Error(err), zap.String("path", path))
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, buffer)
	if err != nil {
		zap.L().Error("write download file failed", zap.Error(err), zap.String("path", path))
		return err
	}

	return nil
}

func (s Crawl) parseSizeText(speed int64) string {
	if speed > 1073741824 {
		return fmt.Sprintf("%.1f GB", float32(speed)/1073741824)
	}

	if speed > 1048576 {
		return fmt.Sprintf("%.1f MB", float32(speed)/1048576)
	}

	if speed > 1024 {
		return fmt.Sprintf("%.1f KB", float32(speed)/1024)
	}

	return fmt.Sprintf("%.1f B", float32(speed))
}

// ensureDir 保证目录存在
func (s Crawl) ensureDir(dir string) error {
	if s.isExists(dir) {
		return nil
	}

	// 递推
	err := s.ensureDir(filepath.Dir(dir))
	if err != nil {
		return err
	}

	err = os.Mkdir(dir, 0755)
	if err != nil {
		if strings.Contains(err.Error(), "exists") {
			return nil
		}

		return err
	}

	return nil
}

// isExists 文件或目录是否存在
func (s Crawl) isExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// print 显示内容
func (s Crawl) print(conf *config.Config, ctx *context.Context) error {
	content, err := conf.StringParameter("parameters", ctx)
	if err != nil {
		return err
	}

	zap.L().Info(content)

	return s.actions(conf, ctx)
}

// match 匹配
func (s Crawl) stringReplace(conf *config.Config, ctx *context.Context) error {
	parameters, err := conf.Config("parameters")
	if err != nil {
		return err
	}

	key, err := parameters.String("key")
	if err != nil {
		return err
	}

	old, err := parameters.String("old")
	if err != nil {
		return err
	}

	new, err := parameters.String("new")
	if err != nil {
		return err
	}

	output, err := parameters.String("output")
	if err != nil {
		return err
	}

	input, err := ctx.Get(key)
	if err != nil {
		return err
	}

	debug := parameters.BoolDefault("debug", defaultDebug)
	result := strings.Replace(input, old, new, -1)

	err = ctx.Set(output, result)
	if err != nil {
		zap.L().Fatal("set context failed", zap.Error(err), zap.String("key", key), zap.String("value", result))
	}

	if debug {
		zap.L().Debug("[DEBUG]string_replace",
			zap.String("input", input),
			zap.String("old", old),
			zap.String("new", new),
			zap.String("result", result))
	}

	err = s.actions(conf, ctx)
	if err != nil {
		zap.L().Fatal("do action failed", zap.Error(err))
	}

	return nil
}
