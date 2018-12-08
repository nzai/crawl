package main

import (
	"flag"
	"time"

	"github.com/nzai/crawl/config"
	"go.uber.org/zap"
)

var (
	jobPath  = flag.String("job", "job.json", "job json file")
	rootPath = flag.String("root", ".", "download root dir")
)

func main() {
	c := zap.NewDevelopmentConfig()
	c.DisableStacktrace = true

	logger, _ := c.Build()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	flag.Parse()

	configs, err := config.OpenFile(*jobPath)
	if err != nil {
		zap.L().Fatal("read job file failed", zap.Error(err), zap.String("path", *jobPath))
	}

	start := time.Now()
	crawl := NewCrawl()
	err = crawl.Do(configs)
	if err != nil {
		zap.L().Fatal("crawl failed", zap.Error(err))
	}

	zap.L().Info("crawl success", zap.Duration("in", time.Now().Sub(start)))
}
