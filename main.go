package main

import (
	"log"
	"os"

	"github.com/go-errors/errors"
	"github.com/nzai/crawl/config"
)

const (
	defaultConfigFilePath = "config.json"
)

func main() {

	filePath := defaultConfigFilePath
	if len(os.Args) > 1 {
		filePath = os.Args[1]
	}

	configs, err := config.OpenFile(filePath)
	if err != nil {
		err1, success := err.(*errors.Error)
		if success {
			log.Fatal(err1.ErrorStack())
		}
		log.Fatal(err)
	}

	crawl := NewCrawl()
	err = crawl.Do(configs)
	if err != nil {
		err1, success := err.(*errors.Error)
		if success {
			log.Fatal(err1.ErrorStack())
		}
		log.Fatal(err)
	}
}
