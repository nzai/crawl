package main

import (
	"fmt"
	"log"
	"os"

	"github.com/nzai/crawl/config"
)

func main() {

	log.Print("<<<<<< 开始 >>>>>>")

	filePath := ""
	if len(os.Args) > 1 {
		filePath = os.Args[1]
	}

	cm, err := config.GetConfig(filePath)
	if err != nil {
		log.Fatalf("解析配置文件发生错误:%s", err.Error())
	}

	action, found := cm["action"]
	if !found {
		log.Fatal("错误的配置文件")
	}

	//	log.Printf("config:%v", cm)
	err = doActions(action.([]interface{}))
	if err != nil {
		log.Fatalf("执行操作发生错误:%s", err.Error())
	}

	log.Print("<<<<<< 结束 >>>>>>")
}

func doActions(actions []interface{}) error {

	for _, action := range actions {
		err := doAction(action.(map[string]interface{}))
		if err != nil {
			return err
		}
	}

	return nil
}

func doAction(action map[string]interface{}) error {
	if action == nil {
		return fmt.Errorf("参数为空")
	}

	_type, found := action["type"]
	if !found {
		log.Fatalf("错误的配置:%v", action)
	}

	switch _type {
	case "get":
		return doGet(action)
	case "match":
		return doMatch(action)
	case "matches":
		return doMatches(action)
	case "download":
		return doDownload(action)
	}

	return nil
}

func doGet(action map[string]interface{}) error {
	log.Print("[Get]")

	_, found := action["url"]
	if !found {
		log.Fatalf("[Get]缺少url配置:%v", action)
	}

	return nil
}

func doMatch(action map[string]interface{}) error {
	log.Print("[Match]")

	return nil
}

func doMatches(action map[string]interface{}) error {
	log.Print("[Matches]")

	return nil
}

func doDownload(action map[string]interface{}) error {
	log.Print("[Download]")

	return nil
}
