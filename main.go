package main

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"sync"

	"github.com/nzai/crawl/config"
	"github.com/nzai/go-utility/io"
	"github.com/nzai/go-utility/net"
)

func main() {

	log.Print("开始 >>>>>>>>>>>>")

	filePath := ""
	if len(os.Args) > 1 {
		filePath = os.Args[1]
	}

	cm, err := config.GetConfig(filePath)
	if err != nil {
		log.Fatalf("解析配置文件发生错误:%s", err.Error())
	}

	err = doNextAction(cm, nil)
	if err != nil {
		log.Fatalf("执行操作发生错误:%s", err.Error())
	}

	log.Print(">>>>>>>>>>>> 结束")
}

func doNextAction(action map[string]interface{}, context interface{}) error {

	nextAction, found := action["action"]
	if !found {
		return nil
	}

	return doActions(nextAction.([]interface{}), context)
}

func doActions(actions []interface{}, context interface{}) error {

	for _, action := range actions {
		err := doAction(action.(map[string]interface{}), context)
		if err != nil {
			return err
		}
	}

	return nil
}

func doAction(action map[string]interface{}, context interface{}) error {
	if action == nil {
		return fmt.Errorf("参数为空")
	}

	_type, found := action["type"]
	if !found {
		return fmt.Errorf("错误的配置:%v", action)
	}

	switch _type {
	case "get":
		return doGet(action, context)
	case "match":
		return doMatch(action, context)
	case "matches":
		return doMatches(action, context)
	case "download":
		return doDownload(action, context)
	case "print":
		return doPrint(action, context)
	case "range":
		return doRange(action, context)
	}

	return nil
}

func doGet(action map[string]interface{}, context interface{}) error {

	_url, found := action["url"]
	if !found {
		return fmt.Errorf("[Get]缺少url配置:%v", action)
	}

	url := _url.(map[string]interface{})

	pattern, found := url["pattern"]
	if !found {
		return fmt.Errorf("错误的配置文件:url缺pattern")
	}

	param, err := parseIndexParameter(url, context)
	if err != nil {
		return err
	}

	parsedUrl := fmt.Sprintf(pattern.(string), param...)
	html, err := net.DownloadString(parsedUrl)
	if err != nil {
		return err
	}

	log.Printf("[Get]\t%s", parsedUrl)

	return doNextAction(action, html)
}

func doMatch(action map[string]interface{}, context interface{}) error {

	pattern, found := action["pattern"]
	if !found {
		return fmt.Errorf("错误的配置文件:match缺pattern")
	}

	complied, err := regexp.Compile(pattern.(string))
	if err != nil {
		return err
	}

	match := complied.FindStringSubmatch(context.(string))
	log.Printf("[Match]\t%d", len(match))
	return doNextAction(action, match)
}

func doMatches(action map[string]interface{}, context interface{}) error {

	pattern, found := action["pattern"]
	if !found {
		return fmt.Errorf("错误的配置文件:matches缺pattern")
	}

	complied, err := regexp.Compile(pattern.(string))
	if err != nil {
		return err
	}

	matches := complied.FindAllStringSubmatch(context.(string), -1)
	log.Printf("[Matches]\t%d", len(matches))
	for _, match := range matches {
		err = doNextAction(action, match)
		if err != nil {
			return err
		}
	}

	return nil
}

func doDownload(action map[string]interface{}, context interface{}) error {

	//	url
	_url, found := action["url"]
	if !found {
		return fmt.Errorf("[Download]缺少url配置:%v", action)
	}
	url := _url.(map[string]interface{})

	//	url pattern
	pattern, found := url["pattern"]
	if !found {
		return fmt.Errorf("错误的配置文件:url缺pattern")
	}

	param, err := parseIndexParameter(url, context)
	if err != nil {
		return err
	}

	//	url result
	parsedUrl := fmt.Sprintf(pattern.(string), param...)

	//	path
	_path, found := action["path"]
	if !found {
		return fmt.Errorf("[Download]缺少path配置:%v", action)
	}
	path := _path.(map[string]interface{})

	// url pattern
	pattern, found = path["pattern"]
	if !found {
		return fmt.Errorf("错误的配置文件:path缺pattern")
	}

	param, err = parseIndexParameter(path, context)
	if err != nil {
		return err
	}

	//	path result
	parsedPath := fmt.Sprintf(pattern.(string), param...)

	//	exists
	exists := "overwrite"
	_exists, found := action["exists"]
	if found {
		exists = _exists.(string)
	}

	//	重复检测
	if exists == "skip" {
		if io.IsExists(parsedPath) {
			return doNextAction(action, nil)
		}
	}

	//	下载
	buffer, err := net.DownloadBufferRetry(parsedUrl, 10, 10)
	if err != nil {
		return err
	}

	//	保存
	err = io.WriteBytes(parsedPath, buffer)
	if err != nil {
		return err
	}

	log.Printf("[Download]\t下载%s到%s", parsedUrl, parsedPath)

	return doNextAction(action, nil)
}

func doPrint(action map[string]interface{}, context interface{}) error {

	pattern, found := action["pattern"]
	if !found {
		return fmt.Errorf("错误的配置文件:print缺pattern")
	}

	params, err := parseIndexParameter(action, context)
	if err != nil {
		return err
	}

	log.Printf("[Print]\t%s", fmt.Sprintf(pattern.(string), params...))

	return doNextAction(action, context)
}

func doRange(action map[string]interface{}, context interface{}) error {

	_width, found := action["width"]
	if !found {
		return fmt.Errorf("错误的配置文件:range param缺width")
	}
	width := int(_width.(float64))
	widthPatter := fmt.Sprintf("%%0%dd", width)

	_start, found := action["start"]
	if !found {
		return fmt.Errorf("错误的配置文件:range param缺start")
	}

	start, err := parseUrlRangeParameter(_start, context)
	if err != nil {
		return err
	}

	_end, found := action["end"]
	if !found {
		return fmt.Errorf("错误的配置文件:range param缺end")
	}

	end, err := parseUrlRangeParameter(_end, context)
	if err != nil {
		return err
	}

	parallel := 1
	_parallel, found := action["parallel"]
	if found {
		parallel = int(_parallel.(float64))
	}

	chanSend := make(chan int, parallel)
	defer close(chanSend)

	var wg sync.WaitGroup
	wg.Add(end - start + 1)

	_matches := context.([]string)
	for index := start; index <= end; index++ {

		chanSend <- 1
		//	并发
		go func(context []string, _index int) {

			matches := make([]string, 0)
			matches = append(context, fmt.Sprintf(widthPatter, _index))

			err = doNextAction(action, matches)
			if err != nil {
				log.Printf("[Range]	发生错误:%s", err.Error())
			}

			log.Printf("[Range]\t%d/%d/%d", start, _index, end)

			<-chanSend
			wg.Done()
		}(_matches, index)
	}

	return nil
}

func parseIndexParameter(params map[string]interface{}, context interface{}) ([]interface{}, error) {

	_params, found := params["param"]
	if !found {
		return nil, nil
	}

	match := context.([]string)
	list := make([]interface{}, 0)
	for _, _param := range _params.([]interface{}) {
		index := int(_param.(float64))
		if index >= 0 {
			list = append(list, match[index])
		}
	}

	return list, nil
}

func parseUrlRangeParameter(param interface{}, context interface{}) (int, error) {
	if reflect.TypeOf(param).Kind() == reflect.Float64 {
		return int(param.(float64)), nil
	}

	_param := param.(map[string]interface{})
	_index, found := _param["index"]
	if !found {
		return 0, fmt.Errorf("错误的配置文件:url param range缺index")
	}

	index := int(_index.(float64))

	offset := 0
	_offset, found := _param["offset"]
	if found {
		offset = int(_offset.(float64))
	}

	matches := context.([]string)

	value, err := strconv.Atoi(matches[index])
	if err != nil {
		return 0, err
	}

	return value + offset, nil
}
