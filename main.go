package main

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"

	"github.com/nzai/crawl/config"
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
	log.Print("[Get]")

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

	return doNextAction(action, html)
}

func doMatch(action map[string]interface{}, context interface{}) error {
	log.Print("[Match]")

	regex, found := action["regex"]
	if !found {
		return fmt.Errorf("错误的配置文件:match缺regex")
	}

	complied, err := regexp.Compile(regex.(string))
	if err != nil {
		return err
	}

	match := complied.FindStringSubmatch(context.(string))

	return doNextAction(action, match)
}

func doMatches(action map[string]interface{}, context interface{}) error {
	log.Print("[Matches]")

	regex, found := action["regex"]
	if !found {
		return fmt.Errorf("错误的配置文件:matches缺regex")
	}

	complied, err := regexp.Compile(regex.(string))
	if err != nil {
		return err
	}

	matches := complied.FindAllStringSubmatch(context.(string), -1)

	for _, match := range matches {
		err = doNextAction(action, match)
		if err != nil {
			return err
		}
	}

	return nil
}

func doDownload(action map[string]interface{}, context interface{}) error {
	log.Print("[Download]")

	return nil
}

func doPrint(action map[string]interface{}, context interface{}) error {
	log.Print("[Print]")

	pattern, found := action["pattern"]
	if !found {
		return fmt.Errorf("错误的配置文件:print缺pattern")
	}

	params, err := parseIndexParameter(action, context)
	if err != nil {
		return err
	}

	log.Printf("%s", fmt.Sprintf(pattern.(string), params...))

	return doNextAction(action, context)
}

func doRange(action map[string]interface{}, context interface{}) error {
	log.Print("[Range]")

	_width, found := action["width"]
	if !found {
		return fmt.Errorf("错误的配置文件:range param缺width")
	}
	width := int(_width.(float64))
	widthPatter := fmt.Sprintf("%%%dd", width)

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

	for index := start; index <= end; index++ {
		value := fmt.Sprintf(widthPatter, index)
		err = doNextAction(action, []string{value})
		if err != nil {
			return err
		}
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
	match := context.([]string)

	return strconv.Atoi(match[index])
}
