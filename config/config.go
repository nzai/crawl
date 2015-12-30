package config

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/nzai/go-utility/io"
	"github.com/nzai/go-utility/path"
)

const (
	defaultConfigFile = "config.json"
)

//	获取配置
func GetConfig(filePath string) (map[string]interface{}, error) {

	if filePath == "" {
		dir, err := path.GetStartupDir()
		if err != nil {
			return nil, err
		}

		filePath = filepath.Join(dir, defaultConfigFile)
	}

	if !io.IsExists(filePath) {
		return nil, fmt.Errorf("%s 不存在", filePath)
	}

	//	读取文件
	buffer, err := io.ReadAllBytes(filePath)
	if err != nil {
		return nil, err
	}

	//	解析配置项
	var m map[string]interface{}
	err = json.Unmarshal(buffer, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}
