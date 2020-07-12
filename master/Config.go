package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	Apipoint        int      `json:"apiPort"`
	ApiReadTimeout  int      `json:"apiReadTimeout"`
	ApiWriteTimeout int      `json:"apiWriteTimeout"`
	EtcdEndPoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
	WebRoot         string   `json:"webroot"`
}

var (
	G_config *Config
)

//加载配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)
	//1.把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	//2.做JSON反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}
	//3.赋值单例
	G_config = &conf
	fmt.Println(conf)
	return
}
