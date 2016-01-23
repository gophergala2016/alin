package config

import (
	"encoding/json"
	"os"
)

var (
	GLOBAL_CONFIG     *AlinConfig
)


type MongoConfig struct {
	HostPort   string `json:"host"`
	ConnString string `json:"conn_string"`
	DB         string `json:"db"`
	Prefix     string `json:"prefix"`
}

type AlinConfig struct {
	Mongo       	[]MongoConfig 					`json:"mongo"`
	HealthCheck 	int           					`json:"health_check"`
	LogFile     	string        					`json:"log_file"`
	Daemon      	bool          					`json:"daemon"`
	IP        		string 							`json:"host"`
	Port      		int    							`json:"port"`
}

func ParseConfig(filename string) (*AlinConfig, error) {
	conf := new(AlinConfig)
	file, _ := os.Open(filename)
	decoder := json.NewDecoder(file)
	err := decoder.Decode(&conf)

	if len(conf.IP) == 0 {
		conf.IP = "0.0.0.0"
	}

	if conf.Port == 0 {
		conf.Port = 8990
	}

	if len(conf.LogFile) == 0 {
		conf.LogFile = "./gogrid.log"
	}

	if conf.HealthCheck == 0 {
		conf.HealthCheck = 500  // health checks for every 500ms by default
	}

	return conf, err
}


func InitConfig(filename string) (err error) {
	GLOBAL_CONFIG, err = ParseConfig(filename)
	return
}