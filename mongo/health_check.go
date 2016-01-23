package mongo

import (
	"picsart-gogrid/config"
	"log"
	"time"
	"errors"
)

var (
	GoMongos         []*AlinMongo
)

func MongoHelthCheck() {
	var err error
	for {
		for i, mgo_grid := range GoMongos {
			if mgo_grid == nil {
				if i <= len(GoMongos)-1 && i <= len(config.GLOBAL_CONFIG.Mongo)-1 {
					GoMongos[i], err = MongoFromConfig(config.GLOBAL_CONFIG.Mongo[i], i)
					if err != nil {
						continue
					}
				}
			}
			if mgo_grid.Session == nil {
				err = mgo_grid.UpdateSession()
				if err != nil {
					continue
				}
			}
			mgo_grid.Session.Refresh()
			err = mgo_grid.Session.Ping()
			if err != nil {
				log.Println("MongoDB ", mgo_grid.Config.HostPort, mgo_grid.Config.ConnString, " Health Check faild -> ", err.Error())
				mgo_grid.Active = false
			} else {
				mgo_grid.Active = true
			}
		}
		time.Sleep(time.Millisecond * time.Duration(config.GLOBAL_CONFIG.HealthCheck))
	}
}

func InitMongoDatabases() error {
	GoMongos = make([]*AlinMongo, len(config.GLOBAL_CONFIG.Mongo))
	var err error
	for i := 0; i < len(config.GLOBAL_CONFIG.Mongo); i++ {
		GoMongos[i], err = MongoFromConfig(config.GLOBAL_CONFIG.Mongo[i], i)
		if err != nil {
			log.Println("Error Loading one of the MongoDB's ", config.GLOBAL_CONFIG.Mongo[i].ConnString, " ", config.GLOBAL_CONFIG.Mongo[i].HostPort, " at Start -> ", err.Error())
		}
	}

	if len(GoMongos) == 0 {
		return errors.New("There is no Active MongoDB's provided !")
	}

	go MongoHelthCheck()

	return nil
}