package mongo

import (
	"errors"
	"fmt"
	"alin/config"
	"labix.org/v2/mgo"
	"strings"
	"github.com/pquerna/ffjson/ffjson"
	"labix.org/v2/mgo/bson"
)

type AlinMongo struct {
	Index   int                // Number from config array
	Config  config.MongoConfig // Mongo config from global config
	Session *mgo.Session
	GridFS  *mgo.GridFS
	DB		*mgo.Database
	Active  bool
}

func MongoFromConfig(conf config.MongoConfig, index int) (mongo *AlinMongo, err error) {
	mongo = new(AlinMongo)
	mongo.Config = conf
	mongo.Index = index

	return mongo, mongo.UpdateSession()
}

func (mongo *AlinMongo) UpdateGridFS() (err error) {
	conf := mongo.Config
	db := mongo.DB
	if db == nil {
		return errors.New(fmt.Sprintf("Database with name %s not Found for %s %s ", conf.DB, conf.HostPort, conf.ConnString))
	}

	mongo.GridFS = db.GridFS(conf.Prefix)
	if mongo.GridFS == nil {
		return errors.New(fmt.Sprintf("Unable to get GridFS instance %s %s ", conf.HostPort, conf.ConnString))
	}

	return nil
}

func (mongo *AlinMongo) UpdateSession() (err error) {
	conf := mongo.Config
	if len(conf.ConnString) > 0 {
		if strings.Contains(conf.ConnString, "mongodb://") {
			mongo.Session, err = mgo.Dial(conf.ConnString)
		} else {
			mongo.Session, err = mgo.Dial(fmt.Sprintf("mongodb://%s", conf.ConnString))
		}

	} else {
		dial_info := &mgo.DialInfo{
			Addrs:    []string{conf.HostPort},
			Direct:   true,
			FailFast: true,
		}

		mongo.Session, err = mgo.DialWithInfo(dial_info)
	}

	if err != nil {
		return err
	}

	mongo.Session.SetSafe(&mgo.Safe{})
	if len(conf.ConnString) == 0 {
		mongo.Session.SetMode(mgo.Monotonic, true)
	}

	mongo.DB = mongo.Session.DB(conf.DB)

	// If prefix is defined then setting gridfs handler
	if len(conf.Prefix) > 0 {
		err = mongo.UpdateGridFS()
		if err != nil {
			return err
		}
	}
	mongo.Active = true
	return nil
}

func (mongo *AlinMongo) QueryFromJson(collection, json_str string) (result map[string]interface{}, err error) {
	conf := mongo.Config
	if mongo.DB == nil {
		return errors.New(fmt.Sprintf("Database with name %s not Found for %s %s ", conf.DB, conf.HostPort, conf.ConnString))
	}

	c := mongo.DB.C(collection)
	query := bson.M{}

	err = ffjson.Unmarshal([]byte(json_str), &query)
	if err != nil {
		return
	}

	q := c.Find(query)
	result = make(map[string]interface{})
	err = q.All(&result)
	return
}