package mongo

import (
	"errors"
	"fmt"
	"alin/config"
	"labix.org/v2/mgo"
	"strings"
)

type GridMongo struct {
	Index   int                // Number from config array
	Config  config.MongoConfig // Mongo config from global config
	Session *mgo.Session
	GridFS  *mgo.GridFS
	Active  bool
}

func MongoFromConfig(conf config.MongoConfig, index int) (mgo_grid *GridMongo, err error) {
	mgo_grid = new(GridMongo)
	mgo_grid.Config = conf
	mgo_grid.Index = index

	return mgo_grid, mgo_grid.UpdateSession()
}

func (mongo *GridMongo) UpdateGridFS() (err error) {
	conf := mongo.Config
	db := mongo.Session.DB(conf.DB)
	if db == nil {
		return errors.New(fmt.Sprintf("Database with name %s not Found for %s %s ", conf.DB, conf.HostPort, conf.ConnString))
	}

	mongo.GridFS = db.GridFS(conf.Prefix)
	if mongo.GridFS == nil {
		return errors.New(fmt.Sprintf("Unable to get GridFS instance %s %s ", conf.HostPort, conf.ConnString))
	}

	return nil
}

func (mgo_grid *GridMongo) UpdateSession() (err error) {
	conf := mgo_grid.Config
	if len(conf.ConnString) > 0 {
		if strings.Contains(conf.ConnString, "mongodb://") {
			mgo_grid.Session, err = mgo.Dial(conf.ConnString)
		} else {
			mgo_grid.Session, err = mgo.Dial(fmt.Sprintf("mongodb://%s", conf.ConnString))
		}

	} else {
		dial_info := &mgo.DialInfo{
			Addrs:    []string{conf.HostPort},
			Direct:   true,
			FailFast: true,
		}

		mgo_grid.Session, err = mgo.DialWithInfo(dial_info)
	}

	if err != nil {
		return err
	}

	mgo_grid.Session.SetSafe(&mgo.Safe{})
	if len(conf.ConnString) == 0 {
		mgo_grid.Session.SetMode(mgo.Monotonic, true)
	}

	err = mgo_grid.UpdateGridFS()
	if err != nil {
		return err
	}
	mgo_grid.Active = true
	return nil
}