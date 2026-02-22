package mongodb

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/scorify/schema"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Schema struct {
	Server     string `key:"target"`
	Port       int    `key:"port" default:"27017"`
	Username   string `key:"username"`
	Password   string `key:"password"`
	AuthSource string `key:"auth_source" default:"admin"`
	Database   string `key:"database"`
	Collection string `key:"collection"`
	Query      string `key:"query"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.Server == "" {
		return fmt.Errorf("server is required; got %q", conf.Server)
	}

	if conf.Port <= 0 || conf.Port > 65535 {
		return fmt.Errorf("port is invalid; got %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username is required; got %q", conf.Username)
	}

	if conf.Password == "" {
		return fmt.Errorf("password is required; got %q", conf.Password)
	}

	if conf.Database == "" {
		return fmt.Errorf("database is required; got %q", conf.Database)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("context deadline is not set")
	}

	timeout := time.Duration(math.Floor(time.Until(deadline).Seconds())) * time.Second

	uri := fmt.Sprintf(
		"mongodb://%s:%s@%s:%d/%s?authSource=%s",
		conf.Username,
		conf.Password,
		conf.Server,
		conf.Port,
		conf.Database,
		conf.AuthSource,
	)

	clientOptions := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(timeout).
		SetServerSelectionTimeout(timeout).
		SetTimeout(timeout)

	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to mongodb server: %w", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to ping mongodb server: %w", err)
	}

	if conf.Collection != "" && conf.Query != "" {
		var filter any
		err = bson.UnmarshalExtJSON([]byte(conf.Query), true, &filter)
		if err != nil {
			return fmt.Errorf("failed to parse query: %w", err)
		}

		collection := client.Database(conf.Database).Collection(conf.Collection)
		result := collection.FindOne(ctx, filter)
		if result.Err() != nil {
			if result.Err() == mongo.ErrNoDocuments {
				return fmt.Errorf("no documents returned from query: %q", conf.Query)
			}
			return fmt.Errorf("failed to execute query: %w", result.Err())
		}
	}

	return nil
}
