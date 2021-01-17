package endpoint

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"testing"
)

func TestMongoPing(t *testing.T) {
	opts := &options.ClientOptions{
		Hosts: []string{"127.0.0.1:27018"},
	}

	// 连接数据库
	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		t.Error(err.Error())
	}

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		t.Error(err.Error())
	}
}

func TestCheckData(t *testing.T) {
	opts := &options.ClientOptions{
		Hosts: []string{"127.0.0.1:27018"},
	}
	opts.Auth = &options.Credential{
		Username: "test",
		Password: "test",
	}

	// 连接数据库
	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		t.Error(err.Error())
	}

	db := client.Database("test")
	cc := db.Collection("ss")

	for i := 0; i < 22032; i++ {
		r := cc.FindOne(context.Background(), bson.M{"_id": i})
		if r.Err() != nil {
			t.Error(r.Err())
		}
	}

}
