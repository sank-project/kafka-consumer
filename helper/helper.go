package helper

import (
	"context"
	"kafka-consumer/model"
	"log"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ConnectDB : This is helper function to connect mongoDB
// If you want to export your function. You must to start upper case function name. Otherwise you won't see your function when you import that on other class.
func ConnectDB() *mongo.Collection {

	mongoURL := model.GetConfiguration()

	// Set client options
	clientOptions := options.Client().ApplyURI(mongoURL.MongoURL)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	logrus.Infof("Connected to MongoDB!")

	collection := client.Database("sank-database").Collection("kafka_message")

	return collection
}
