package consumer

import (
	"context"
	"encoding/json"
	"os"

	"kafka-consumer/helper"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

//kafka consumer hold sarama consumer
type KafkaConsumer struct {
	Consumer sarama.Consumer
}

type Post struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Body  string `json:"body"`
}

//Consume function to consume message from kafka

func (c *KafkaConsumer) Consume(topics []string, signals chan os.Signal) {

	//connect to mongodb

	collection := helper.ConnectDB()

	chanMessage := make(chan *sarama.ConsumerMessage, 256)

	for _, topic := range topics {
		partitionList, err := c.Consumer.Partitions(topic)
		if err != nil {
			logrus.Errorf("Unable to get partition got err %v", err)
			continue
		}

		for _, partition := range partitionList {
			go consumeMessage(c.Consumer, topic, partition, chanMessage)
		}

	}
	logrus.Infof("Kafka is consuming...")

ConsumerLoop:
	for {
		select {
		case msg := <-chanMessage:
			logrus.Infof(string(msg.Value))

			insertDB := &Post{}
			err := json.Unmarshal(msg.Value, insertDB)
			if err != nil {
				logrus.Panic(err)
			}
			// insert our book model.
			_, err = collection.InsertOne(context.TODO(), insertDB)
			if err != nil {
				logrus.Panic(err)
			}
			logrus.Printf("Successfull insert to mongoDB")

		case sig := <-signals:
			if sig == os.Interrupt {
				break ConsumerLoop
			}
		}
	}

}

func consumeMessage(consumer sarama.Consumer, topic string, partition int32, c chan *sarama.ConsumerMessage) {
	msg, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		logrus.Errorf("Unable to consume message partition %v got erro %v", partition, err)
		return
	}
	defer func() {
		if err := msg.Close(); err != nil {
			logrus.Errorf("Unable to close partition %v: %v", partition, err)
		}
	}()

	for {
		msg := <-msg.Messages()
		c <- msg
	}
}
