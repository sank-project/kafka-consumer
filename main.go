package main

import (
	"kafka-consumer/consumer"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

func main() {
	//setup logging
	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	kafkaConfig := getKafkaConfig("", "")

	consumers, err := sarama.NewConsumer([]string{"kafka:9092"}, kafkaConfig)
	if err != nil {
		logrus.Errorf("Unable to create kafka consumer got error %v", err)
		return
	}
	defer func() {
		if err := consumers.Close(); err != nil {
			logrus.Errorf("Unable to stop kafka consumer: %v", err)
			return
		}
	}()

	logrus.Infof("Success create kafka consumer")

	kafkaConsumer := &consumer.KafkaConsumer{
		Consumer: consumers,
	}

	signals := make(chan os.Signal, 1)
	kafkaConsumer.Consume([]string{"test_topic"}, signals)
}

func getKafkaConfig(username, password string) *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Net.WriteTimeout = 5 * time.Second
	kafkaConfig.Producer.Retry.Max = 0

	if username != "" {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = username
		kafkaConfig.Net.SASL.Password = password
	}
	return kafkaConfig
}
