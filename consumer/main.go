package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main(){
	// kafka topic 
	topic := "coffee"

	msgCount := 0

// create consumer then start
	broker := []string{"locahost:9092"}
	consumer, err := connectToConsumer(broker)
	if err != nil {
		panic(err)
	}

	partConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	log.Printf("consumer is running with topic: %s\n", topic)

// handle os signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
// make go routines
	doneCh := make(chan struct{})
	go func(){
		for {
			select{
				case err := <-partConsumer.Errors():
					log.Println(err)
				case msg := <-partConsumer.Messages():{
					msgCount++
					log.Println(msg)
					order := string(msg.Value)
					log.Printf("making coffee %s\n", order)
				}
				case <- sigChan: {
					log.Panicln("interrupt detected")
					doneCh <- struct{}{}
				}
			}
		}
	}()
	<-doneCh
	log.Println("proccessed", msgCount, "maessages")

// close consumers on exit
	err = consumer.Close()
	if err != nil {
		panic(err)
	}
}


func connectToConsumer(brokerUrls []string)(sarama.Consumer, error){
	config := *sarama.NewConfig()
	config.Producer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokerUrls, &config)
	return consumer, err
}