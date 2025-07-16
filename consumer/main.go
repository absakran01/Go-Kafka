package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)


type CoffeeOrder struct {
	CosName string `json:"Name"`
	CofType string `json:"Type"`
}




func main(){
	// kafka topic 
	topic := "coffee"

	msgCount := 0



	// connect to couchbase
	cluster := InitDB()
	if cluster == nil {
		log.Fatal("Failed to connect to Couchbase Cluster")
	}
	// create bucket
	bucketName := "kafka"
	bucket := cluster.Bucket(bucketName)
	if bucket == nil {
		log.Fatalf("Failed to open bucket: %s", bucketName)
	} else {
		log.Printf("Connected to bucket: %s", bucketName)
	}
	//scope
	scopeName := "kafka"
	scope := bucket.Scope(scopeName)
	if scope == nil { 
		log.Fatalf("Failed to open scope: %s", scopeName)
	} else {
		log.Printf("Connected to scope: %s", scopeName)
	}
	// create collection
	collectionName := "coffee"
	collection := bucket.Collection(collectionName)
	if collection == nil {
		log.Fatalf("Failed to open collection: %s", collectionName)
	} else {
		log.Printf("Connected to collection: %s", collection.Name())
	}

// create consumer then start
	broker := []string{"localhost:9092"}
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
					order, err := toObj(msg.Value)
					if err != nil { 
						log.Println("Failed to unmarshal message:", err)
						continue
					}

					// insert into couchbase
					rslt, err := bucket.DefaultCollection().Upsert(order.CosName, order, nil)
					if err != nil {
						log.Println("Failed to insert into Couchbase:", err)
					} else { 
						log.Println("Inserted order into Couchbase:", rslt)
					}

					log.Printf("making coffee %s\n", order)
				}
				case <- sigChan: {
					log.Println("interrupt detected")
					doneCh <- struct{}{}
				}
			}
		}
	}()
	<-doneCh
	log.Println("processed", msgCount, "messages")

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




func toObj(orderJson []byte) (CoffeeOrder, error) {
	var order CoffeeOrder
	err := json.Unmarshal(orderJson, &order)
	if err != nil {
		return CoffeeOrder{}, err
	}
	return order, nil
}