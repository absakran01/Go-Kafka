package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type CoffeeOrder struct {
	CosName string `json:"Name"`
	CofType string `json:"Type"`
}

func main() {
	http.HandleFunc("/order", CoffeeOrderHandler)
	http.ListenAndServe(":8080", nil)
}

func connectToProducer(brokerUrls []string)(sarama.SyncProducer, error){
	config := *sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(brokerUrls, &config)
	return producer, err
}

func pushOrderToQueue(topic string, message []byte) error{
	broker := []string{"localhost:9092"}
	//create connection to kafka
	producer, err := connectToProducer(broker)
	if err != nil{
		return err
	}
	defer producer.Close()

	// create new msg
	msg := sarama.ProducerMessage{
		Value: sarama.StringEncoder(message),
		Topic: topic,
	}

	//send message to kafka
	partition , offset, err := producer.SendMessage(&msg)
	if err != nil{
		return err
	}

	log.Printf(
		"order sent to topic = %s \npartition = %s \noffset = %s\n",
		topic,
		partition, 
		offset)

	return nil
	 
}

func CoffeeOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var order CoffeeOrder
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		
		var orderJson, err = json.Marshal(order)
		if err!= nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Process the coffee order (e.g., send to Kafka)
		err = pushOrderToQueue("coffee", orderJson)
		if err!= nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// respond to user 
		w.Header().Set("Content-Type", "application/json")
		response := map[string]interface{}{
			"success", true,
			"msg": "order for " + order.CosName + " of type " + order.CofType + "sent successfully"
		}
		w.Write(response)
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(order)
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}
