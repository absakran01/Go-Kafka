package main

import (
	"github.com/couchbase/gocb/v2"
	"log"
	"os"
	"github.com/joho/godotenv"
)

func InitDB() *gocb.Cluster{
	//set verbose logging
	// gocb.SetLogger(gocb.DefaultStdioLogger())

	// init env
	err := godotenv.Load("./../.env")
	if err != nil {
		log.Fatal("Error loading .env file - ", err)
	}

		// Update this to your cluster details
		connectionString := os.Getenv("CB_URL")
		if connectionString == "" {
			log.Fatal("CB_URL environment variable is not set")
		}
		username := os.Getenv("CB_ACCESS_NAME")
		if username == "" {
			log.Fatal("CB_ACCESS_NAME environment variable is not set")
		}
		password := os.Getenv("CB_ACCESS_PASSWORD")
		if password == "" { 
			log.Fatal("CB_ACCESS_PASSWORD environment variable is not set")
		}


		options := gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
		}


		// Sets a pre-configured profile called "wan-development" to help avoid latency issues
		// when accessing Capella from a different Wide Area Network
		// or Availability Zone (e.g. your laptop).
		if err := options.ApplyProfile(gocb.
		ClusterConfigProfileWanDevelopment); err != nil {
			log.Fatal(err)
		}

		// Initialize the Connection
		cluster, err := gocb.Connect(connectionString, options)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Connected to Couchbase Cluster")

		exOrder := CoffeeOrder{}
		getRslt, err := cluster.Bucket("kafka").DefaultCollection().Get("sara", nil)
		getRslt.Content(&exOrder)
		if err != nil {
			log.Println("Error getting document from kafka bucket:", err)
		} else {
			log.Println("Document retrieved from kafka bucket:", exOrder)
		}
		return cluster
}