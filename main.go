package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

type VisitMessage struct {
	UserProfileId string `json:"userProfileId"`
	VisitMetadata string `json:"visitMetadata"`
}

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	db, err := getDatabase()
  	if err != nil {
    	panic(err)
  	}
  	if err := db.Ping(); err != nil {
    	panic(err)
  	}
	
	for _, message := range sqsEvent.Records {
		processMessage(db, message)
	}

	return nil
}

func getDatabase() (*sql.DB, error) {
	db, err := sql.Open("mysql", os.Getenv("DSN"))
	return db, err
}

func processMessage(db *sql.DB, message events.SQSMessage) {
	fmt.Printf("The message %s for event source %s = %s \n", message.MessageId, message.EventSource, message.Body)
	data := VisitMessage{}
	json.Unmarshal([]byte(message.Body), &data)

	fmt.Printf("User profile id is %s \n", data.UserProfileId)
	fmt.Printf("Visit metadata is %s \n", data.VisitMetadata)

	insertVisit(db, data)
}

func insertVisit(db *sql.DB, data VisitMessage) {
	fmt.Printf("Inserting visit log \n")
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	insertSql := "INSERT INTO UserProfileVisit(id, userProfileId, visitMetadata) VALUES(?, ?, ?)"
	_, err := db.ExecContext(ctx, insertSql, uuid.New(), data.UserProfileId, data.VisitMetadata)
	if err != nil {
		log.Fatal(err)
		fmt.Printf("Failed to insert visit log \n")
	} else {
		fmt.Printf("Inserted visit log! \n")
	}
}


