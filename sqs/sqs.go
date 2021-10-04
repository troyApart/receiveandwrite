package sqs

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type sqsMessage struct {
	Attributes    sqsAttributes          `json:"Attributes"`
	Body          map[string]interface{} `json:"Body"`
	MessageId     string                 `json:"MessageId"`
	ReceiptHandle string                 `json:"ReceiptHandle"`
}

type sqsAttributes struct {
	SentTimestamp string `json:"SentTimestamp"`
}

func ReceiveAndWrite(s sqsiface.SQSAPI, queueURL, filePrefix string) {
	t := time.Now().Unix()
	if filePrefix != "" && strings.LastIndex(filePrefix, "_") != len(filePrefix)-1 && strings.LastIndex(filePrefix, "-") != len(filePrefix)-1 {
		filePrefix = fmt.Sprintf("%s_", filePrefix)
	}
	filename := fmt.Sprintf("%s%d.json", filePrefix, t)
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var messages []sqsMessage
	for {
		result, err := s.ReceiveMessage(&awssqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(awssqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(awssqs.QueueAttributeNameAll),
			},
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: aws.Int64(10),
			VisibilityTimeout:   aws.Int64(60), // 60 seconds
			WaitTimeSeconds:     aws.Int64(0),
		})
		if err != nil {
			log.Fatal(err)
		}
		if len(result.Messages) == 0 {
			log.Println("Received no messages")
			break
		}
		for _, m := range result.Messages {
			var body map[string]interface{}
			err := json.Unmarshal([]byte(aws.StringValue(m.Body)), &body)
			if err != nil {
				log.Fatal(err)
			}
			var sentTimestamp string
			if v, ok := m.Attributes["SentTimestamp"]; ok {
				sentTimestamp = aws.StringValue(v)
			}

			messages = append(messages, sqsMessage{
				Attributes: sqsAttributes{
					SentTimestamp: sentTimestamp,
				},
				Body:          body,
				MessageId:     aws.StringValue(m.MessageId),
				ReceiptHandle: aws.StringValue(m.ReceiptHandle),
			})
		}
	}

	if len(messages) == 0 {
		log.Println("No messages to write, deleting file")
		os.Remove(filename)
	}

	ms, err := json.Marshal(messages)
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.Write(ms)
	if err != nil {
		log.Fatal(err)
	}

	for _, m := range messages {
		_, err = s.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: aws.String(m.ReceiptHandle),
		})
		if err != nil {
			log.Println(err)
		}
	}
}
