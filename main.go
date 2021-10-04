package main

import (
	"flag"
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/troyApart/receiveandwrite/sqs"
)

func main() {
	queueURLPtr := flag.String("queue-url", "", "specify a Queue URL or Name")
	filePrefixPtr := flag.String("file-prefix", "messages", "specify a prefix for file")
	flag.Parse()
	if *queueURLPtr == "" {
		log.Fatal("No Queue URL or Name specified")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	s := awssqs.New(sess)

	sqs.ReceiveAndWrite(s, *queueURLPtr, *filePrefixPtr)

	log.Println("Job's Done!")
}
