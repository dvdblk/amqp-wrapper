package dvdblk

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

const initialReconnectDelay time.Duration = 1
const maxReconnectDelay time.Duration = 16

type messageHandler func(delivery amqp.Delivery)
type onConnectionAliveHandler func()

type Queue struct {
	name string
	url string
	connection *amqp.Connection
	channel *amqp.Channel
	onConnectionClosed chan *amqp.Error
	l *log.Logger
}

func New(name string, url string) *Queue {
	queue := Queue{
		name: name,
		url: url,
		l: log.New(os.Stdout, fmt.Sprintf("[%s-Queue] ", name), log.Ldate | log.Ltime),
	}
	return &queue
}

func (queue *Queue) ConnectAndKeepAlive(onConnect onConnectionAliveHandler) {
	for {
		var reconnectDelay = initialReconnectDelay
		for !queue.Connect() {
			queue.l.Printf("Failed to connect. Retrying in %d seconds...", reconnectDelay)
			time.Sleep(reconnectDelay * time.Second)
			// exponential backoff
			reconnectDelay = reconnectDelay * 2
			if reconnectDelay > maxReconnectDelay {
				reconnectDelay = maxReconnectDelay
			}
		}
		onConnect()
		<-queue.onConnectionClosed
	}
}

func (queue *Queue) Connect() bool {
	queue.l.Printf("Attempting to connect to %s!", queue.url)
	c, err := amqp.Dial(queue.url)
	if err != nil {
		return false
	}
	ch, err := c.Channel()
	if err != nil {
		return false
	}
	ch.Confirm(false)
	_, err = ch.QueueDeclare(
		queue.name,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return false
	}
	queue.connection = c
	queue.channel = ch
	queue.onConnectionClosed = make(chan *amqp.Error)
	queue.channel.NotifyClose(queue.onConnectionClosed)
	queue.l.Printf("Connected to %s!", queue.url)
	return true
}

func (queue *Queue) SendMessage(message string) error {
	return queue.channel.Publish(
		"",
		queue.name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

func (queue *Queue) ConsumeMessages(handler messageHandler) {
	msgs, err := queue.channel.Consume(
		queue.name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		queue.l.Println("Failed to register a consumer")
	}
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			handler(d)
		}
	}()
	queue.l.Printf("Waiting for messages...")
	<-forever
}
